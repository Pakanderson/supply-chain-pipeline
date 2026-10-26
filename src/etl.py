"""
etl.py — incremental ETL for the supply chain warehouse.

This is the piece that's deliberately DIFFERENT from the sibling
ecommerce-data-warehouse project. That project does a full refresh every
run (simple, fine for its data size). This one simulates the pattern you
actually need once a full refresh becomes too slow or too wasteful: an
INCREMENTAL load driven by a WATERMARK.

The source CSV covers Jan 2015 - Jan 2018. We treat each calendar MONTH
as one "batch" — as if a new month of orders lands in the warehouse each
time the pipeline runs, the way a real operational feed would deliver new
data daily/hourly rather than all at once. Concretely:

  1. Look at the watermark table: what's the most recent batch we've
     already loaded?
  2. Find every batch in the source data that's NEWER than that.
  3. For each pending batch, in order: transform it, load it, and only
     THEN advance the watermark to that batch. If the process crashes
     halfway through a batch, the watermark still points at the last
     batch that fully committed — so re-running the pipeline resumes
     from there instead of duplicating already-loaded data or silently
     skipping a partially-loaded one.

That per-batch commit-then-advance-watermark sequence is the actual
mechanism that makes this "idempotent AND incremental" rather than just
one or the other.
"""

import logging
from datetime import date, datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.models import (
    Base, DimCustomer, DimProduct, DimLocation, DimDate,
    FactOrderItem, EtlWatermark, EtlRunLog, EtlBatchLog,
)

logger = logging.getLogger("supply_chain_etl")
# Explicit level + dedicated handler, NOT logging.basicConfig(): basicConfig
# only configures the root logger, and Prefect reconfigures that same root
# logger (to PREFECT_LOGGING_LEVEL, WARNING by default here) once a flow
# actually starts running. With no level of its own, this logger would then
# silently inherit WARNING and drop every info() call for the whole run,
# making a multi-minute first load look hung with zero console output.
logger.setLevel(logging.INFO)
if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(_handler)
    logger.propagate = False

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"
RAW_CSV = RAW_DIR / "DataCoSupplyChainDataset.csv"
PIPELINE_NAME = "supply_chain_orders"

# The raw file has several near-duplicate columns (a very realistic
# messy-data quirk — e.g. "Sales" vs "Order Item Total" vs "Sales per
# customer", or "Product Card Id" vs "Order Item Cardprod Id" holding the
# same value under two names). Rather than carry all of them forward, this
# is the explicit, documented decision about which column is canonical for
# each concept — worth pointing at directly if asked "why does the raw
# file have more columns than your schema?".
COLUMN_MAP = {
    "Order Id": "order_id",
    "Order Item Id": "order_item_id",
    "Customer Id": "customer_id",
    "Customer Fname": "first_name",
    "Customer Lname": "last_name",
    "Customer Segment": "segment",
    "Customer City": "customer_city",
    "Customer State": "customer_state",
    "Customer Country": "customer_country",
    "Product Card Id": "product_id",       # canonical product id (duplicate of "Order Item Cardprod Id")
    "Product Name": "product_name",
    "Category Id": "category_id",
    "Category Name": "category_name",
    "Department Id": "department_id",
    "Department Name": "department_name",
    "Product Price": "price",
    "Market": "market",
    "Order Region": "region",
    "Order Country": "order_country",
    "Order State": "order_state",
    "Order City": "order_city",
    "order date (DateOrders)": "order_date",
    "shipping date (DateOrders)": "shipping_date",
    "Order Status": "order_status",
    "Delivery Status": "delivery_status",
    "Shipping Mode": "shipping_mode",
    "Late_delivery_risk": "late_delivery_risk",
    "Days for shipping (real)": "days_for_shipping_real",
    "Days for shipment (scheduled)": "days_for_shipment_scheduled",
    "Order Item Quantity": "quantity",
    "Order Item Discount": "discount",
    "Order Item Discount Rate": "discount_rate",
    "Order Item Product Price": "item_product_price",
    "Sales": "sales",                       # canonical "revenue for this line" (duplicate of "Sales per customer")
    "Order Item Total": "order_item_total",  # canonical "total charged for this line" incl. discount
    "Order Profit Per Order": "profit_per_order",  # canonical profit figure (duplicate of "Benefit per order")
    "Order Item Profit Ratio": "profit_ratio",
}


# ---------------------------------------------------------------------------
# EXTRACT
# ---------------------------------------------------------------------------

def extract() -> pd.DataFrame:
    """Read the full raw CSV once. The file is Latin-1 encoded (city/state
    names contain non-ASCII characters) — reading it as UTF-8 raises a
    UnicodeDecodeError, which is how this requirement was discovered."""
    logger.info("EXTRACT: reading %s", RAW_CSV)
    df = pd.read_csv(RAW_CSV, encoding="latin1")
    logger.info("  %d rows, %d columns", len(df), df.shape[1])
    return df


# ---------------------------------------------------------------------------
# TRANSFORM (per batch)
# ---------------------------------------------------------------------------

def clean(raw: pd.DataFrame) -> pd.DataFrame:
    """
    Rename columns to our schema's names, parse dates, and tag every row
    with its batch_month. Deliberately separate from transform_batch()
    below and called exactly ONCE per pipeline run (not once per batch) —
    renaming/parsing all 180k rows 37 times (once per monthly batch) would
    waste most of the pipeline's runtime re-doing the same work. Clean
    once, then slice out each batch cheaply.
    """
    df = raw.rename(columns=COLUMN_MAP).copy()
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df["shipping_date"] = pd.to_datetime(df["shipping_date"], errors="coerce")
    df["batch_month"] = df["order_date"].dt.strftime("%Y-%m")
    return df


def list_available_batches(cleaned: pd.DataFrame) -> list[str]:
    """Every distinct 'YYYY-MM' batch present in the (already-cleaned)
    source data, sorted chronologically — "what batches COULD be loaded",
    independent of what already HAS been loaded."""
    return sorted(cleaned["batch_month"].dropna().unique().tolist())


def get_watermark(session: Session) -> str | None:
    row = session.get(EtlWatermark, PIPELINE_NAME)
    return row.last_loaded_batch if row else None


def get_pending_batches(session: Session, cleaned: pd.DataFrame) -> list[str]:
    """The actual incremental logic: all available batches strictly after
    the watermark. On a completely fresh database, the watermark is None,
    so every batch is 'pending' — the first run loads full history, and
    every run after that only loads what's new."""
    all_batches = list_available_batches(cleaned)
    watermark = get_watermark(session)
    if watermark is None:
        return all_batches
    return [b for b in all_batches if b > watermark]


def transform_batch(cleaned: pd.DataFrame, batch_month: str) -> pd.DataFrame:
    """Slice out one batch (one calendar month) from the already-cleaned
    data. This is where you'd add batch-specific data quality checks in a
    more advanced version — e.g. reject the batch if >X% of rows have a
    null customer_id."""
    batch = cleaned[cleaned["batch_month"] == batch_month].copy()
    logger.info("TRANSFORM: batch %s -> %d rows", batch_month, len(batch))
    return batch


# ---------------------------------------------------------------------------
# LOAD (per batch, with get-or-create dimensions)
# ---------------------------------------------------------------------------

class DimensionCache:
    """
    Holds the business-key -> surrogate-key mapping for each dimension,
    loaded from the database once per pipeline run and updated in memory
    as new dimension members are discovered while loading batches.

    This is the "incremental dimension" pattern: unlike the sibling
    project (which rebuilds every dimension from scratch each run), here
    a customer/product/location seen in batch 3 needs to still be there,
    unchanged, when batch 7 loads — so dimensions are built up
    get-or-create style, never dropped.
    """

    def __init__(self, session: Session):
        self.session = session
        self.customer = dict(session.execute(select(DimCustomer.customer_id, DimCustomer.customer_key)).all())
        self.product = dict(session.execute(select(DimProduct.product_id, DimProduct.product_key)).all())
        self.location = {
            (r.market, r.region, r.country, r.state, r.city): r.location_key
            for r in session.execute(select(DimLocation)).scalars().all()
        }
        self.date = dict(session.execute(select(DimDate.full_date, DimDate.date_key)).all())

    def get_or_create_customer(self, row) -> int | None:
        cid = row.customer_id
        if pd.isna(cid):
            return None
        cid = int(cid)
        if cid not in self.customer:
            dim = DimCustomer(
                customer_id=cid,
                first_name=row.first_name,
                last_name=row.last_name,
                segment=row.segment,
                city=row.customer_city,
                state=row.customer_state,
                country=row.customer_country,
            )
            self.session.add(dim)
            self.session.flush()
            self.customer[cid] = dim.customer_key
        return self.customer[cid]

    def get_or_create_product(self, row) -> int | None:
        pid = row.product_id
        if pd.isna(pid):
            return None
        pid = int(pid)
        if pid not in self.product:
            dim = DimProduct(
                product_id=pid,
                product_name=row.product_name,
                category_id=_int_or_none(row.category_id),
                category_name=row.category_name,
                department_id=_int_or_none(row.department_id),
                department_name=row.department_name,
                price=_float_or_none(row.price),
            )
            self.session.add(dim)
            self.session.flush()
            self.product[pid] = dim.product_key
        return self.product[pid]

    def get_or_create_location(self, row) -> int | None:
        key = (row.market, row.region, row.order_country, row.order_state, row.order_city)
        if key not in self.location:
            dim = DimLocation(
                market=row.market, region=row.region, country=row.order_country,
                state=row.order_state, city=row.order_city,
            )
            self.session.add(dim)
            self.session.flush()
            self.location[key] = dim.location_key
        return self.location[key]

    def get_or_create_date(self, ts) -> int | None:
        if pd.isna(ts):
            return None
        d = ts.date()
        if d not in self.date:
            dim = DimDate(
                date_key=DimDate.key_for(d), full_date=d, year=d.year, month=d.month,
                day=d.day, quarter=(d.month - 1) // 3 + 1, day_of_week=d.strftime("%A"),
                is_weekend=1 if d.weekday() >= 5 else 0,
            )
            self.session.add(dim)
            self.session.flush()
            self.date[d] = dim.date_key
        return self.date[d]


def _int_or_none(v):
    return None if pd.isna(v) else int(v)


def _float_or_none(v):
    return None if pd.isna(v) else float(v)


def load_batch(session: Session, batch_df: pd.DataFrame, cache: DimensionCache) -> int:
    """Resolve/create dimension rows for this batch, then bulk-insert the
    fact rows. Returns rows loaded."""
    fact_records = []
    for row in batch_df.itertuples(index=False):
        customer_key = cache.get_or_create_customer(row)
        product_key = cache.get_or_create_product(row)
        location_key = cache.get_or_create_location(row)
        order_date_key = cache.get_or_create_date(row.order_date)
        shipping_date_key = cache.get_or_create_date(row.shipping_date)

        fact_records.append({
            "order_id": int(row.order_id),
            "order_item_id": int(row.order_item_id),
            "customer_key": customer_key,
            "product_key": product_key,
            "location_key": location_key,
            "order_date_key": order_date_key,
            "shipping_date_key": shipping_date_key,
            "order_status": row.order_status,
            "delivery_status": row.delivery_status,
            "shipping_mode": row.shipping_mode,
            "late_delivery_risk": _int_or_none(row.late_delivery_risk),
            "days_for_shipping_real": _int_or_none(row.days_for_shipping_real),
            "days_for_shipment_scheduled": _int_or_none(row.days_for_shipment_scheduled),
            "quantity": _int_or_none(row.quantity),
            "discount": _float_or_none(row.discount),
            "discount_rate": _float_or_none(row.discount_rate),
            "product_price": _float_or_none(row.item_product_price),
            "sales": _float_or_none(row.sales),
            "order_item_total": _float_or_none(row.order_item_total),
            "profit_per_order": _float_or_none(row.profit_per_order),
            "profit_ratio": _float_or_none(row.profit_ratio),
            "batch_month": row.batch_month,
        })

    if fact_records:
        session.bulk_insert_mappings(FactOrderItem, fact_records)
    return len(fact_records)


def advance_watermark(session: Session, batch_month: str) -> None:
    wm = session.get(EtlWatermark, PIPELINE_NAME)
    if wm is None:
        wm = EtlWatermark(pipeline_name=PIPELINE_NAME)
        session.add(wm)
    wm.last_loaded_batch = batch_month
    wm.updated_at = datetime.utcnow()


# ---------------------------------------------------------------------------
# ORCHESTRATION (plain-Python version — flows/pipeline_flow.py wraps this
# with Prefect for scheduling/retries/observability, but the core logic
# lives here so it's testable without needing Prefect at all)
# ---------------------------------------------------------------------------

def ensure_schema(engine) -> None:
    Base.metadata.create_all(engine)  # note: create_all, NOT drop_all — incremental!


def run_incremental_pipeline(engine, raw: pd.DataFrame | None = None) -> dict:
    """
    Run one pipeline pass: load every batch newer than the current
    watermark. Safe to call repeatedly — with no new batches available,
    it's a fast no-op.
    """
    ensure_schema(engine)
    session = Session(bind=engine)
    raw = raw if raw is not None else extract()
    cleaned = clean(raw)

    run_log = EtlRunLog(started_at=datetime.utcnow(), status="running")
    session.add(run_log)
    session.commit()

    total_rows = 0
    batches_processed = 0
    try:
        pending = get_pending_batches(session, cleaned)
        logger.info("Pending batches: %s", pending if pending else "(none — already up to date)")

        cache = DimensionCache(session)

        for batch_month in pending:
            batch_started = datetime.utcnow()
            try:
                batch_df = transform_batch(cleaned, batch_month)
                rows = load_batch(session, batch_df, cache)
                advance_watermark(session, batch_month)
                session.commit()  # commit THIS batch + watermark together, atomically

                session.add(EtlBatchLog(
                    run_key=run_log.run_key, batch_month=batch_month, status="success",
                    rows_loaded=rows, started_at=batch_started, finished_at=datetime.utcnow(),
                ))
                session.commit()

                total_rows += rows
                batches_processed += 1
                logger.info("  batch %s: loaded %d rows, watermark advanced", batch_month, rows)
            except Exception as exc:
                session.rollback()
                session.add(EtlBatchLog(
                    run_key=run_log.run_key, batch_month=batch_month, status="failed",
                    started_at=batch_started, finished_at=datetime.utcnow(), error=str(exc),
                ))
                session.commit()
                logger.exception("  batch %s FAILED — watermark not advanced, will retry next run", batch_month)
                raise

        run_log.status = "success"
        run_log.finished_at = datetime.utcnow()
        run_log.batches_processed = batches_processed
        run_log.rows_loaded = total_rows
        session.commit()

    except Exception as exc:
        run_log.status = "failed"
        run_log.finished_at = datetime.utcnow()
        run_log.notes = str(exc)
        session.commit()
        raise
    finally:
        session.close()

    return {"batches_processed": batches_processed, "rows_loaded": total_rows}
