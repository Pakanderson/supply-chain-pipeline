"""
models.py — the schema for the supply chain warehouse.

Same star-schema philosophy as the ecommerce-data-warehouse project (see
that project's docs/ARCHITECTURE.md for the general reasoning on
dimensions/facts/surrogate keys). What's DIFFERENT here, on purpose, is the
two control tables at the bottom of this file: EtlWatermark and
EtlRunLog. Those exist because this pipeline loads data INCREMENTALLY,
batch by batch, instead of doing a full refresh every run — see
src/etl.py and docs/ARCHITECTURE.md for why that matters.

One more difference worth noticing: the source data here is a single flat
CSV (unlike Olist's already-relational 8 files). Part of what this project
demonstrates is DESIGNING a normalized schema from a denormalized source —
arguably closer to what you'd actually do with a raw export from an
internal system than working with data someone already split into tables
for you.
"""

from datetime import date, datetime

from sqlalchemy import (
    Column, Integer, String, Float, Date, DateTime,
    ForeignKey, UniqueConstraint, Index,
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


# ---------------------------------------------------------------------------
# DIMENSIONS
# ---------------------------------------------------------------------------

class DimCustomer(Base):
    """One row per customer. Note: email/password/street fields from the
    raw data are deliberately dropped during transform — they're not
    needed for the analysis and dropping unnecessary PII-shaped fields
    during ETL is standard practice, worth mentioning if asked."""
    __tablename__ = "dim_customer"

    customer_key = Column(Integer, primary_key=True, autoincrement=True)
    customer_id = Column(Integer, unique=True, nullable=False)  # source "Customer Id"
    first_name = Column(String, nullable=True)
    last_name = Column(String, nullable=True)
    segment = Column(String, nullable=True)      # Consumer / Corporate / Home Office
    city = Column(String, nullable=True)
    state = Column(String, nullable=True)
    country = Column(String, nullable=True)


class DimProduct(Base):
    """One row per product SKU (source 'Product Card Id')."""
    __tablename__ = "dim_product"

    product_key = Column(Integer, primary_key=True, autoincrement=True)
    product_id = Column(Integer, unique=True, nullable=False)
    product_name = Column(String, nullable=True)
    category_id = Column(Integer, nullable=True)
    category_name = Column(String, nullable=True)
    department_id = Column(Integer, nullable=True)
    department_name = Column(String, nullable=True)
    price = Column(Float, nullable=True)


class DimLocation(Base):
    """
    One row per distinct (market, region, country, state, city) combination
    an order shipped to. Pulling this out into its own dimension avoids
    repeating those five text columns on every one of 180k fact rows —
    the same normalization argument as the customer/product dimensions,
    just applied to geography instead.
    """
    __tablename__ = "dim_location"

    location_key = Column(Integer, primary_key=True, autoincrement=True)
    market = Column(String, nullable=True)
    region = Column(String, nullable=True)
    country = Column(String, nullable=True)
    state = Column(String, nullable=True)
    city = Column(String, nullable=True)

    __table_args__ = (
        UniqueConstraint("market", "region", "country", "state", "city", name="uq_location"),
    )


class DimDate(Base):
    __tablename__ = "dim_date"

    date_key = Column(Integer, primary_key=True)  # YYYYMMDD
    full_date = Column(Date, nullable=False, unique=True)
    year = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    day = Column(Integer, nullable=False)
    quarter = Column(Integer, nullable=False)
    day_of_week = Column(String, nullable=False)
    is_weekend = Column(Integer, nullable=False)

    @staticmethod
    def key_for(d: date) -> int:
        return int(d.strftime("%Y%m%d"))


# ---------------------------------------------------------------------------
# FACT
# ---------------------------------------------------------------------------

class FactOrderItem(Base):
    """
    Grain: one row per order line item — matches the grain of the source
    CSV exactly (each row of DataCoSupplyChainDataset.csv is already one
    order item).
    """
    __tablename__ = "fact_order_item"

    order_item_key = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(Integer, nullable=False)
    order_item_id = Column(Integer, nullable=False)

    customer_key = Column(Integer, ForeignKey("dim_customer.customer_key"), nullable=True)
    product_key = Column(Integer, ForeignKey("dim_product.product_key"), nullable=True)
    location_key = Column(Integer, ForeignKey("dim_location.location_key"), nullable=True)
    order_date_key = Column(Integer, ForeignKey("dim_date.date_key"), nullable=True)
    shipping_date_key = Column(Integer, ForeignKey("dim_date.date_key"), nullable=True)

    order_status = Column(String, nullable=True)
    delivery_status = Column(String, nullable=True)
    shipping_mode = Column(String, nullable=True)
    late_delivery_risk = Column(Integer, nullable=True)
    days_for_shipping_real = Column(Integer, nullable=True)
    days_for_shipment_scheduled = Column(Integer, nullable=True)

    quantity = Column(Integer, nullable=True)
    discount = Column(Float, nullable=True)
    discount_rate = Column(Float, nullable=True)
    product_price = Column(Float, nullable=True)
    sales = Column(Float, nullable=True)
    order_item_total = Column(Float, nullable=True)
    profit_per_order = Column(Float, nullable=True)
    profit_ratio = Column(Float, nullable=True)

    # this is the column incremental loading is built around: every batch
    # this pipeline loads is "all rows where this date falls in batch X"
    batch_month = Column(String, nullable=False)  # 'YYYY-MM', e.g. '2017-03'

    __table_args__ = (
        UniqueConstraint("order_id", "order_item_id", name="uq_supply_order_item"),
        Index("ix_fact_soi_customer_key", "customer_key"),
        Index("ix_fact_soi_product_key", "product_key"),
        Index("ix_fact_soi_location_key", "location_key"),
        Index("ix_fact_soi_order_date_key", "order_date_key"),
        Index("ix_fact_soi_batch_month", "batch_month"),
    )


# ---------------------------------------------------------------------------
# CONTROL TABLES — this is what makes the pipeline incremental & resumable
# ---------------------------------------------------------------------------

class EtlWatermark(Base):
    """
    Tracks the last successfully-loaded batch for a named pipeline.

    This is THE mechanism incremental loading is built on: instead of
    asking "reload everything", the pipeline asks "what's the latest batch
    I've already loaded?" and only processes batches after that. One row
    per pipeline name, updated after every successful batch.
    """
    __tablename__ = "etl_watermark"

    pipeline_name = Column(String, primary_key=True)
    last_loaded_batch = Column(String, nullable=True)  # 'YYYY-MM', NULL = nothing loaded yet
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class EtlRunLog(Base):
    """One row per pipeline RUN (which may process zero or more batches),
    for observability — "when did this last run, did it succeed, how much
    did it load". A real orchestrator's UI (Prefect's included) gives you
    this same information, but it's still good practice to also record it
    in your own database, not just trust an external tool's retention."""
    __tablename__ = "etl_run_log"

    run_key = Column(Integer, primary_key=True, autoincrement=True)
    started_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    finished_at = Column(DateTime, nullable=True)
    status = Column(String, nullable=False, default="running")
    batches_processed = Column(Integer, nullable=True)
    rows_loaded = Column(Integer, nullable=True)
    notes = Column(String, nullable=True)


class EtlBatchLog(Base):
    """One row per BATCH processed (one row per month, in this project) —
    lets you answer 'which specific batch failed?' rather than only
    'the run failed somewhere'."""
    __tablename__ = "etl_batch_log"

    batch_log_key = Column(Integer, primary_key=True, autoincrement=True)
    run_key = Column(Integer, ForeignKey("etl_run_log.run_key"), nullable=False)
    batch_month = Column(String, nullable=False)
    status = Column(String, nullable=False)  # success / failed
    rows_loaded = Column(Integer, nullable=True)
    attempt = Column(Integer, nullable=False, default=1)
    started_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    finished_at = Column(DateTime, nullable=True)
    error = Column(String, nullable=True)


ALL_TABLES = [
    DimCustomer, DimProduct, DimLocation, DimDate,
    FactOrderItem, EtlWatermark, EtlRunLog, EtlBatchLog,
]
