"""
tests/test_etl.py

Run with: pytest

These tests specifically target the thing that makes this project
different from the sibling ecommerce-data-warehouse project: INCREMENTAL,
WATERMARK-DRIVEN loading. A full-refresh pipeline only really needs an
idempotency test ("run it twice, same result"). This pipeline needs that
PLUS proof that:

  - a fresh database with no watermark loads everything
  - a database that already has SOME batches loaded only loads the NEW
    ones on the next run, not everything again
  - the watermark actually advances to reflect what got loaded
"""

import sys
from pathlib import Path

import pytest
from sqlalchemy import create_engine, select, func, text
from sqlalchemy.orm import Session

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.etl import extract, clean, list_available_batches, get_pending_batches, run_incremental_pipeline
from src.models import DimCustomer, DimProduct, FactOrderItem, EtlWatermark


@pytest.fixture(scope="module")
def raw_data():
    return extract()


@pytest.fixture(scope="module")
def cleaned(raw_data):
    return clean(raw_data)


def test_extract_reads_expected_shape(raw_data):
    assert len(raw_data) > 100_000
    assert "Order Id" in raw_data.columns


def test_clean_produces_monthly_batches(cleaned):
    batches = list_available_batches(cleaned)
    assert len(batches) > 12  # more than a year of data
    # batches should be sorted chronologically, e.g. '2015-01' < '2015-02'
    assert batches == sorted(batches)


def make_engine(tmp_path, name="test.db"):
    return create_engine(f"sqlite:///{tmp_path / name}", connect_args={"check_same_thread": False})


def test_fresh_database_loads_all_batches(tmp_path, raw_data, cleaned):
    engine = make_engine(tmp_path)
    all_batches = list_available_batches(cleaned)

    result = run_incremental_pipeline(engine, raw=raw_data)

    assert result["batches_processed"] == len(all_batches)
    assert result["rows_loaded"] == len(cleaned)


def test_second_run_is_a_no_op(tmp_path, raw_data):
    engine = make_engine(tmp_path)
    run_incremental_pipeline(engine, raw=raw_data)          # first run: loads everything
    result = run_incremental_pipeline(engine, raw=raw_data)  # second run: should find nothing pending

    assert result["batches_processed"] == 0
    assert result["rows_loaded"] == 0


def test_incremental_loading_only_processes_new_batches(tmp_path, raw_data, cleaned):
    """The core promise of this pipeline, tested directly: give it a
    subset of history first, then the full dataset, and check the second
    call only loads what wasn't there before."""
    engine = make_engine(tmp_path)

    all_batches = list_available_batches(cleaned)
    first_two_batches = set(all_batches[:2])
    subset_mask = cleaned["batch_month"].isin(first_two_batches)
    raw_subset = raw_data[subset_mask.values]

    first_result = run_incremental_pipeline(engine, raw=raw_subset)
    assert first_result["batches_processed"] == 2

    # now "new data arrives" — run again with the FULL dataset
    second_result = run_incremental_pipeline(engine, raw=raw_data)
    assert second_result["batches_processed"] == len(all_batches) - 2
    assert second_result["rows_loaded"] == len(cleaned) - subset_mask.sum()

    with Session(engine) as session:
        wm = session.get(EtlWatermark, "supply_chain_orders")
        assert wm.last_loaded_batch == all_batches[-1]


def test_referential_integrity_after_full_load(tmp_path, raw_data):
    engine = make_engine(tmp_path)
    run_incremental_pipeline(engine, raw=raw_data)

    with Session(engine) as session:
        orphan_customers = session.execute(
            select(func.count()).select_from(FactOrderItem)
            .where(FactOrderItem.customer_key.is_not(None))
            .where(~FactOrderItem.customer_key.in_(select(DimCustomer.customer_key)))
        ).scalar_one()
        orphan_products = session.execute(
            select(func.count()).select_from(FactOrderItem)
            .where(FactOrderItem.product_key.is_not(None))
            .where(~FactOrderItem.product_key.in_(select(DimProduct.product_key)))
        ).scalar_one()

    assert orphan_customers == 0
    assert orphan_products == 0


def test_no_duplicate_order_items_after_incremental_then_full_run(tmp_path, raw_data, cleaned):
    """Combine the incremental scenario with a duplicate check — the
    scenario most likely to introduce duplicates if the watermark logic
    had an off-by-one bug (e.g. '>=' instead of '>' when comparing
    batches)."""
    engine = make_engine(tmp_path)
    all_batches = list_available_batches(cleaned)
    subset_mask = cleaned["batch_month"].isin(set(all_batches[:5]))
    raw_subset = raw_data[subset_mask.values]

    run_incremental_pipeline(engine, raw=raw_subset)
    run_incremental_pipeline(engine, raw=raw_data)

    with Session(engine) as session:
        total = session.execute(select(func.count()).select_from(FactOrderItem)).scalar_one()
        distinct_pairs = session.execute(
            text("SELECT COUNT(*) FROM (SELECT DISTINCT order_id, order_item_id FROM fact_order_item)")
        ).scalar_one()

    assert total == len(cleaned)
    assert total == distinct_pairs
