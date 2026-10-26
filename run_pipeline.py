#!/usr/bin/env python3
"""
run_pipeline.py — the single command that runs the orchestrated pipeline.

Usage:
    python run_pipeline.py

Unlike the sibling ecommerce-data-warehouse project (full refresh every
run), this pipeline is INCREMENTAL: the first run loads all history in
monthly batches; every run after that only loads batches that haven't
been loaded yet, tracked via a watermark stored in the database itself
(see the etl_watermark table / src/models.py). Run this file twice in a
row and the second run will find nothing pending — that's expected, not
a bug, and is exactly what you want a real scheduled pipeline to do.

This delegates to flows/pipeline_flow.py, which wraps the core ETL logic
(in src/etl.py) with Prefect for retries and observability.
"""

import os
import sys
from pathlib import Path

# Quiet down Prefect's own background analytics/telemetry (unrelated to
# our pipeline's database) so pipeline output isn't drowned out by
# Prefect's internal ephemeral-server logging. Set before importing
# prefect. Users running this for real can override these normally.
os.environ.setdefault("PREFECT_SERVER_ANALYTICS_ENABLED", "false")
os.environ.setdefault("PREFECT_LOGGING_LEVEL", "WARNING")

sys.path.insert(0, str(Path(__file__).resolve().parent))

from flows.pipeline_flow import supply_chain_pipeline_flow


def main() -> None:
    result = supply_chain_pipeline_flow()
    print(f"\nDone. {result['rows_loaded']:,} rows loaded across "
          f"{result['batches_processed']} batch(es) this run.")
    print("Run it again to see the watermark in action (it should load 0 new batches).")
    print("Explore the data: sql/*.sql, or connect any SQLite browser to db/supply_chain.db")


if __name__ == "__main__":
    main()
