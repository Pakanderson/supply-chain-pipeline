"""
flows/pipeline_flow.py — orchestration layer, built with Prefect.

Everything that actually matters (extract/transform/load, the watermark,
the batch loop) lives in src/etl.py as plain Python functions with no
Prefect dependency at all — that's deliberate, so the core pipeline logic
is unit-testable without spinning up an orchestrator (see tests/test_etl.py).

This file's only job is to wrap that logic with the things an orchestrator
gives you for free:
  - RETRIES: if a batch fails (e.g. a transient database connection
    error), Prefect retries the task automatically. Because the pipeline
    only advances its watermark after a batch fully commits, a retry
    naturally resumes from the first batch that didn't complete — it
    doesn't reprocess batches that already succeeded.
  - LOGGING/OBSERVABILITY: every task run, its duration, and its result
    are visible in Prefect's UI/API, not just in a log file on disk.
  - SCHEDULING: in a real deployment you'd attach a schedule (e.g. "every
    day at 2am") to this flow instead of running it by hand. See the
    bottom of this file for how that's done.

Run it directly with:  python flows/pipeline_flow.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from prefect import flow, task
from prefect.logging import get_run_logger

from src.db import get_engine, DATABASE_URL
from src.etl import extract, run_incremental_pipeline


@task(retries=2, retry_delay_seconds=10, name="extract-raw-csv")
def extract_task():
    return extract()


@task(retries=2, retry_delay_seconds=30, name="load-pending-batches")
def load_pending_batches_task(raw):
    """
    The whole incremental batch loop, as a single retryable Prefect task.
    If this fails partway through (say, batch 2017-06 raises an error),
    src/etl.py has already committed batches up to 2017-05 and advanced
    the watermark to match — so when Prefect retries this task, it starts
    again from 2017-06, not from the beginning. That resumability is the
    entire point of the watermark design; the retry here just proves it.
    """
    logger = get_run_logger()
    engine = get_engine()
    logger.info("Target database: %s", DATABASE_URL)
    result = run_incremental_pipeline(engine, raw=raw)
    logger.info(
        "Pipeline pass complete: %d batch(es), %d row(s) loaded",
        result["batches_processed"], result["rows_loaded"],
    )
    return result


@flow(name="supply-chain-incremental-etl")
def supply_chain_pipeline_flow() -> dict:
    raw = extract_task()
    result = load_pending_batches_task(raw)
    return result


if __name__ == "__main__":
    outcome = supply_chain_pipeline_flow()
    print(f"\nDone. {outcome['rows_loaded']:,} rows loaded across "
          f"{outcome['batches_processed']} batch(es).")

    # --- Turning this into a real schedule ---------------------------------
    # To actually run this on a schedule instead of by hand, Prefect's
    # recommended path is `prefect deploy` (reads a prefect.yaml) or:
    #
    #   supply_chain_pipeline_flow.serve(
    #       name="daily-supply-chain-load",
    #       cron="0 2 * * *",     # every day at 02:00
    #   )
    #
    # That starts a long-running process that triggers the flow on the
    # given schedule. It's commented out here because a portfolio project
    # shouldn't leave a background process running — but this is exactly
    # what you'd add to move from "I run this by hand" to "this runs
    # itself every night."
