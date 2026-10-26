# Architecture & design decisions

Read `ecommerce-data-warehouse/docs/ARCHITECTURE.md` first for the general
star-schema reasoning (dimensions, facts, surrogate keys, grain) — it
applies here too and isn't repeated. This document covers what's specific
to THIS project: incremental loading and orchestration.

## 1. Why incremental instead of full refresh?

The sibling project reprocesses everything on every run — reasonable at
its data size (~90MB, ~20 seconds to reload). Incremental loading is what
you reach for once that stops being true: when the source data is too
large to fully reprocess on every run, or when "reprocess everything"
would mean re-deriving history you don't actually need to touch again.

This project simulates that by treating the static 3-year CSV as if it
arrived one month at a time — the same shape as a real feed that lands
daily/hourly, just at a batch size that's convenient to demo (37 monthly
batches instead of ~1,100 daily ones; the mechanism is identical either
way, only the batch key would change from `'YYYY-MM'` to a full date).

## 2. The watermark pattern

A watermark is a small piece of state — here, one row in `etl_watermark`
holding `last_loaded_batch` — that answers "how far did I get last time?"
Every run:

1. reads the watermark
2. computes which batches in the source are newer than it
3. processes only those
4. after EACH batch fully commits, advances the watermark to match

Step 4 happening per-batch (not once at the end) is what makes a crash
mid-run safe: if batch 14 of 37 fails, batches 1-13 are already committed
and the watermark already points at batch 13. Re-running the pipeline
doesn't reprocess 1-13 and doesn't skip 14 — it just picks up at 14. This
combination — safe to re-run (idempotent) AND only touching new data
(incremental) — is the actual production-grade pattern, and it's stronger
than either property alone.

`tests/test_etl.py::test_incremental_loading_only_processes_new_batches`
proves this isn't just a design claim: it loads 2 batches, then hands the
pipeline the full dataset, and checks the second run loads exactly the
remaining 35 — not 37, not 0.

## 3. Incremental dimensions ("get-or-create")

Because batches load over multiple runs, dimension tables can't be
rebuilt from scratch each time (that would delete customers/products
already referenced by previously-loaded fact rows). Instead,
`DimensionCache` in `src/etl.py` loads the existing business-key ->
surrogate-key mapping from the database once per run, and for each new
row either reuses an existing surrogate key or creates one on the fly.
This is the standard "get-or-create" pattern for dimensions that grow
over time, and it's the piece that would need to evolve into a proper
Slowly Changing Dimension (SCD Type 1 or 2) strategy if customer/product
*attributes* — not just new members — could change between batches. Worth
naming that limitation directly if asked "what doesn't this handle?".

## 4. Why Prefect, and why the core logic doesn't depend on it

`src/etl.py` has zero Prefect imports. `flows/pipeline_flow.py` is a thin
wrapper that adds:

- **retries** on the extract and load steps, with backoff
- **observability** — every task run's duration/outcome is visible via
  Prefect, not just in whatever log file happens to exist
- a documented path to **scheduling** (see the comment at the bottom of
  `flows/pipeline_flow.py`)

Keeping the core pipeline logic orchestrator-agnostic is deliberate: it
means `tests/test_etl.py` can unit-test the actual ETL behavior — the
incremental loading, the referential integrity, the idempotency — without
needing Prefect running at all, and it means swapping Prefect for Airflow
later would only touch `flows/`, not `src/`.

## 5. Why the fact table is flatter than the e-commerce project's

The e-commerce project has four separate fact tables (order, order item,
payment, review) because those are genuinely different grains that came
from four separate source files. This project's source is a single flat
CSV already at "one row per order line item" — so there's one fact table,
`fact_order_item`, at that same grain. Splitting it further wouldn't add
anything; matching the schema's complexity to what the *data* actually
supports, rather than adding tables for their own sake, is itself a
design decision worth being able to explain.
