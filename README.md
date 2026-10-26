# Supply Chain & Operations Analytics Pipeline

An **orchestrated, incremental** ETL pipeline — the companion piece to
`ecommerce-data-warehouse`. Where that project proves you can design a
star schema and load it cleanly, this one proves you can build a pipeline
that behaves the way a real, recurring, production pipeline has to:
resumable, safe to re-run, and driven by an orchestrator rather than a
script you run by hand.

## What this project demonstrates (and how it differs from the e-commerce project)

| Skill | Where | vs. ecommerce-data-warehouse |
|---|---|---|
| Designing a normalized schema from a flat file | `src/models.py` | that project's source data was already split into 8 relational tables; this one starts from ONE 53-column flat CSV |
| Incremental ETL with a watermark | `src/etl.py` | that project does a full refresh every run |
| Pipeline orchestration (Prefect: retries, logging) | `flows/pipeline_flow.py` | that project is a plain script |
| Resumable, batch-level failure handling | `src/etl.py`, `etl_batch_log` table | new here |
| SQL: joins, CTEs, window functions | `sql/*.sql` | same techniques, different domain — proves they transfer |
| Postgres-ready deployment | `docker/docker-compose.yml` | that project documents it; this one includes a runnable Postgres |

## The data

[DataCo Smart Supply Chain for Big Data Analysis](https://www.kaggle.com/datasets/shashwatwork/dataco-smart-supply-chain-for-big-data-analysis)
— ~180,000 order line items from a global retailer, January 2015 to
January 2018, covering shipping mode, delivery risk, customer segment,
product category, and profitability. One flat CSV, 53 columns, several
duplicate/near-duplicate columns (a very realistic messy-data quirk —
documented in `src/etl.py`'s `COLUMN_MAP`).

## Architecture

```
DataCoSupplyChainDataset.csv
        |
     extract()                                    (src/etl.py)
        |
     clean()  -- rename columns, parse dates, tag every row with
        |         its batch_month ('YYYY-MM')
        v
 for each PENDING batch (newer than the watermark):
        |
   transform_batch()  --> get-or-create dimension rows
        |                 (customer / product / location / date)
   load_batch()        --> bulk-insert fact rows for this batch
        |
   advance_watermark() --> commit batch + watermark together, atomically
        |
        v
   db/supply_chain.db  (SQLite by default, Postgres-ready)
        |
   sql/*.sql analytical queries
```

The whole batch loop above is wrapped by `flows/pipeline_flow.py` using
**Prefect** for retries and observability — see
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for exactly what that adds
and why the core logic in `src/etl.py` deliberately doesn't depend on
Prefect at all.

## Quickstart

```bash
# 1. install dependencies
pip install -r requirements.txt

# 2. download the raw dataset into data/raw/ (not committed to the repo —
#    see scripts/fetch_data.py for why, and for the manual/Kaggle alternative)
python scripts/fetch_data.py

# 3. run the pipeline (first run loads all history, in monthly batches)
python run_pipeline.py

# 4. run it again — the watermark means this finds nothing new to load
python run_pipeline.py

# 5. run the tests
pytest
```

Optional — run against real Postgres instead of SQLite:

```bash
cd docker && docker compose up -d && cd ..
pip install psycopg2-binary
export DATABASE_URL="postgresql+psycopg2://supply_chain:supply_chain@localhost:5432/supply_chain"
python run_pipeline.py
```

## Project structure

```
supply-chain-pipeline/
├── scripts/fetch_data.py     downloads the raw CSV into data/raw/ (run this first)
├── data/raw/              DataCoSupplyChainDataset.csv lives here once fetched (not modified)
├── db/                    built SQLite database lives here
├── src/
│   ├── db.py                database connection/engine setup
│   ├── models.py             SQLAlchemy ORM models: schema + control tables
│   └── etl.py                extract / clean / transform_batch / load_batch,
│                              the watermark, and the incremental run loop
├── flows/pipeline_flow.py    Prefect orchestration wrapping src/etl.py
├── sql/                    three analytical queries with explanatory comments
├── tests/                  pytest suite: incremental-loading + integrity checks
├── docker/docker-compose.yml   optional local Postgres
├── docs/
│   └── ARCHITECTURE.md       design decisions, explained
├── run_pipeline.py         single entrypoint
└── requirements.txt
```

## Sample insight

`sql/02_profitability_by_category.sql` surfaces categories that sell in
real volume but run a thin or negative margin once discounts are counted
— exactly the kind of thing a "total revenue" dashboard hides and a
margin-aware query catches.

## Attribution

Dataset: Fabian Constante, Fernando Silva, and António Pereira,
"DataCo Smart Supply Chain for Big Data Analysis", made available on
Kaggle. Used here for educational/portfolio purposes.

## Author

**Dr. Paul Kwaku Larbi Anderson**, Data Scientist & Social Scientist
[LinkedIn](https://www.linkedin.com/in/paul-kl-anderson) · [GitHub](https://github.com/Pakanderson)
