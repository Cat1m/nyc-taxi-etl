# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

Batch ETL pipeline for NYC TLC Yellow Taxi trip data, built as a learning project /
portfolio piece. Medallion architecture (Bronze → Silver → Gold):

- **DuckDB** as both the compute engine and the local warehouse (`warehouse.duckdb`
  at repo root)
- **Great Expectations (GX)** validates Bronze (distribution/boundary checks,
  not just binary constraints) before Silver runs
- **dbt (dbt-duckdb)** for transforms/modeling/testing (Silver + Gold layers)
- **Airflow (LocalExecutor)**, run via docker-compose, for orchestration
  (download → load_bronze → validate_bronze (GX) → dbt_run → dbt_test)
- **Metabase**, run via docker-compose (separate from Airflow), for the
  dashboard — reads the Gold layer read-only

No Spark/BigQuery yet — see README.md "Future work" for what's left: a
dashboard chart tracking GX results over time, cleaning up the `payment_type =
0` handling, then PySpark/BigQuery last (bigger architectural changes).

README.md is in Vietnamese and is the source of truth for project goals, demo
queries, and status; keep it in sync with major milestones if asked to update
progress.

## Current state

All 8 steps (scaffold, ingestion, bronze load, dbt Silver/Gold, Airflow
orchestration, portfolio polish, Great Expectations on Bronze, Metabase
dashboard) are implemented — see README.md "Trạng thái hiện tại" for the
checklist. `warehouse.duckdb` locally has 4 months backfilled (2023-01 through
2023-04).

## Commands

```bash
# setup (Windows, from repo root)
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt

# one-command local run (ingest -> GX validate -> dbt run/test for one month;
# skips Docker-only steps (Airflow, Metabase) and prints what to do next)
python quickstart.py [--year-month 2023-01]

# ingestion: download a month + profile it, then load into bronze (idempotent)
python ingestion/download_tlc.py --year-month 2023-01
python ingestion/load_bronze.py --year-month 2023-01

# dbt (always from repo root — see path gotcha below)
dbt deps --project-dir transform --profiles-dir transform
dbt seed --project-dir transform --profiles-dir transform
dbt run  --project-dir transform --profiles-dir transform
dbt test --project-dir transform --profiles-dir transform
dbt docs generate --project-dir transform --profiles-dir transform
dbt docs serve --project-dir transform --profiles-dir transform --port 8080

# Airflow (from orchestration/, requires Docker Desktop running)
cd orchestration
docker compose build
docker compose up airflow-init          # first time only, or after DB reset
docker compose up -d airflow-webserver airflow-scheduler
# UI: http://localhost:8081 (airflow/airflow)
```

Full command reference, including Postgres metadata DB inspection and
troubleshooting steps actually hit during development (DuckDB lock errors,
scheduler/pause race condition), is in `docs/commands.md` — check there before
re-deriving a command from scratch.

There is no pytest/lint config; dbt test (`dbt test ...` above) is the
correctness check for the transform layer.

## Architecture

```
NYC TLC (Parquet files)
        │  ingestion/download_tlc.py
        ▼
   Bronze layer        bronze_yellow_trips, bronze_taxi_zone_lookup — raw, untouched
        │  quality/validate_bronze.py (Great Expectations, read-only)
        │  distribution/boundary checks — blocks the pipeline on failure
        │  dbt staging (stg_trips)
        ▼
   Silver layer        typed, renamed columns, junk rows filtered (see comment
        │               block at top of stg_trips.sql for exactly which filters
        │               and why — it deliberately does NOT filter payment_type=0
        │               or unusual RatecodeID values; those are real trips, not junk)
        │  dbt marts
        ▼
   Gold layer           star schema: fact_trips + dim_datetime, dim_location,
                        dim_vendor, dim_payment
        │  read-only
        ▼
   Metabase dashboard   docker-compose, independent of Airflow; 5 charts today
```

Each layer is a separate DuckDB table/model — nothing is overwritten in place,
so transforms can be re-run and traced without re-downloading data.

Star schema notes:
- `dim_location` uses NYC TLC's own `LocationID` as a natural key (no
  surrogate key needed); `dim_datetime` and `fact_trips` use surrogate keys
  (`dbt_utils.generate_surrogate_key`) since no natural key exists.
- `fact_trips.trip_key`'s `unique` test is `severity: warn`, not `error` — a
  known, verified-benign collision (~12 rows out of ~6M for one month, distinct
  trips that happen to share vendor+pickup+dropoff+locations) because TLC
  provides no real trip_id. See the column description in `_marts_models.yml`
  before "fixing" this by tightening the key further.
- Analysts/BI tools are expected to `JOIN fact_trips` with the `dim_*` tables
  themselves at query time — no model pre-joins them, by design (normalization).

Orchestration: `orchestration/dags/nyc_taxi_pipeline.py` derives `year_month`
from the DAG run's own logical date (`{{ ds[:7] }}`) rather than a hardcoded
value, so `airflow dags backfill` / catchup naturally produces one correct run
per month. `max_active_runs=1` is required because DuckDB is single-writer —
two concurrent DAG runs would both try to write `warehouse.duckdb`.

## Key conventions

- **Idempotency pattern**: `ingestion/load_bronze.py` uses delete-by-partition
  (`DELETE ... WHERE year_month = X`) then insert for `bronze_yellow_trips`
  (partitioned data), and `CREATE OR REPLACE TABLE` for `bronze_taxi_zone_lookup`
  (a static, non-partitioned lookup). Re-running for the same month must never
  duplicate rows — verify this property is preserved if you touch this file.
- **Path resolution gotcha**: `transform/profiles.yml`'s DuckDB `path` is
  resolved relative to the **current working directory when the command is
  run**, not relative to `profiles.yml`'s own location. Always invoke
  `dbt`/`duckdb` from the repo root (see Commands above) — running from inside
  `transform/` will create/open a wrong `warehouse.duckdb` elsewhere. This has
  already caused one stray duplicate warehouse file outside the repo; don't
  reintroduce a relative `../` path.
- `data/raw/` and `data/staging/` are gitignored (regenerated by ingestion
  scripts); only `.gitkeep` is tracked. Never assume Parquet files there are
  committed.
- `ingestion/download_tlc.py` downloads are skip-if-exists (idempotent by
  filename), not content-hash based.
- On Windows, stdout is reconfigured to UTF-8 in scripts that print DuckDB's
  `.show()` output, since box-drawing characters crash under legacy cp1252
  consoles — keep this pattern in any new script that prints DuckDB tables.
- Code comments and docstrings in this repo are written in Vietnamese
  explaining the *why* (rationale, trade-offs, what was verified before a
  filtering decision was made — e.g. the block comment in `stg_trips.sql`);
  match this style/language when extending existing files rather than
  switching to English mid-file.
- `docs/data_profiling_2023-01.md` is the evidentiary basis for every filter
  rule in `stg_trips.sql`. If you change a filter threshold, verify against
  this doc (or re-profile) rather than guessing a new threshold.
- **GX data context split**: `quality/gx/expectations/` and
  `quality/gx/checkpoints/` are source, committed; `quality/gx/uncommitted/`
  (including Data Docs HTML) is generated output, gitignored. Each run is
  tagged with `run_name = year_month`, so Data Docs accumulate history across
  months instead of being overwritten — don't change this to Ephemeral context.
- `quality/validate_bronze.py` reads Bronze via a **read-only** DuckDB
  connection and exits non-zero on any expectation failure, so Airflow's
  BashOperator blocks `dbt_run` the same way `dbt_test` already blocks the
  pipeline downstream — preserve this exit-code contract if you touch it.
- `dashboard/` (Metabase via docker-compose) is intentionally decoupled from
  `orchestration/`'s docker-compose — it only reads the Gold layer, never
  writes `warehouse.duckdb`.
