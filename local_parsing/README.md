# `local_parsing/` — run the Spark XML parser locally

```powershell
python local_parsing/run_parsing.py account daily     # one table, low-overhead local run
python local_parsing/run_parsing.py account history   # one table, full backfill on a real cluster
python local_parsing/run_parsing.py all daily         # every table in DATASETS, one after another
```

Two arguments — `<dataset> <mode>`. `<dataset>` is a key of `DATASETS` in
[`config.py`](config.py) (or `all`); `<mode>` is `daily` / `history`.
**Every setting is in [`config.py`](config.py)** — hard-coded: a `Dataset` per
source table, a `Job` per mode. Edit values there, not the script. With `all`,
one dataset failing is logged and the rest still run (exit code 1 if any failed).

## Files

| File | What |
|---|---|
| `run_parsing.py` | The local plumbing: `build_spark_session()`, the Oracle read, the Iceberg write, `run_one()` (one dataset), `main()` (arg parsing + per-dataset loop). Imports the parsing logic from `python_parsing.py`. |
| `python_parsing.py` | The XML-parsing library — a **faithful copy** of the repo-root `python_parsing.py` (only `scb.core.logger` → stdlib `logging`). `apply_xml_parsing`, `normalize_arrays`, `reconcile_iceberg_schema` + helpers. Diff fixes straight against the bank's file. |
| `config.py` | All settings. `DATASETS` (a `Dataset` per source table) + `JOBS` (`DAILY` / `HISTORY`) + shared Oracle/Iceberg/S3 constants. Only the 4 secrets (Oracle + MinIO user/password) are non-literal — read from the repo-root `.env` via `python-dotenv`. |
| `fetch_jars.ps1` | Downloads the 9 pinned jars into `jars/` (versions mirror `spark-operator/spark-custom-image/Dockerfile`). |
| `requirements.txt` | `pyspark==3.5.5`, `python-oracledb`, `python-dotenv` (+ a JDK 11/17). |

## `daily` vs `history`: every setting that differs, and what it costs

Parsing/reconcile logic (`python_parsing.py`) is byte-identical in both — nothing
here changes what gets computed, only how expensively Spark gets there. Times
below are **estimated ranges**, not measurements from this environment — Spark
startup costs are well-documented in general, but the honest way to get real
numbers here is `run_parsing.py`'s own per-phase timings (`session_build`,
`read`, `parse`, `write`, …), printed on every run. Treat these as "is this
worth doing" ballparks, not a benchmark result.

**Shared by both** (not a difference, despite looking like a `daily`-only trick
at first glance — `HISTORY.spark_conf` sets these identically):
`spark.sql.catalogImplementation=in-memory` (skips Hive metastore / embedded
Derby init — both profiles avoid it), `spark.jars=<local files>` (both load
already-downloaded jars, never resolve from Maven on launch), Arrow-based
Python↔JVM transfer. Driver heap for local runs comes from
`PYSPARK_SUBMIT_ARGS` in `.env` (default `--driver-memory 4g`), not the
`spark_conf` dict.

**Settings that actually differ:**

| Setting | `daily` | `history` | Est. time impact | Why |
|---|---|---|---|---|
| `spark.master` | `local[2]` (in-process) | `$SPARK_MASTER` — `local[*]` by default, a `spark://…`/`k8s://…` URL on a cluster | **~2–30s saved** by `daily` when history points at a real cluster (busy master + executor allocation = network round-trips a local JVM skips) | Usually the single biggest line item once history is on a cluster. |
| `reader` | `thin` (`python-oracledb`, single connection, driver-side) | `jdbc` (Spark's own reader, 16-way parallel) | Roughly a wash at `daily`'s volume (~a few seconds either way) | Not really a speed trick — `thin` exists because `daily`'s data is small enough that one connection is fine; it would *not* scale to `history`'s volume (see the earlier conversation on why). |
| `jars` loaded | 6 (`_BASE_JARS`) | 9 (+ `ojdbc8`, `xmlparserv2`, `xdb`) | **~1–3s saved** by `daily` | 3 fewer jars for the JVM to classload at `SparkContext` startup — `ojdbc8`/`xmlparserv2`/`xdb` are modest-sized, so this is a small line item, not a big one. |
| `spark.ui.enabled` | `false` | `true` | **~1–3s saved** by `daily` | Skips starting the embedded Jetty HTTP server for the Spark UI. |
| `spark.sql.shuffle.partitions` | `1` | `200` (Spark's own default) | **~2–10s saved per shuffle stage**, more if the pipeline hits several | 200 tasks get scheduled for *any* shuffle-triggering step (a `groupBy`/join inside parsing or reconcile) even when there's only a few thousand rows total — each task carries real per-task scheduling overhead regardless of how little data it holds. This is one of the most commonly-cited "small data on Spark" taxes. |
| `spark.default.parallelism` | `2` | not set (cluster-core-count default) | Bundled into the row above, not a separate large number | Same class of overhead as shuffle partitions, applied to RDD-level ops rather than DataFrame shuffles. |
| `spark.sql.adaptive.enabled`(+`coalescePartitions`) | `false` | `true` | **~1–3s saved** by `daily` — but a **net win** for `history`, not a cost there | AQE re-plans the query using runtime stats, which is pure overhead when there's nothing meaningful to re-optimize (`daily`'s tiny data) but a real optimization at `history`'s scale (better join strategies, fewer/larger output files). |
| driver address (`SPARK_LOCAL_IP` env) | `127.0.0.1` (from `.env`) | dropped by `run_parsing.py` for a non-local master → platform assigns the routable IP | **Highly variable — ~0s to 30s+, occasionally a much longer stall** | Loopback works around Windows being slow to resolve the local hostname. The runner only honours it for a local master, so on a real cluster it can't strand remote executors. |
| `spark.executor.instances`/`memory`/`cores`, `heartbeatInterval`, `network.timeout` | not set (single in-process JVM has no separate executors) | `4` × `4g`/`2 cores`, `60s`, `300s` | N/A — not a time cost, a capability `daily`'s execution model doesn't have | These only mean something once there's an actual cluster of separate executor processes to size and keep alive. |

**Rough total**: for `daily`'s actual data volume, these settings together are
plausibly saving somewhere in the **low tens of seconds** per run, dominated by
avoiding cluster negotiation and the 200-task shuffle-scheduling tax — proportionally
significant for a job whose useful work is itself only a few seconds, which is
the entire point of tuning it this way. At `history`'s volume, most of these
same settings would be actively *wrong* to copy over (no cluster parallelism,
AQE's real optimizations turned off, a driver address remote executors can't
reach) — this isn't "one config is just better," it's two profiles tuned for
genuinely different data sizes.

**`write_mode`/`reader`/`jdbc_num_partitions` aren't performance knobs at all** —
`merge` (daily, updates existing rows) vs `replace` (history, rewrites the whole
table) is a correctness choice matching what each job is actually for, not a
speed optimization.

## Setup (one time)

```powershell
# a JDK 11 or 17 on JAVA_HOME / PATH
pip install -r local_parsing/requirements.txt
pwsh local_parsing/fetch_jars.ps1
# Windows: winutils.exe + hadoop.dll in %HADOOP_HOME%\bin, on PATH
```

The Docker stack must be up (`docker compose up -d`); Oracle seeds itself on a
fresh DB via `init-scripts/00_setup.sh` (see repo-root README).

## What to edit in `config.py`

- **Add a source table** — a new `DATASETS` entry:
  ```python
  "customer": Dataset(
      name="customer",
      source_table="customer",
      target_table="bronze.customer_wide",
      date_field="c167",        # the XML tag under /row this table dates on
      # merge_key / oracle_schema / normalize_arrays default sensibly
  ),
  ```
  Then `run_parsing.py customer daily`. `lookup_metadata.csv` must have rows with
  `table_name = customer`.
- `Dataset.target_table` — `"bronze.account_wide"` **is** dbt's real table. Use
  `"bronze.account_wide_spark"` until this path is the deliberate cutover.
- `DAILY.window` — `("today","today")` by default; pin to `("20260101","20260101")`
  to reprocess a fixed day (on each dataset's `date_field`), or `None` for a full read.

## What to set in `.env` (per environment, no code change)

`config.py` reads these from the repo-root `.env` (via `python-dotenv`); the
defaults shown are the local Docker stack.

| Var(s) | Controls | Default |
|---|---|---|
| `ORACLE_HOST` / `ORACLE_PORT` / `ORACLE_SERVICE` | Oracle connection | `localhost` / `1521` / `XEPDB1` |
| `ORACLE_SCHEMA` | owner of the source tables (`Dataset.oracle_schema` default) | `source_user` |
| `ORACLE_APP_USER` / `ORACLE_APP_PASSWORD` | Oracle login | — |
| `ICEBERG_REST_URI` / `ICEBERG_WAREHOUSE` | Iceberg catalog (the bank's Lakekeeper in prod) | `http://localhost:8181` / `s3://warehouse/` |
| `MINIO_ENDPOINT` (or `S3_ENDPOINT`) / `AWS_REGION` (or `S3_REGION`) | object store | `http://localhost:9000` / `us-east-1` |
| `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | object-store credentials | — |
| `SPARK_LOCAL_IP` | driver's network address for **local** runs (dodges a slow Windows hostname lookup). `run_parsing.py` keeps it only when the master is `local[*]` and **drops it for a real cluster**, so the platform assigns the routable IP — effectively daily-only, safe to leave set. Read natively by Spark, not wired in `config.py`. | `127.0.0.1` in `.env` |
| `SPARK_MASTER` | **HISTORY** only — `local[*]` for one machine, `spark://…`/`k8s://…` for a cluster | `local[*]` |
| `SPARK_EXECUTOR_INSTANCES` / `_MEMORY` / `_CORES` | HISTORY executor sizing (ignored when `SPARK_MASTER` is `local[*]`) | `4` / `4g` / `2` |
| `PYSPARK_SUBMIT_ARGS` | driver JVM heap for **local** runs, e.g. `--driver-memory 4g pyspark-shell` (keep the trailing token). Read natively by PySpark at JVM launch; dropped by `run_parsing.py` for a non-local master. | `--driver-memory 4g pyspark-shell` |
| `SPARK_JDBC_NUM_PARTITIONS` | HISTORY parallel-read slices | `16` |

`DAILY` is always `local[2]` and, apart from `SPARK_LOCAL_IP`, needs none of the
`SPARK_*` vars. `CATALOG` / `NAMESPACE` stay hard-coded — they are the contract
shared with dbt/Trino.

## Verify

```powershell
docker compose exec trino trino --catalog iceberg --schema bronze `
  --execute "SELECT count(*) FROM account_wide"
```

`run_parsing.py` prints per-phase timings (`session_build`, `read`, `parse`,
`write`, …) so the startup cost is visible. Parity vs dbt: build dbt's
`account_wide` for the same window, then compare row counts / columns /
`JOIN ... USING (recid)`. Known diffs: `account_number` (no `<c0>` tag → NULL).
