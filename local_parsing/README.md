# `local_parsing/` - run the Spark XML parser locally

```bash
python local_parsing/run_parsing.py account daily     # one table, low-overhead local run
python local_parsing/run_parsing.py account history   # one table, full backfill, local[*]
python local_parsing/run_parsing.py all daily         # every table in DATASETS, one after another

# reprocess a missed/old day, or a bounded slice of history, instead of the
# job's own default window (daily: today; history: everything):
python local_parsing/run_parsing.py account daily --start-date 20260909 --end-date 20260909
python local_parsing/run_parsing.py account history --start-date 20260901 --end-date 20260910
```

Two positional arguments - `<dataset> <mode>`. `<dataset>` is a key of
`DATASETS` in [`config.py`](config.py) (or `all`); `<mode>` is `daily` /
`history`. `--start-date`/`--end-date` (`YYYYMMDD`, must be given together)
override that run's window; omitting them keeps each job's own default
(`daily`: today; `history`: a full, unwindowed read). When `history` runs
with an explicit window, it writes with `merge` instead of its normal
`replace` - a windowed `replace` would wipe every row outside the window,
since `replace` is a full-table `createOrReplace()`; `merge` only touches
rows matched by `recid` in that window's batch, leaving the rest of the
table untouched.
**Every other setting is in [`config.py`](config.py)** - hard-coded: a
`Dataset` per source table, a `Job` per mode. Edit values there, not the
script. With `all`, one dataset failing is logged and the rest still run
(exit code 1 if any failed).

### Raw XML is landed permanently before parsing

T24 has a short retention window and deletes source rows after it passes -
whatever this reads from Oracle may be the only copy of that XML that will
ever exist. So before `apply_xml_parsing` touches anything, `run_one()`
writes the untouched raw XML to its own Iceberg table (`Dataset.raw_table`,
default `staging.<name>_raw_spark` - a separate table from dbt's
`iceberg.staging.account_raw`, so the two pipelines never write the same
table). This happens on **every** run, `daily` and `history` alike, and uses
a conditional merge on `recid` (only updates a row when its `xmlrecord`
actually changed), matching `dbt`'s `conditional_merge` macro. See the
`local-parsing-requirements` skill (Requirements 2 and 3) for the full
reasoning.

## Files

| File | What |
|---|---|
| `run_parsing.py` | The local plumbing: `build_spark_session()`, the Oracle read, `write_raw()` (permanent raw-XML landing), `write_result()` (the parsed Iceberg write), `run_one()` (one dataset), `main()` (`argparse` CLI + per-dataset loop). Imports the parsing logic from `python_parsing.py`. |
| `python_parsing.py` | The XML-parsing library - a **faithful copy** of the repo-root `python_parsing.py` (only `scb.core.logger` → stdlib `logging`). `apply_xml_parsing`, `normalize_arrays`, `reconcile_iceberg_schema` + helpers. Diff fixes straight against the bank's file. |
| `config.py` | All settings. `DATASETS` (a `Dataset` per source table) + `JOBS` (`DAILY` / `HISTORY`) + shared Oracle/Iceberg/S3 constants. Only the 4 secrets (Oracle + MinIO user/password) are non-literal - read from the repo-root `.env` via `python-dotenv`. |
| `fetch_jars.sh` | Downloads the 9 pinned jars into `jars/` (versions mirror `spark-operator/spark-custom-image/Dockerfile`). Safe to re-run - skips any jar already present. |
| `requirements.txt` | `pyspark==3.5.5`, `python-dotenv` (+ a JDK 11/17). |

## `daily` vs `history`: every setting that differs, and what it costs

Parsing/reconcile logic (`python_parsing.py`) is byte-identical in both - nothing
here changes what gets computed, only how expensively Spark gets there. Times
below are **estimated ranges**, not measurements from this environment - Spark
startup costs are well-documented in general, but the honest way to get real
numbers here is `run_parsing.py`'s own per-phase timings (`session_build`,
`read`, `parse`, `write`, …), printed on every run. Treat these as "is this
worth doing" ballparks, not a benchmark result.

**Shared by both** (not a difference, despite looking like a `daily`-only trick
at first glance - `HISTORY.spark_conf` sets these identically):
`spark.sql.catalogImplementation=in-memory` (skips Hive metastore / embedded
Derby init - both profiles avoid it), `spark.jars=<local files>` (both load
already-downloaded jars, never resolve from Maven on launch), Arrow-based
Python↔JVM transfer. Driver heap for local runs comes from
`PYSPARK_SUBMIT_ARGS` in `.env` (default `--driver-memory 4g`), not the
`spark_conf` dict.

**Also shared, and *not* optimized away**: the `write_raw` phase (raw XML →
its own Iceberg table, before parsing) runs on every `daily` and `history`
invocation alike, adding one more Iceberg write to each. This is accepted,
correctness-driven overhead, not a gap in the tuning above - T24's retention
window means an unpersisted read may be the only copy of that data that will
ever exist. See the `local-parsing-requirements` skill, Requirement 2.

**Settings that actually differ:**

| Setting | `daily` | `history` | Est. time impact | Why |
|---|---|---|---|---|
| `spark.master` | `local[2]` (in-process, 2 threads) | `local[*]` (in-process, every core on this machine) | none - both are single-machine, no cluster negotiation | Pinned to `local[*]` rather than read from `SPARK_MASTER`: this subtask has no access to the bank's Kubernetes cluster or any account/config to point at (see the `local-parsing-requirements` skill, Requirement 1). If/when cluster access exists, this becomes a real, config-only difference again - no code change needed beyond re-introducing the env var. |
| `reader` | `jdbc`, 1 partition (single connection) | `jdbc`, 16-way parallel | none | Both jobs read via Spark JDBC now - `daily` used a separate lightweight `python-oracledb` ("thin") reader previously, removed once both jobs needed the same `ojdbc8`/`xmlparserv2`/`xdb` jars anyway (see the `local-parsing-requirements` skill, Requirement 1's Update note) and `oracledb` dropped from `requirements.txt`. `daily` still uses only 1 partition - one connection is already fast enough at its volume, no reason to add JDBC's partition-planning overhead for nothing. |
| `jars` loaded | 9 (`_JARS`, same list) | 9 (`_JARS`, same list) | none | Both jobs load the same jar list now - no jar-count difference to save time on. |
| `spark.ui.enabled` | `false` | `true` | **~1–3s saved** by `daily` | Skips starting the embedded Jetty HTTP server for the Spark UI. |
| `spark.sql.shuffle.partitions` | `1` | `200` (Spark's own default) | **~2–10s saved per shuffle stage**, more if the pipeline hits several | 200 tasks get scheduled for *any* shuffle-triggering step (a `groupBy`/join inside parsing or reconcile) even when there's only a few thousand rows total - each task carries real per-task scheduling overhead regardless of how little data it holds. This is one of the most commonly-cited "small data on Spark" taxes. |
| `spark.default.parallelism` | `2` | not set (defaults to this machine's core count under `local[*]`) | Bundled into the row above, not a separate large number | Same class of overhead as shuffle partitions, applied to RDD-level ops rather than DataFrame shuffles. |
| `spark.sql.adaptive.enabled`(+`coalescePartitions`) | `false` | `true` | **~1–3s saved** by `daily` - but a **net win** for `history`, not a cost there | AQE re-plans the query using runtime stats, which is pure overhead when there's nothing meaningful to re-optimize (`daily`'s tiny data) but a real optimization at `history`'s scale (better join strategies, fewer/larger output files). |
| `spark.sql.codegen.wholeStage` | `false` | not set (`true`, Spark's default) | **~0.1–1s saved per query/stage**, compounding across `apply_xml_parsing`'s per-field `transform`/`filter` expressions | Whole-stage codegen JIT-compiles a Java class per stage via Janino before running it - worthwhile once a stage processes enough rows to amortize the compile, a net loss on `daily`'s few-thousand-row batches. Same "small data on Spark" tax as the shuffle-partitions row above, just at the codegen layer instead of the scheduler layer; `history`'s volume is exactly where this compilation cost pays for itself, so it stays on there. |
| driver address (`SPARK_LOCAL_IP` env) | `127.0.0.1` (from `.env`) | same - `127.0.0.1` (from `.env`) | none | Loopback works around Windows being slow to resolve the local hostname; applies equally to both jobs now that both are always local. (Not currently dropped for a cluster master - there's no cluster to point at; see the `spark.master` row above.) |

**Rough total**: for `daily`'s actual data volume, these settings together are
plausibly saving somewhere in the **low tens of seconds** per run, dominated by
the 200-task shuffle-scheduling tax and per-stage codegen compilation -
proportionally significant for a job whose useful work is itself only a few
seconds, which is the entire point of tuning it this way. At `history`'s
volume, most of these same settings would be actively *wrong* to copy over
(AQE's real optimizations turned off, whole-stage codegen disabled right when
it'd start paying for itself) - this isn't "one config is just better," it's
two profiles tuned for genuinely different data sizes, independent of the fact
that both currently run on this one machine.

**`write_mode`/`reader`/`jdbc_num_partitions` aren't performance knobs at all** -
`merge` (daily, updates existing rows) vs `replace` (history, rewrites the whole
table) is a correctness choice matching what each job is actually for, not a
speed optimization.

## Setup (one time)

```bash
# a JDK 11 or 17 on JAVA_HOME / PATH, e.g.: sudo apt install openjdk-17-jdk-headless
pip install -r local_parsing/requirements.txt
bash local_parsing/fetch_jars.sh
# no winutils.exe/hadoop.dll needed on Linux (that's a Windows-only Hadoop requirement)
```

No system JDK available (or don't want to install one)? A JDK can instead live
right next to the venv, at `local_parsing/.jdk/` - export `JAVA_HOME` to point
at it before running (`export JAVA_HOME="$(pwd)/local_parsing/.jdk"`, then
prepend `$JAVA_HOME/bin` to `PATH`). Adding those two lines to the end of
`local_parsing/.venv/bin/activate` makes them apply automatically on every
`source local_parsing/.venv/bin/activate`.

If `pip install` fails with `externally-managed-environment` (recent Debian/Ubuntu),
use a venv instead: `python3 -m venv .venv && source .venv/bin/activate` (installing
`python3-venv` first via your own `sudo apt install python3-venv` if that command
itself is missing), then re-run `pip install -r local_parsing/requirements.txt`
inside it. If a venv genuinely isn't an option, `pip install --user
--break-system-packages -r local_parsing/requirements.txt` is a user-scoped
fallback that doesn't touch system-managed packages.

The Docker stack must be up (`docker compose up -d`); Oracle seeds itself on a
fresh DB via `init-scripts/00_setup.sh` (see repo-root README).

## What to edit in `config.py`

- **Add a source table** - a new `DATASETS` entry:
  ```python
  "customer": Dataset(
      name="customer",
      source_table="customer",
      target_table="bronze.customer_wide",
      date_field="c167",        # the XML tag under /row this table dates on
      # merge_key / oracle_schema / normalize_arrays / raw_table default sensibly
  ),
  ```
  Then `run_parsing.py customer daily`. `lookup_metadata.csv` must have rows with
  `table_name = customer`.
- `Dataset.target_table` - `"bronze.account_wide"` **is** dbt's real table. Use
  `"bronze.account_wide_spark"` until this path is the deliberate cutover.
- `Dataset.raw_table` - defaults to `"staging.<name>_raw_spark"`; override only
  if a table needs a different raw-landing name.
- `DAILY.window` / `HISTORY.window` - each job's *default* window when no
  `--start-date`/`--end-date` is given on the CLI (`("today","today")` /
  `None` respectively). Prefer the CLI flags for a one-off reprocess; edit
  these only to change the job's standing default.

## What to set in `.env` (per environment, no code change)

`config.py` reads these from the repo-root `.env` (via `python-dotenv`); the
defaults shown are the local Docker stack.

| Var(s) | Controls | Default |
|---|---|---|
| `ORACLE_HOST` / `ORACLE_PORT` / `ORACLE_SERVICE` | Oracle connection | `localhost` / `1521` / `XEPDB1` |
| `ORACLE_SCHEMA` | owner of the source tables (`Dataset.oracle_schema` default) | `source_table` |
| `ORACLE_APP_USER` / `ORACLE_APP_PASSWORD` | Oracle login | `system` / - |
| `ICEBERG_REST_URI` / `ICEBERG_WAREHOUSE` | Iceberg catalog (the bank's Lakekeeper in prod) | `http://localhost:8181` / `s3://warehouse/` |
| `MINIO_ENDPOINT` (or `S3_ENDPOINT`) / `AWS_REGION` (or `S3_REGION`) | object store | `http://localhost:9000` / `us-east-1` |
| `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | object-store credentials | - |
| `SPARK_LOCAL_IP` | driver's network address (dodges a slow Windows hostname lookup). Applies to both jobs - both are always `local[...]` on this one machine (no bank cluster access for this subtask; see the `local-parsing-requirements` skill, Requirement 1). Read natively by Spark, not wired in `config.py`. | `127.0.0.1` in `.env` |
| `PYSPARK_SUBMIT_ARGS` | driver JVM heap, e.g. `--driver-memory 4g pyspark-shell` (keep the trailing token). Read natively by PySpark at JVM launch. Applies to both jobs. | `--driver-memory 4g pyspark-shell` |
| `SPARK_JDBC_NUM_PARTITIONS` | HISTORY's local read parallelism (JDBC reader partitions, still useful under `local[*]` - more concurrent read tasks across this machine's cores) | `16` |

`DAILY` is always `local[2]` and, apart from `SPARK_LOCAL_IP`, needs none of the
`SPARK_*` vars. `CATALOG` / `NAMESPACE` stay hard-coded - they are the contract
shared with dbt/Trino.

## Verify

```bash
docker compose exec trino trino --catalog iceberg --schema bronze \
  --execute "SELECT count(*) FROM account_wide"
```

`run_parsing.py` prints per-phase timings (`session_build`, `read`, `parse`,
`write`, …) so the startup cost is visible. Parity vs dbt: build dbt's
`account_wide` for the same window, then compare row counts / columns /
`JOIN ... USING (recid)`. Known diffs: `account_number` (no `<c0>` tag → NULL).
