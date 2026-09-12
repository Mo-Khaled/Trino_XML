# Four parsing pipelines - how to run each

This repo now has four separate implementations of the same T24 XML-parsing
logic, at different points in the project's history. This file is a map:
what each one is, how it's actually run, and how they relate to each other.
It doesn't restate their semantics - each has its own README with the real
detail; this is the entry point for "which one do I run, and how."

| | `dbt/` | `trino_parsing/` | `local_parsing/` | `python_parsing.py` |
|---|---|---|---|---|
| Engine | Trino (regex-tokenized) | Trino (regex-tokenized) | Spark (real XML parser, via a local runner) | Spark (real XML parser) |
| Status | **Active, current pipeline** | Frozen reference - this is what `dbt/` was ported from | **Second active pipeline** - a live-connected local runner, ported from `python_parsing.py` | The bank's production reference - what the other three were built to match semantically |
| Orchestration | `dbt run` / `dbt run-operation` / `dbt test` | None - generates SQL text, run by hand in DBeaver | `python run_parsing.py <dataset> <daily\|history>` | None here - library functions, called from a Databricks/Spark job that lives outside this repo |
| Runnable as-is in this repo? | Yes | Yes | Yes | **No** - see below |

If you only need to actually run something today, that's `dbt/` (the
production-track pipeline) or `local_parsing/` (a live Spark run against the
same local stack). The other two are for understanding what it's built on
and what it's matched against.

---

## 1. `dbt/` - the active pipeline

Full detail: [`dbt/README.md`](dbt/README.md).

**One-time setup:**
```bash
python -m pip install dbt-trino
cd dbt
export DBT_PROFILES_DIR=.
dbt debug
dbt seed          # loads lookup_metadata locally from seeds/lookup_metadata.csv
```

**Day-to-day run** (from `dbt/`, with the stack up via `docker compose up -d`
at the repo root):
```bash
# 1. Ingest + tokenize. Omit --vars for a full read (first run, or a full resync).
dbt run --select account_raw account_attributes --vars '{start_date: "20260902", end_date: "20260902"}'

# 2. Widen account_wide's schema BEFORE pivoting, if this batch needs it.
#    Always safe to run -- no-ops if account_wide doesn't exist yet or nothing changed.
dbt run-operation reconcile_wide_schema --args '{table_name: account}'

# 3. Pivot the (possibly windowed) changed rows into account_wide.
dbt run --select account_wide --vars '{start_date: "20260902", end_date: "20260902"}'

# 4. Verify.
dbt test
```

For a different table, swap `account` for that table's name everywhere above
(`customer_raw customer_attributes`, `{table_name: customer}`, `customer_wide`)
- each table needs its own model files under `models/staging/<table>/` and
`models/bronze/<table>/` first (see `dbt/README.md`'s "Not yet done" section
for exactly what to add).

This is the only one of the three that implements the *full* Spark-matching
logic - unpinned-`m_index` fields (Branches 2/3), not just the pinned-`m_index`
case, and explicit stale-column handling. The other two don't have this yet.

---

## 2. `trino_parsing/` - the frozen Trino-native reference

Full detail: [`trino_parsing/README.md`](trino_parsing/README.md).

This is a pure text generator (`gen_sql.py`) - it never connects to anything.
You run it, get a `.sql` file, and paste that into DBeaver's Trino editor by
hand. `reconcile_wide_schema.py` is the one exception - a live-connected
Python script for the schema-widening step, since that step needs to inspect
live table state and can't be pre-generated as static SQL.

**Setup:**
```bash
cd trino_parsing
python -m pip install trino   # only needed for reconcile_wide_schema.py
```

**First-time bootstrap** (run each generated file in DBeaver's Trino editor
as it's produced):
```bash
python gen_sql.py --mode ingest    --table account > sql/account/01_ingest_bootstrap.sql
python gen_sql.py --mode bootstrap --table account > sql/account/03_bootstrap.sql
python gen_sql.py --mode wide      --table account --lookup ../reference/lookup_metadata.csv > sql/account/06_wide.sql
```

**Daily refresh** (same shape, with that day's window):
```bash
python gen_sql.py --mode ingest-refresh --table account --start-date 20260902 --end-date 20260902 > refresh.sql
python gen_sql.py --mode incremental    --table account --start-date 20260902 --end-date 20260902 > incr.sql
python reconcile_wide_schema.py --table account --apply incremental --start-date 20260902 --end-date 20260902
```
(`reconcile_wide_schema.py` replaces `gen_sql.py --mode wide`/`wide-incremental`
entirely - it does the pivot itself once it's done any needed widening, so
those two modes exist in `gen_sql.py` but normally aren't the thing you run.)

**Known limitation, not shared by `dbt/`**: `reconcile_wide_schema.py` only
widens between two shapes, `VARCHAR` and `ARRAY(VARCHAR)` - every lookup row
here is assumed to pin an exact `m_index` (blank defaults to `1`), so there's
no unpinned-`m_index`/nested-array case to handle. `dbt/`'s port fixed this;
this script was left as-is once the dbt project superseded it.

---

## 3. `local_parsing/` - the live Spark runner

Full detail: [`local_parsing/README.md`](local_parsing/README.md).

Unlike `trino_parsing/`'s text-generator and the repo-root `python_parsing.py`
(library code with no entrypoint), this one actually connects and runs: a
real `SparkSession`, a real JDBC read from Oracle, and a real write into this
repo's Iceberg stack. `python_parsing.py` in this directory is a faithful
copy of the repo-root file (only the logger is swapped for stdlib
`logging`), so fixes can be diffed straight against the bank's original.

**One-time setup:**
```bash
cd local_parsing
pip install -r requirements.txt   # + a JDK 11/17 on JAVA_HOME/PATH
bash fetch_jars.sh
```

**Day-to-day run** (from the repo root, with the stack up via
`docker compose up -d`):
```bash
python local_parsing/run_parsing.py account daily     # one table, low-overhead local run
python local_parsing/run_parsing.py account history   # one table, full backfill

# reprocess a missed/old day, or a bounded slice of history:
python local_parsing/run_parsing.py account daily --start-date 20260909 --end-date 20260909
```

This is the only one of the three Trino/Spark-engine pipelines here that
does the parsing itself in Spark, the same engine (and the same
`apply_xml_parsing`/`normalize_arrays`/`reconcile_iceberg_schema` functions)
as the bank's actual production reference - `dbt/` and `trino_parsing/`
re-implement the same semantics in Trino SQL instead.

---

## 4. `python_parsing.py` - the Spark reference (not runnable here)

This is **library code**, not a script - a handful of functions
(`apply_xml_parsing`, `reconcile_iceberg_schema`, etc.) meant to be imported
and called from inside the bank's actual Databricks/Spark job. There's no
`if __name__ == "__main__"` entrypoint, and it can't be run standalone in
this repo:

- It imports `from scb.core.logger import get_logger` - an internal bank
  package that isn't vendored here.
- It needs a live `SparkSession` with the Databricks `spark-xml` library on
  the classpath (`parse_xml_with_schema()` calls into it via JVM interop).
- Nothing in this repo constructs that session, loads real T24 data into a
  Spark DataFrame, or writes the result anywhere - the calling harness lives
  outside this repo entirely.

**What actually invoking it would look like**, conceptually, inside that
external harness:
```python
# 1. Build a SparkSession with the Databricks spark-xml package available.
# 2. Load this table's lookup rows into a DataFrame shaped like
#    lookup_metadata: field_index, m_index, resolved_name_en.
schema_df = ...  # e.g. spark.read from wherever lookup_metadata lives in Spark's world

# 3. Read the raw XMLRECORD rows for this table into a DataFrame.
raw_df = ...

# 4. Parse.
parsed_df = apply_xml_parsing(spark, raw_df, schema_df, metadata_cols=[...])

# 5. Before writing, widen the target Iceberg table's schema if this batch needs it.
parsed_df = reconcile_iceberg_schema(spark, parsed_df, "catalog.bronze.account_wide")

# 6. Write parsed_df to the Iceberg table (not shown in python_parsing.py itself).
```

This file's role in *this* repo is purely as the reference the other three
pipelines were built to match - `trino_parsing/` first (Trino SQL, matched
semantically), then `dbt/` extended to match it exactly (unpinned `m_index`,
stale-column handling), and `local_parsing/` as a direct, faithful copy
(same engine, same functions, ported logger only). It's not something to run
day-to-day here; if you need to actually execute it, that happens in the
bank's real Databricks environment, not this local Docker stack.
