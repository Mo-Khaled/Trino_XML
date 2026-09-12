#!/usr/bin/env python
"""Local runner for the bank's ``python_parsing.py``.

    python local_parsing/run_parsing.py account daily     # one table, low-overhead local run
    python local_parsing/run_parsing.py account history   # one table, full backfill, local[*]
    python local_parsing/run_parsing.py all daily         # every table in DATASETS, in turn

    # reprocess a missed/old day (or a bounded slice of history) instead of
    # the job's own default window (daily: today; history: everything):
    python local_parsing/run_parsing.py account daily --start-date 20260909 --end-date 20260909
    python local_parsing/run_parsing.py account history --start-date 20260901 --end-date 20260910

Takes two positional arguments - ``<dataset> <mode>``.  ``<dataset>`` is a key
of ``config.DATASETS`` (or ``all``); ``<mode>`` is ``daily`` / ``history``.
``--start-date``/``--end-date`` (``YYYYMMDD``, given together) override that
job's default window for this run only; ``history`` run with an explicit
window writes with ``merge`` instead of ``replace`` so it doesn't wipe rows
outside the window (see the local-parsing-requirements skill, Requirement 3).
**Every** other setting lives in ``local_parsing/config.py`` (hard-coded;
``Dataset`` per table, ``Job`` per mode).  With ``all``, one dataset failing
is logged and the rest still run; the process exits non-zero if any failed.

One-time setup:  pip install -r local_parsing/requirements.txt   (+ a JDK 11/17)
                 bash local_parsing/fetch_jars.sh
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config
from python_parsing import (
    apply_xml_parsing,
    normalize_arrays,
    reconcile_iceberg_schema,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(name)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(os.path.dirname(os.path.abspath(__file__)), "run_parsing.log")),
    ],
)
logger = logging.getLogger("run_parsing")

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_DATE_RE = re.compile(r"^\d{8}$")


# --------------------------------------------------------------------------- #
# SparkSession - the only real change vs python_parsing.py                     #
# --------------------------------------------------------------------------- #
def build_spark_session(job: "config.Job"):
    from pyspark.sql import SparkSession

    conf = config.spark_conf_for(job)  # tuned conf + Iceberg wiring + spark.jars

    builder = SparkSession.builder.appName(f"xml-parsing-{job.name}")
    for key, value in conf.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN" if job.name == "daily" else "INFO")
    return spark


# --------------------------------------------------------------------------- #
# read / write                                                                #
# --------------------------------------------------------------------------- #
def load_lookup(spark, dataset):
    """lookup_metadata.csv -> DataFrame(field_index, m_index, resolved_name_en).

    Filtered to ``dataset.source_table``.  Blank ``m_index`` -> Python ``None``
    (NOT 1 - that is the Trino path's rule and the wrong semantics here).
    """
    import csv

    from pyspark.sql.types import LongType, StringType, StructField, StructType

    table_lc = dataset.source_table.strip().lower()
    rows = []
    with open(config.LOOKUP_CSV, newline="", encoding="utf-8-sig") as fh:
        for r in csv.DictReader(fh):
            if (r.get("table_name") or "").strip().lower() != table_lc:
                continue
            m_raw = (r.get("m_index") or "").strip()
            rows.append({
                "field_index": (r.get("field_index") or "").strip(),
                "m_index": int(m_raw) if m_raw else None,
                "resolved_name_en": (r.get("resolved_name_en") or "").strip(),
            })
    if not rows:
        raise ValueError(f"No lookup rows for {dataset.source_table!r} in {config.LOOKUP_CSV}")
    n_none = sum(1 for r in rows if r["m_index"] is None)
    logger.info("lookup_metadata[%s]: %d rows, %d blank m_index",
                dataset.source_table, len(rows), n_none)
    schema = StructType([
        StructField("field_index", StringType(), False),
        StructField("m_index", LongType(), True),
        StructField("resolved_name_en", StringType(), False),
    ])
    return spark.createDataFrame(rows, schema)


def _check_source_idents(dataset) -> str:
    """Validate every dataset-supplied token that lands in a SQL string."""
    for label, value in (("source_table", dataset.source_table),
                         ("date_field", dataset.date_field)):
        if not _IDENT_RE.match(value):
            raise ValueError(f"dataset.{label} {value!r} is not a bare identifier")
    if dataset.oracle_schema and not _IDENT_RE.match(dataset.oracle_schema):
        raise ValueError(f"dataset.oracle_schema {dataset.oracle_schema!r} is not a bare identifier")
    return config.source_fqn(dataset)


def read_jdbc(spark, dataset, job, window):
    """History read via partitioned Spark JDBC (needs the ojdbc8 + xmlparserv2 + xdb jars)."""
    fq = _check_source_idents(dataset)
    n = job.jdbc_num_partitions

    where = ""
    if window:
        for d in window:
            if not _DATE_RE.match(d):
                raise ValueError(f"window date {d!r} is not YYYYMMDD")
        where = (f" WHERE XMLCAST(XMLQUERY('/row/{dataset.date_field}/text()' PASSING a.xmlrecord "
                 f"RETURNING CONTENT) AS VARCHAR2(8)) BETWEEN '{window[0]}' AND '{window[1]}'")
    sub = (f"(SELECT a.recid, a.xmlrecord.getClobVal() AS xmlrecord, "
           f"MOD(ORA_HASH(a.recid), {n}) AS pkey FROM {fq} a{where}) t")
    logger.info("Oracle read (jdbc) dbtable: %s", sub)

    df = (spark.read.format("jdbc")
          .option("url", config.oracle_jdbc_url())
          .option("dbtable", sub)
          .option("user", config.ORACLE_USER)
          .option("password", config.ORACLE_PASSWORD)
          .option("driver", "oracle.jdbc.OracleDriver")
          .option("partitionColumn", "pkey")
          .option("lowerBound", "0")
          .option("upperBound", str(n))
          .option("numPartitions", str(n))
          .option("fetchsize", "5000")
          .load())
    for src, dst in {"RECID": "recid", "XMLRECORD": "XMLRECORD"}.items():
        if src in df.columns and src != dst:
            df = df.withColumnRenamed(src, dst)
    for pk in ("PKEY", "pkey"):
        if pk in df.columns:
            df = df.drop(pk)
    return df


def _table_exists(spark, fqn: str) -> bool:
    try:
        return spark.catalog.tableExists(fqn)
    except Exception:  # noqa: BLE001
        return False


def write_raw(spark, df, target: str, merge_key: str) -> None:
    #Land the untouched raw XML permanently, before any parsing.
    from pyspark.sql import functions as F

    raw_df = df.select(
        F.col("recid"),
        F.col("XMLRECORD").alias("xmlrecord"),
        F.current_timestamp().alias("ingested_at"),
    )
    if not _table_exists(spark, target):
        ns = ".".join(target.split(".")[:-1])
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ns}")
        logger.info("bootstrap create raw table %s", target)
        (raw_df.writeTo(target).using("iceberg")
           .tableProperty("format-version", "2")
           .tableProperty("write.merge.mode", "merge-on-read")
           .tableProperty("write.update.mode", "merge-on-read")
           .createOrReplace())
        return

    raw_df.createOrReplaceTempView("_raw_updates")
    spark.sql(
        f"MERGE INTO {target} t USING _raw_updates s "
        f"ON t.{merge_key} = s.{merge_key} "
        f"WHEN MATCHED AND t.xmlrecord IS DISTINCT FROM s.xmlrecord THEN UPDATE SET * "
        f"WHEN NOT MATCHED THEN INSERT *"
    )


def write_result(spark, df, target: str, write_mode: str, merge_key: str) -> None:
    if not _table_exists(spark, target):
        ns = ".".join(target.split(".")[:-1])
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ns}")
        logger.info("bootstrap create %s", target)
        (df.writeTo(target).using("iceberg")
           .tableProperty("format-version", "2")
           .tableProperty("write.merge.mode", "merge-on-read")
           .tableProperty("write.update.mode", "merge-on-read")
           .createOrReplace())
        return

    df = reconcile_iceberg_schema(spark, df, target)   # widen table / align df first
    if write_mode == "replace":
        df.writeTo(target).using("iceberg").createOrReplace()
    else:  # merge
        df.createOrReplaceTempView("_updates")
        spark.sql(
            f"MERGE INTO {target} t USING _updates s "
            f"ON t.{merge_key} = s.{merge_key} "
            f"WHEN MATCHED THEN UPDATE SET * "
            f"WHEN NOT MATCHED THEN INSERT *"
        )


def run_one(spark, dataset, job, window_override=None) -> None:
    """Parse + write a single dataset under a single job.  Raises on failure.

    ``window_override`` (start, end) comes from --start-date/--end-date on
    the CLI and takes precedence over the job's own default window - used to
    reprocess a missed/old day (daily) or a bounded slice of history.
    """
    from pyspark.sql import functions as F

    window = window_override if window_override is not None else config.resolve_window(job)
    target = config.target_fqn(dataset)
    raw_target = config.raw_fqn(dataset)
    write_mode = job.write_mode
    if window_override is not None and write_mode == "replace":
        write_mode = "merge"
        logger.info("explicit window on a %r job -> using merge instead of replace "
                    "(a windowed replace would delete every row outside the window)",
                    job.name)
        
    timings: dict[str, float] = {}
    def phase(name, fn):
        t = time.perf_counter()
        out = fn()
        timings[name] = time.perf_counter() - t
        return out

    logger.info("dataset=%s  job=%s  target=%s  raw_target=%s  window=%s  reader=%s  write-mode=%s",
                dataset.name, job.name, target, raw_target, window or "FULL", job.reader, write_mode)

    schema_df = phase("load_lookup", lambda: load_lookup(spark, dataset))
    raw_df = phase("read", lambda: read_jdbc(spark, dataset, job, window))
    # cache: read is lazy for the jdbc reader, so without this, count/write_raw/parse
    # below would each re-issue the Oracle query instead of reusing the one read.
    raw_df = raw_df.cache()
    n = phase("count_source", raw_df.count)
    logger.info("source rows: %d", n)
    if n == 0:
        logger.warning("nothing to write for %s in this window", dataset.name)
        raw_df.unpersist()
        return

    phase("write_raw", lambda: write_raw(spark, raw_df, raw_target, dataset.merge_key))

    parsed = phase("parse", lambda: apply_xml_parsing(
        spark, raw_df, schema_df, metadata_cols=[F.col("recid")]))
    if dataset.normalize_arrays:
        parsed = phase("normalize_arrays", lambda: normalize_arrays(parsed))

    phase("write", lambda: write_result(spark, parsed, target, write_mode, dataset.merge_key))
    final_n = phase("count_target", spark.table(target).count)
    raw_df.unpersist()

    logger.info("---- timings (s) [%s] ----", dataset.name)
    for name, secs in timings.items():
        logger.info("  %-16s %8.2f", name, secs)
    logger.info("  %-16s %8.2f", "TOTAL", sum(timings.values()))
    logger.info("source rows=%d  target(%s) rows=%d", n, target, final_n)


def _date_arg(s: str) -> str:
    if not _DATE_RE.match(s):
        raise argparse.ArgumentTypeError(f"{s!r} is not YYYYMMDD")
    return s


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="run_parsing.py",
        description="Run the local Spark XML-parsing pipeline for one dataset (or all).",
    )
    p.add_argument("dataset", help=f"one of: {', '.join(config.DATASETS)} (or 'all')")
    p.add_argument("mode", choices=sorted(config.JOBS), help="daily | history")
    p.add_argument("--start-date", type=_date_arg, metavar="YYYYMMDD",
                    help="override this run's window start (must be given with --end-date)")
    p.add_argument("--end-date", type=_date_arg, metavar="YYYYMMDD",
                    help="override this run's window end (must be given with --start-date)")
    return p


def main(argv=None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)

    if bool(args.start_date) != bool(args.end_date):
        parser.error("--start-date and --end-date must be given together")
    window_override = (args.start_date, args.end_date) if args.start_date else None

    try:
        datasets = config.resolve_datasets(args.dataset)
    except KeyError:
        parser.error(f"unknown dataset {args.dataset!r}; known: {', '.join(config.DATASETS)} (or 'all')")
    job = config.JOBS[args.mode]

    t = time.perf_counter()
    spark = build_spark_session(job)
    logger.info("session_build     %8.2f", time.perf_counter() - t)

    failed: list[str] = []
    try:
        for ds in datasets:
            try:
                run_one(spark, ds, job, window_override=window_override)
            except Exception:  # noqa: BLE001
                logger.exception("dataset %s FAILED - continuing with the rest", ds.name)
                failed.append(ds.name)
    finally:
        spark.stop()

    if failed:
        logger.error("FAILED datasets: %s", ", ".join(failed))
        return 1
    logger.info("all datasets OK: %s", ", ".join(d.name for d in datasets))
    return 0



if __name__ == "__main__":
    raise SystemExit(main())
