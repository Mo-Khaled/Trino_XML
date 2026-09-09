#!/usr/bin/env python
"""Local runner for the bank's ``python_parsing.py``.

    python local_parsing/run_parsing.py account daily     # one table, low-overhead local run
    python local_parsing/run_parsing.py account history   # one table, full backfill on a cluster
    python local_parsing/run_parsing.py all daily         # every table in DATASETS, in turn

Takes two arguments - ``<dataset> <mode>``.  ``<dataset>`` is a key of
``config.DATASETS`` (or ``all``); ``<mode>`` is ``daily`` / ``history``.
**Every** setting lives in ``local_parsing/config.py`` (hard-coded; ``Dataset``
per table, ``Job`` per mode).  With ``all``, one dataset failing is logged and
the rest still run; the process exits non-zero if any failed.

One-time setup:  pip install pyspark==3.5.5 python-oracledb   (+ a JDK 11/17)
                 pwsh local_parsing/fetch_jars.ps1
"""

from __future__ import annotations

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
    level=logging.INFO, format="%(asctime)s %(levelname)-5s %(name)s | %(message)s"
)
logger = logging.getLogger("run_parsing")

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


# --------------------------------------------------------------------------- #
# SparkSession - the only real change vs python_parsing.py                     #
# --------------------------------------------------------------------------- #
def build_spark_session(job: "config.Job"):
    from pyspark.sql import SparkSession

    conf = config.spark_conf_for(job)  # tuned conf + Iceberg wiring + spark.jars

    # SPARK_LOCAL_IP and PYSPARK_SUBMIT_ARGS come from .env (loaded by
    # `import config`); Spark reads both from the environment at JVM launch.
    # They are local-run conveniences - drop them for a real cluster so the
    # platform / spark-operator assigns the driver address and sizing itself.
    if not str(conf.get("spark.master", "")).startswith("local"):
        os.environ.pop("SPARK_LOCAL_IP", None)
        os.environ.pop("PYSPARK_SUBMIT_ARGS", None)

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


def read_thin(spark, dataset, window):
    """Daily read via python-oracledb (thin) -> driver -> createDataFrame.

    Selects only ``recid`` and ``xmlrecord`` - never any other physical column.    """
    import oracledb

    from pyspark.sql.types import StringType, StructField, StructType

    fq = _check_source_idents(dataset)
    oracledb.defaults.fetch_lobs = False  # CLOB -> str

    sql = f"SELECT a.recid, a.xmlrecord.getClobVal() AS xmlrecord FROM {fq} a"
    binds: dict[str, str] = {}
    if window:
        sql += (f" WHERE XMLCAST(XMLQUERY('/row/{dataset.date_field}/text()' PASSING a.xmlrecord "
                "RETURNING CONTENT) AS VARCHAR2(8)) BETWEEN :sd AND :ed")
        binds = {"sd": window[0], "ed": window[1]}

    logger.info("Oracle read (thin): %s", sql)
    with oracledb.connect(user=config.ORACLE_USER, password=config.ORACLE_PASSWORD,
                          dsn=config.oracle_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, binds)
            fetched = cur.fetchall()  # (recid, xmlrecord)
    logger.info("Oracle returned %d rows", len(fetched))

    raw_schema = StructType([
        StructField("XMLRECORD", StringType(), True),   # name hard-coded by apply_xml_parsing
        StructField("recid", StringType(), False),
    ])
    return spark.createDataFrame(
        [{"XMLRECORD": xml, "recid": str(recid)} for (recid, xml) in fetched], raw_schema
    )


def read_jdbc(spark, dataset, job, window):
    """History read via partitioned Spark JDBC (needs the ojdbc8 + xmlparserv2 + xdb jars)."""
    fq = _check_source_idents(dataset)
    n = job.jdbc_num_partitions

    where = ""
    if window:
        for d in window:
            if not re.match(r"^\d{8}$", d):
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


def write_result(spark, df, target: str, write_mode: str, merge_key: str) -> None:
    if not _table_exists(spark, target):
        ns = ".".join(target.split(".")[:-1])
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ns}")
        logger.info("bootstrap create %s", target)
        (df.writeTo(target).using("iceberg")
           .tableProperty("format-version", "2").createOrReplace())
        return

    df = reconcile_iceberg_schema(spark, df, target)   # widen table / align df first
    if write_mode == "replace":
        df.writeTo(target).using("iceberg").createOrReplace()
    elif write_mode == "append":
        df.writeTo(target).append()
    else:  # merge
        df.createOrReplaceTempView("_updates")
        spark.sql(
            f"MERGE INTO {target} t USING _updates s "
            f"ON t.{merge_key} = s.{merge_key} "
            f"WHEN MATCHED THEN UPDATE SET * "
            f"WHEN NOT MATCHED THEN INSERT *"
        )


def run_one(spark, dataset, job) -> None:
    """Parse + write a single dataset under a single job.  Raises on failure."""
    from pyspark.sql import functions as F

    window = config.resolve_window(job)
    target = config.target_fqn(dataset)
    timings: dict[str, float] = {}

    def phase(name, fn):
        t = time.perf_counter()
        out = fn()
        timings[name] = time.perf_counter() - t
        return out

    logger.info("dataset=%s  job=%s  target=%s  window=%s  reader=%s  write-mode=%s",
                dataset.name, job.name, target, window or "FULL", job.reader, job.write_mode)

    schema_df = phase("load_lookup", lambda: load_lookup(spark, dataset))
    raw_df = phase("read", lambda: (
        read_thin(spark, dataset, window) if job.reader == "thin"
        else read_jdbc(spark, dataset, job, window)
    ))
    n = phase("count_source", raw_df.count)
    logger.info("source rows: %d", n)
    if n == 0:
        logger.warning("nothing to write for %s in this window", dataset.name)
        return

    parsed = phase("parse", lambda: apply_xml_parsing(
        spark, raw_df, schema_df, metadata_cols=[F.col("recid")]))
    if dataset.normalize_arrays:
        parsed = phase("normalize_arrays", lambda: normalize_arrays(parsed))

    phase("write", lambda: write_result(spark, parsed, target, job.write_mode, dataset.merge_key))
    final_n = phase("count_target", spark.table(target).count)

    logger.info("---- timings (s) [%s] ----", dataset.name)
    for name, secs in timings.items():
        logger.info("  %-16s %8.2f", name, secs)
    logger.info("  %-16s %8.2f", "TOTAL", sum(timings.values()))
    logger.info("source rows=%d  target(%s) rows=%d", n, target, final_n)


def main(argv=None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if len(args) != 2 or args[1] not in config.JOBS:
        sys.stderr.write(
            "usage: run_parsing.py {<dataset>|all} {daily|history}\n"
            f"  datasets: {', '.join(config.DATASETS)}\n"
        )
        return 2
    try:
        datasets = config.resolve_datasets(args[0])
    except KeyError:
        sys.stderr.write(
            f"unknown dataset {args[0]!r}; known: {', '.join(config.DATASETS)} (or 'all')\n"
        )
        return 2
    job = config.JOBS[args[1]]

    t = time.perf_counter()
    spark = build_spark_session(job)
    logger.info("session_build     %8.2f", time.perf_counter() - t)

    failed: list[str] = []
    try:
        for ds in datasets:
            try:
                run_one(spark, ds, job)
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
