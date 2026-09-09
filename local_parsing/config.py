"""All settings for ``run_parsing.py`` in one place.

``run_parsing.py`` takes two arguments - ``<dataset> <mode>`` - e.g.
``account daily`` or ``all history``.  ``<dataset>`` is a key of ``DATASETS``
(or ``all`` for every one); ``<mode>`` is ``daily`` or ``history``.  Everything
else is read from here.  Values are hard-coded; edit them here, not the script.

Two orthogonal axes:
  * ``Dataset`` - *what* table to parse (source/target/merge key/date tag).
  * ``Job``     - *how* to run it (reader, cluster vs local, window, spark conf).
Any dataset can run under any mode.

"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date

from dotenv import load_dotenv

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_HERE)

JARS_DIR = os.path.join(_HERE, "jars")
LOOKUP_CSV = os.path.join(_REPO_ROOT, "reference", "lookup_metadata.csv")

# Load repo-root .env into os.environ (a real env var already set always wins).
load_dotenv(os.path.join(_REPO_ROOT, ".env"))

def _secret(key: str, default: str = "") -> str:
    """Read a setting from the real environment, else the repo-root .env
    (loaded above). A real env var already set always wins over the file."""
    return os.environ.get(key) or default


# ─────────────────────────────── shared ─────────────────────────────── #
# CATALOG / NAMESPACE are the contract with the Trino/dbt side - keep in sync
CATALOG = "iceberg"
NAMESPACE = "bronze"

# Oracle connection - shared by every dataset. Defaults target the local Docker
ORACLE_HOST = _secret("ORACLE_HOST", "localhost")
ORACLE_PORT = _secret("ORACLE_PORT", "1521")
ORACLE_SERVICE = _secret("ORACLE_SERVICE", "XEPDB1")
ORACLE_SCHEMA = _secret("ORACLE_SCHEMA", "source_user")   # default table owner (per-dataset override below); "" if the connecting user owns it
ORACLE_USER = _secret("ORACLE_APP_USER", "source_user")
ORACLE_PASSWORD = _secret("ORACLE_APP_PASSWORD")

# Iceberg REST catalog + object store. Defaults = local Docker stack.
ICEBERG_REST_URI = _secret("ICEBERG_REST_URI", "http://localhost:8181")
ICEBERG_WAREHOUSE = _secret("ICEBERG_WAREHOUSE", "s3://warehouse/")
S3_ENDPOINT = _secret("S3_ENDPOINT") or _secret("MINIO_ENDPOINT", "http://localhost:9000")
S3_REGION = _secret("S3_REGION") or _secret("AWS_REGION", "us-east-1")
S3_ACCESS_KEY = _secret("MINIO_ROOT_USER")
S3_SECRET_KEY = _secret("MINIO_ROOT_PASSWORD")

# The driver's network address is set once, in .env, via the standard Spark env
# var SPARK_LOCAL_IP (loaded into the environment by load_dotenv above, read
# natively by Spark). Local dev: SPARK_LOCAL_IP=127.0.0.1. Cluster: leave it to
# the platform. Nothing about it is wired here.

# Jars must exist in JARS_DIR - run:  pwsh local_parsing/fetch_jars.ps1
_BASE_JARS = [
    "spark-xml_2.12-0.15.0.jar",
    "iceberg-spark-runtime-3.5_2.12-1.6.1.jar",
    "iceberg-spark-extensions-3.5_2.12-1.6.1.jar",
    "iceberg-aws-bundle-1.6.1.jar",
    "hadoop-aws-3.3.4.jar",
    "aws-java-sdk-bundle-1.12.262.jar",
]
_ORACLE_JDBC_JARS = ["ojdbc8.jar", "xmlparserv2-19.3.0.0.jar", "xdb-19.3.0.0.jar"]


# ═══════════════════════════════ DATASETS ══════════════════════════════ #
# One entry per Oracle source table.  Add a table = add a dict entry; the
# runner and every helper are dataset-driven, nothing else changes.
@dataclass(frozen=True)
class Dataset:
    name: str                       # CLI selector + logical name, e.g. "account"
    source_table: str               # Oracle table (bare identifier)
    target_table: str               # "bronze.account_wide" -> catalog prepended when 1 dot
    merge_key: str = "recid"        # PK column for the daily MERGE
    date_field: str = "c167"        # XML tag under /row driving the daily window filter
    oracle_schema: str = ORACLE_SCHEMA   # table owner; "" if the connecting user owns it
    normalize_arrays: bool = True   # collapse single-element ARRAY<STRING> -> scalar


DATASETS: dict[str, "Dataset"] = {
    "account": Dataset(
        name="account",
        source_table="account",
        target_table="bronze.account_wide",
        date_field="c167",
    ),
    # ---- add more tables here, e.g.: --------------------------------------
    # "customer": Dataset(
    #     name="customer",
    #     source_table="customer",
    #     target_table="bronze.customer_wide",
    #     date_field="c167",          # <- the customer table's own date tag
    # ),
}


@dataclass(frozen=True)
class Job:
    name: str
    write_mode: str          # "replace" | "append" | "merge"
    reader: str              # "thin" (python-oracledb) | "jdbc" (partitioned Spark)
    jdbc_num_partitions: int
    window: tuple | None     # ("YYYYMMDD","YYYYMMDD"), ("today","today"), or None = full read
    jars: list
    spark_conf: dict


# ═══════════════════════════════ DAILY ═══════════════════════════════ #
# Low-overhead local run for the small daily delta.
DAILY = Job(
    name="daily",
    write_mode="merge",
    reader="thin",
    jdbc_num_partitions=1,
    window=("today", "today"),   # single-day window on the dataset's date_field; ("20260101","20260101") to pin
    jars=_BASE_JARS,
    spark_conf={
        # ---- the point of this job: strip per-run startup overhead ----
        "spark.master": "local[2]",                         # no cluster negotiation and 2 threads in this machine 
        "spark.sql.catalogImplementation": "in-memory",     # skip Hive metastore / Derby init
        "spark.ui.enabled": "false",                        # no Jetty Spark-UI server
        "spark.ui.showConsoleProgress": "false",
        "spark.sql.shuffle.partitions": "1",                # tiny data: no 200-task shuffle plan
        "spark.default.parallelism": "2",
        "spark.sql.adaptive.enabled": "false",              # AQE round-trips are pure overhead here
        "spark.sql.adaptive.coalescePartitions.enabled": "false",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        "spark.sql.execution.arrow.pyspark.fallback.enabled": "true",
        # driver address + heap for local runs come from .env
        # (SPARK_LOCAL_IP, PYSPARK_SUBMIT_ARGS) - not wired here.
    },
)

# ══════════════════════════════ HISTORY ═════════════════════════════ #
# Full backfill - startup overhead is accepted. Cluster wiring (master, executor
# sizing, jdbc partitions) is read from .env inline below; every default is the
# local-stack value, so a bare checkout runs history on one machine. Point
# SPARK_MASTER at spark://... / k8s://... in .env for a cluster. The tuning
# knobs stay fixed (they track data size, not environment). Driver address +
# heap for local runs come from .env (SPARK_LOCAL_IP, PYSPARK_SUBMIT_ARGS),
# same as DAILY; on a cluster the platform sets them.
_history_conf = {
    "spark.master": _secret("SPARK_MASTER", "local[*]"),
    "spark.sql.catalogImplementation": "in-memory",             # skip Hive metastore / Derby init
    "spark.ui.enabled": "true",
    "spark.sql.shuffle.partitions": "200",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.execution.arrow.pyspark.enabled": "true",
    "spark.executor.instances": _secret("SPARK_EXECUTOR_INSTANCES", "4"),
    "spark.executor.memory": _secret("SPARK_EXECUTOR_MEMORY", "4g"),
    "spark.executor.cores": _secret("SPARK_EXECUTOR_CORES", "2"),
    "spark.executor.heartbeatInterval": "60s",
    "spark.network.timeout": "300s",
}

HISTORY = Job(
    name="history",
    write_mode="replace",
    reader="jdbc",
    jdbc_num_partitions=int(_secret("SPARK_JDBC_NUM_PARTITIONS", "16")),
    window=None,                 # full read
    jars=_BASE_JARS + _ORACLE_JDBC_JARS,
    spark_conf=_history_conf,
)

JOBS = {"daily": DAILY, "history": HISTORY}


def resolve_datasets(selector: str) -> list[Dataset]:
    """``"all"`` -> every dataset; otherwise the single named one.

    Raises ``KeyError`` for an unknown name so the runner can print usage.
    """
    if selector == "all":
        return list(DATASETS.values())
    return [DATASETS[selector]]


# ─────────────────────────────── helpers ────────────────────────────── #
def iceberg_conf() -> dict:
    c = f"spark.sql.catalog.{CATALOG}"
    return {
        "spark.sql.extensions":
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        c: "org.apache.iceberg.spark.SparkCatalog",
        f"{c}.type": "rest",
        f"{c}.uri": ICEBERG_REST_URI,
        f"{c}.warehouse": ICEBERG_WAREHOUSE,
        f"{c}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"{c}.s3.endpoint": S3_ENDPOINT,
        f"{c}.s3.path-style-access": "true",
        f"{c}.s3.access-key-id": S3_ACCESS_KEY,
        f"{c}.s3.secret-access-key": S3_SECRET_KEY,
        f"{c}.s3.region": S3_REGION,
        f"{c}.client.region": S3_REGION,
        "spark.sql.defaultCatalog": CATALOG,
        "spark.sql.session.timeZone": "UTC",
    }


def spark_conf_for(job: Job) -> dict:
    """Job's tuned conf + shared Iceberg wiring + resolved ``spark.jars``."""
    missing = [j for j in job.jars if not os.path.isfile(os.path.join(JARS_DIR, j))]
    if missing:
        raise FileNotFoundError(
            f"Missing jar(s) in {JARS_DIR}: {', '.join(missing)}. "
            "Run:  pwsh local_parsing/fetch_jars.ps1"
        )
    conf = dict(job.spark_conf)
    conf.update(iceberg_conf())
    conf["spark.jars"] = ",".join(
        os.path.join(JARS_DIR, j).replace("\\", "/") for j in job.jars
    )
    return conf


def resolve_window(job: Job):
    if job.window is None:
        return None
    start, end = job.window
    today = date.today().strftime("%Y%m%d")
    return (today if start == "today" else start, today if end == "today" else end)


def target_fqn(ds: Dataset) -> str:
    raw = ds.target_table
    dots = raw.count(".")
    if dots == 0:
        return f"{CATALOG}.{NAMESPACE}.{raw}"
    if dots == 1:
        return f"{CATALOG}.{raw}"
    return raw


def source_fqn(ds: Dataset) -> str:
    return f"{ds.oracle_schema}.{ds.source_table}" if ds.oracle_schema else ds.source_table


def oracle_dsn() -> str:
    return f"{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}"


def oracle_jdbc_url() -> str:
    return f"jdbc:oracle:thin:@//{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}"
