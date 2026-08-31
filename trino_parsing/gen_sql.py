#!/usr/bin/env python3
"""
Generate Trino SQL that parses T24 XMLRECORD blobs into an Iceberg bronze
attributes table -- one row per (recid, field_index, m_index, s_index).

This is the Trino port of the bank's PySpark logic in parsing/python_parsing.py
(which produces named business columns; see that file's docstring for the m/s
semantics this preserves). It emits SQL text only -- it never connects to
anything. Run a mode, paste the output into DBeaver's Trino editor.

Every mode here is genuinely table-agnostic: the EAV output has a fixed
7-column shape no matter what the lookup table contains, so --table only ever
changes a WHERE filter or a table-name substitution, never the SQL's shape or
column count. Business-field names (the lookup table) are never read by
Python at all -- only --mode reconcile joins against --lookup-table, and only
inside the generated SQL itself, at query time. There is no separate
"discover which fields need special handling" step: m_index and s_index are
stored as plain columns for every value unconditionally, so nothing about
generation depends on what the data looks like -- anyone curious which fields
repeat or carry sub-values can just query the resulting table directly
(SELECT field_index, max(m_index), bool_or(s_index > 1) ... GROUP BY
field_index) instead of running a dedicated mode for it.

Modes
  ingest       Oracle XMLTYPE -> iceberg.bronze.<table>_raw (getClobVal passthrough)
  ingest-refresh  Windowed re-read of Oracle, MERGE into <table>_raw
  bootstrap    First load: CREATE TABLE iceberg.bronze.<table>_attributes AS <tokenized>
  incremental  Re-tokenize, delete+insert only recids whose xml_hash changed
  reconcile    Lookup-coverage report: tags in the data with no lookup row
"""

import argparse
import sys
from pathlib import Path

# One regex, three capture groups:
#   1 = tag name (c20), 2 = raw attributes (' m="18"'), 3 = text value.
# Group 2 is not optional, so it always participates and never yields a NULL slot.
# [^<]* for the value means newlines are fine without a DOTALL flag.
TAG_RE = r'<(c\d+)([^>]*)>([^<]*)</c\d+>'
M_RE = r'm="(\d+)"'
S_RE = r's="(\d+)"'

# Oracle's XMLTYPE serializer canonicalizes empty elements to self-closing form
# on getClobVal() -- <c100></c100> as written in SQL comes back as <c100/>,
# and TAG_RE's ...>value</cNN> shape cannot match that (confirmed against a
# live Oracle round-trip: the fixture's two empty fields, c20/m=32 and c100,
# both serialize this way). Rather than complicate TAG_RE with an alternation
# that would need optional capture groups (risking misaligned zip() arrays),
# normalize self-closing tags back to explicit-close form before tokenizing.
SELF_CLOSE_RE = r'<(c\d+)([^>]*)/>'
SELF_CLOSE_REPLACEMENT = r'<$1$2></$1>'

# Shape of one XML element after tokenizing.
TOKEN_ROW = "ROW(tag VARCHAR, m INTEGER, s INTEGER, val VARCHAR)"

def unescape_expr(inner: str) -> str:
    """XML entity decoding. &amp; must be decoded LAST or '&amp;lt;' breaks."""
    e = inner
    for ent, ch in (("&lt;", "<"), ("&gt;", ">"), ("&quot;", '"'),
                    ("&apos;", "'"), ("&#39;", "'"), ("&amp;", "&")):
        lit = ch.replace("'", "''")  # a literal ' inside a Trino string literal is doubled
        e = f"replace({e}, '{ent}', '{lit}')"
    return e


def blank_to_null(check_expr: str, value_expr: str = None) -> str:
    """Equivalent to NULLIF(value_expr, ''), written without NULLIF.

    Confirmed against a live Trino 483 instance: NULLIF(...) used as a field
    inside a ROW(...) constructor inside a transform(...) lambda triggers a
    real planner bug ("class io.trino.sql.ir.Bind cannot be cast to class
    io.trino.sql.ir.Lambda") -- reproduced with a minimal literal-array query,
    so it's not specific to this pipeline's data or CTE shape. CASE WHEN is
    semantically identical and does not trigger it.

    check_expr and value_expr are split so a cheap check (e.g. the raw,
    un-decoded value) can guard an expensive value expression (e.g. the
    fully entity-unescaped text) without evaluating the latter twice.
    """
    value_expr = check_expr if value_expr is None else value_expr
    return f"CASE WHEN {check_expr} = '' THEN NULL ELSE {value_expr} END"


# ---------------------------------------------------------------------------
# Shared CTE block
# ---------------------------------------------------------------------------

def token_ctes(source: str) -> str:
    """raw -> tokens -> exploded: one row per (recid, tag, m, s, val).

    Shared by every mode that needs tokenized XML. Table-agnostic -- the only
    input is which raw table to read.
    """
    val = unescape_expr("e[3]")
    return f"""-- Part 1/4 -- read XML, hash it, normalize self-closing empty tags.
WITH raw AS (
  SELECT
    recid,
    xmlrecord,
    to_hex(md5(to_utf8(xmlrecord))) AS xml_hash,
    -- <c100/> (Oracle's serialized form of an empty field) -> <c100></c100>.
    regexp_replace(xmlrecord, '{SELF_CLOSE_RE}', '{SELF_CLOSE_REPLACEMENT}') AS xmlrecord_norm
  FROM {source}
),

-- Part 2/4 -- extract every tag into one (tag, m, s, value) struct per
-- occurrence, one array per record. 3 scans total, not one per field.
tokens AS (
  SELECT
    recid,
    xml_hash,
    transform(
      zip(
        regexp_extract_all(xmlrecord_norm, '{TAG_RE}', 1),
        regexp_extract_all(xmlrecord_norm, '{TAG_RE}', 2),
        regexp_extract_all(xmlrecord_norm, '{TAG_RE}', 3)
      ),
      e -> CAST(
        ROW(
          e[1],
          CAST(COALESCE(regexp_extract(e[2], '{M_RE}', 1), '1') AS INTEGER),
          CAST(COALESCE(regexp_extract(e[2], '{S_RE}', 1), '1') AS INTEGER),
          {blank_to_null("e[3]", val)}
        ) AS {TOKEN_ROW}
      )
    ) AS entries
  FROM raw
),

-- Part 3/4 -- one row per tag occurrence. Sentinel keeps a no-tag record's
-- recid alive (UNNEST on an empty array drops the row); filtered out below.
exploded AS (
  SELECT t.recid, t.xml_hash, u.tag, u.m, u.s, u.val
  FROM tokens t
  CROSS JOIN UNNEST(
    IF(cardinality(t.entries) = 0,
       ARRAY[CAST(ROW('__empty__', 1, 1, NULL) AS {TOKEN_ROW})],
       t.entries)
  ) AS u(tag, m, s, val)
)"""




# ---------------------------------------------------------------------------
# Modes
# ---------------------------------------------------------------------------

def mode_ingest(a) -> str:
    """Land raw XML text. Oracle does zero field-level work -- getClobVal() is a
    serialization call, not an XPath query."""
    raw = f"{a.raw_target}"
    return f"""-- Generated by gen_sql.py --mode ingest --table {a.table}
-- XMLTYPE is unsupported by the Trino Oracle connector; getClobVal()
-- serializes it via native passthrough. Only usage in the pipeline --
-- everything downstream reads Iceberg.

CREATE SCHEMA IF NOT EXISTS {a.schema}
WITH (location = 's3://warehouse/bronze');

DROP TABLE IF EXISTS {raw};

CREATE TABLE {raw}
WITH (format = 'PARQUET') AS
SELECT
  recid,
  xmlrecord,
  current_timestamp AS ingested_at
FROM TABLE(
  oracle.system.query(
    query => 'SELECT
      a.recid,
      a.xmlrecord.getClobVal() AS xmlrecord
    FROM {a.oracle_table} a'
  )
);

-- Truncation check: Iceberg length vs Oracle dbms_lob.getlength().
-- Zero rows = no truncation; any row returned needs investigation.
WITH iceberg_lengths AS (
  SELECT recid, length(xmlrecord) AS iceberg_length
  FROM {raw}
),
oracle_lengths AS (
  SELECT recid, oracle_length
  FROM TABLE(
    oracle.system.query(
      query => 'SELECT a.recid, CAST(dbms_lob.getlength(a.xmlrecord.getclobval()) AS NUMBER(10)) AS oracle_length
      FROM {a.oracle_table} a'
    )
  )
)
SELECT i.recid, i.iceberg_length, o.oracle_length
FROM iceberg_lengths i
JOIN oracle_lengths o ON i.recid = o.recid
WHERE i.iceberg_length <> o.oracle_length;

-- Token completeness: matched_tags vs raw_tag_opens must be equal after
-- normalizing self-closing tags. A mismatch means the regex is missing
-- an element shape.
SELECT
  count(*) AS records,
  sum(cardinality(regexp_extract_all(
    regexp_replace(xmlrecord, '{SELF_CLOSE_RE}', '{SELF_CLOSE_REPLACEMENT}'),
    '{TAG_RE}', 1
  ))) AS matched_tags,
  sum(cardinality(regexp_extract_all(xmlrecord, '<c\\d+[ />]'))) AS raw_tag_opens
FROM {raw};
"""


def mode_ingest_refresh(a) -> str:
    raw = a.raw_target
    if a.start_date:
        window_comment = (
            f"-- Window: {a.watermark_field} BETWEEN {a.start_date} AND {a.end_date}. "
            f"Rows outside it are never serialized by getClobVal() or transferred."
        )
        where_clause = f"""
    WHERE XMLCAST(
      XMLQUERY(''/row/{a.watermark_field}/text()'' PASSING a.xmlrecord RETURNING CONTENT)
      AS VARCHAR2(8)
    ) BETWEEN ''{a.start_date}'' AND ''{a.end_date}''"""
    else:
        window_comment = "-- No --start-date/--end-date given: reads all of Oracle."
        where_clause = ""
    return f"""-- Generated by gen_sql.py --mode ingest-refresh --table {a.table}
{window_comment}
-- Oracle stays read-only; the target is what becomes incremental. Deletes are
-- intentionally not inferred -- that needs a source tombstone/CDC feed.

DROP TABLE IF EXISTS {raw}_stage;

CREATE TABLE {raw}_stage
WITH (format = 'PARQUET') AS
SELECT
  recid,
  xmlrecord,
  current_timestamp AS ingested_at
FROM TABLE(
  oracle.system.query(
    query => 'SELECT
      a.recid,
      a.xmlrecord.getClobVal() AS xmlrecord
    FROM {a.oracle_table} a{where_clause}'
  )
);

MERGE INTO {raw} AS t
USING {raw}_stage AS s
ON t.recid = s.recid
WHEN MATCHED AND t.xmlrecord IS DISTINCT FROM s.xmlrecord THEN UPDATE SET
  xmlrecord = s.xmlrecord,
  ingested_at = s.ingested_at
WHEN NOT MATCHED THEN INSERT (recid, xmlrecord, ingested_at)
VALUES (s.recid, s.xmlrecord, s.ingested_at);

DROP TABLE IF EXISTS {raw}_stage;
"""


def mode_bootstrap(a) -> str:
    """First load: one EAV row per (recid, field_index, m_index, s_index).

    Generic by construction -- {a.source}/{a.target} are the only things that
    differ between tables. New lookup fields need no ALTER TABLE: they land
    automatically as new field_index values, since every tag is tokenized
    regardless of whether the lookup has a row for it yet.
    """
    return f"""-- Generated by gen_sql.py --mode bootstrap --table {a.table}
-- One row per (recid, field_index, m_index, s_index) -- no per-table codegen.

CREATE SCHEMA IF NOT EXISTS {a.schema}
WITH (location = 's3://warehouse/bronze');

DROP TABLE IF EXISTS {a.target};

CREATE TABLE {a.target}
WITH (format = 'PARQUET', partitioning = ARRAY['field_index']) AS
{token_ctes(a.source)}
-- Part 4/4 -- name and write out, dropping the Part 3 sentinel row.
SELECT
  recid,
  tag AS field_index,
  m AS m_index,
  s AS s_index,
  val AS field_value,
  xml_hash,
  current_timestamp AS ingested_at
FROM exploded
WHERE tag != '__empty__';

SELECT count(*) AS row_count FROM {a.target};
"""


def mode_incremental(a) -> str:
    """Re-tokenize (optionally windowed) {a.source}, replace only the
    attribute rows of recids whose xml_hash actually changed.

    Delete+insert per changed recid, not a per-attribute MERGE key: a changed
    record can gain or lose tags entirely (not just change a value), and
    delete+insert is the only way to also drop attribute rows for a tag that
    disappeared. Change detection itself needs no watermark/overlap-day math
    -- xml_hash is an exact content comparison, so it needs no date field or
    clock-skew tolerance.

    --start-date/--end-date (same flags as --mode ingest-refresh) instead
    restrict what gets RE-TOKENIZED in the first place, using ingested_at --
    a column ingest-refresh only bumps when a record's content actually
    changed. This is an optimization, not the correctness check: it relies
    on the same window (or wider) having been used for the ingest-refresh
    that fed this run, or a real change outside it would be silently missed.
    """
    if a.start_date:
        window_comment = (
            f"-- Window: only re-tokenizes {a.source} rows whose ingested_at falls in "
            f"[{a.start_date}, {a.end_date}]. ingest-refresh only bumps ingested_at when "
            f"content actually changed, so this must use the same window (or wider) as "
            f"the ingest-refresh that fed this run, or a real change outside it would be "
            f"silently skipped here."
        )
        windowed_source = (
            f"(SELECT * FROM {a.source} "
            f"WHERE ingested_at >= date_parse('{a.start_date}', '%Y%m%d') "
            f"AND ingested_at < date_parse('{a.end_date}', '%Y%m%d') + INTERVAL '1' DAY) "
            f"AS windowed_raw"
        )
    else:
        window_comment = f"-- No --start-date/--end-date given: re-tokenizes all of {a.source}."
        windowed_source = a.source

    return f"""-- Generated by gen_sql.py --mode incremental --table {a.table}
{window_comment}
-- Prerequisite: run --mode bootstrap once, and --mode ingest-refresh before this.

DROP TABLE IF EXISTS {a.target}_stage;

CREATE TABLE {a.target}_stage
WITH (format = 'PARQUET') AS
{token_ctes(windowed_source)}
SELECT
  recid,
  tag AS field_index,
  m AS m_index,
  s AS s_index,
  val AS field_value,
  xml_hash,
  current_timestamp AS ingested_at
FROM exploded
WHERE tag != '__empty__';

DROP TABLE IF EXISTS {a.target}_changed;

CREATE TABLE {a.target}_changed
WITH (format = 'PARQUET') AS
SELECT DISTINCT s.recid
FROM {a.target}_stage AS s
LEFT JOIN (SELECT DISTINCT recid, xml_hash FROM {a.target}) AS t
  ON t.recid = s.recid
WHERE t.recid IS NULL OR t.xml_hash IS DISTINCT FROM s.xml_hash;

DELETE FROM {a.target}
WHERE recid IN (SELECT recid FROM {a.target}_changed);

INSERT INTO {a.target}
SELECT s.*
FROM {a.target}_stage AS s
JOIN {a.target}_changed AS c ON c.recid = s.recid;

SELECT count(*) AS changed_records FROM {a.target}_changed;

DROP TABLE IF EXISTS {a.target}_stage;
DROP TABLE IF EXISTS {a.target}_changed;
"""


def mode_reconcile(a) -> str:
    """Lookup-coverage report, not schema DDL.

    The EAV table's schema never drifts -- it has the same 7 columns no
    matter what the lookup contains, so there's nothing to ALTER. What can
    still drift is coverage: a tag appears in real data with no lookup row
    naming it yet. Generic: joins the already-materialized {a.target}
    against --lookup-table, same shape for any table.
    """
    return f"""-- Generated by gen_sql.py --mode reconcile --table {a.table}
-- With EAV output there is no schema to ALTER -- new fields already land as
-- new field_index values (see --mode bootstrap). This instead reports the
-- other direction: tags present in real data with no lookup row naming them.

SELECT DISTINCT a.field_index
FROM {a.target} AS a
LEFT JOIN {a.lookup_table} AS l
  ON l.table_name = '{a.table}' AND l.field_index = a.field_index
WHERE l.field_index IS NULL
ORDER BY a.field_index;
"""


# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mode", default="bootstrap",
                    choices=["ingest", "ingest-refresh",
                             "bootstrap", "incremental", "reconcile"])
    ap.add_argument("--table", default="account",
                    help="table_name filter -- the only thing that changes "
                         "between tables for every mode")
    ap.add_argument("--oracle-schema", default="source_user",
                    help="Oracle schema that owns the source tables -- the "
                         "connector user (ORACLE_APP_USER) need not be this "
                         "schema's owner, so table references are always "
                         "qualified with it")
    ap.add_argument("--oracle-table", default=None,
                    help="fully-qualified Oracle table for --mode ingest "
                         "(default: <oracle-schema>.<table>)")
    ap.add_argument("--schema", default="iceberg.bronze")
    ap.add_argument("--source", default=None,
                    help="raw XML table (default: <schema>.<table>_raw)")
    ap.add_argument("--target", default=None,
                    help="parsed attributes table (default: <schema>.<table>_attributes)")
    ap.add_argument("--lookup-table", default="iceberg.bronze.lookup_metadata",
                    help="live, Trino-queryable lookup table -- joined against "
                         "directly by --mode reconcile. Not read by Python: "
                         "every mode emits the exact same SQL shape for any "
                         "table, since the output is one EAV row per (recid, "
                         "field_index, m_index, s_index), never a named "
                         "column per field.")
    ap.add_argument("--watermark-field", default="c167",
                    help="XML tag holding the YYYYMMDD source-change date, "
                         "used only by --mode ingest-refresh's date window")
    ap.add_argument("--start-date", default=None,
                    help="YYYYMMDD -- for --mode ingest-refresh, filter the Oracle-side "
                         "read to --watermark-field BETWEEN --start-date AND --end-date, "
                         "so rows outside the window are never serialized/transferred. "
                         "Omit both to read all of Oracle (e.g. for a full resync).")
    ap.add_argument("--end-date", default=None, help="YYYYMMDD, paired with --start-date")
    ap.add_argument("--out", default="-")
    a = ap.parse_args()

    if bool(a.start_date) != bool(a.end_date):
        sys.exit("--start-date and --end-date must be given together")

    a.oracle_table = a.oracle_table or f"{a.oracle_schema}.{a.table}"
    a.raw_target = f"{a.schema}.{a.table}_raw"
    a.source = a.source or a.raw_target
    a.target = a.target or f"{a.schema}.{a.table}_attributes"

    sql = {
        "ingest": mode_ingest,
        "ingest-refresh": mode_ingest_refresh,
        "bootstrap": mode_bootstrap,
        "incremental": mode_incremental,
        "reconcile": mode_reconcile,
    }[a.mode](a)

    if a.out == "-":
        print(sql)
    else:
        out = Path(a.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(sql, encoding="utf-8")
        print(f"wrote {out}", file=sys.stderr)


if __name__ == "__main__":
    main()
