#!/usr/bin/env python3
"""
Generate Trino SQL that parses T24 XMLRECORD blobs into wide Iceberg bronze tables.

This is the Trino port of the bank's PySpark logic in parsing/python_parsing.py.
It emits SQL text only -- it never connects to anything. Run a mode, paste the
output into DBeaver's Trino editor.

Why a generator and not one static query: SQL fixes its projection list at plan
time, so a query cannot produce dynamically-named columns from lookup metadata.
The tag -> business-name mapping has to be baked in at generation time.

Modes
  ingest       Oracle XMLTYPE -> iceberg.bronze.<table>_raw (getClobVal passthrough)
  discover     Report which tags carry s sub-values; feeds --fields-with-s
  bootstrap    First load: CREATE TABLE iceberg.bronze.<table> AS <parsed>
  incremental  Watermarked stage + MERGE into the same table
  reconcile    ALTER TABLE statements for lookup drift
"""

import argparse
import csv
import re
import sys
from collections import defaultdict
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

# Shape of one XML element after tokenizing, and of one element inside the
# per-tag map built by multimap_agg.
TOKEN_ROW = "ROW(tag VARCHAR, m INTEGER, s INTEGER, val VARCHAR)"
ENTRY_ROW = "ROW(m INTEGER, s INTEGER, val VARCHAR)"
EMPTY_ENTRIES = f"CAST(ARRAY[] AS ARRAY({ENTRY_ROW}))"

RESERVED = {
    "order", "group", "select", "from", "where", "table", "values", "current_date",
    "current_time", "current_timestamp", "user", "end", "case", "when", "then",
    "else", "and", "or", "not", "null", "true", "false", "exists", "between",
}

# Columns the pipeline adds alongside the parsed business fields.
META_COLS = [
    ("source_updated_date", "DATE"),
    ("xml_hash", "VARCHAR"),
    ("ingested_at", "TIMESTAMP(3) WITH TIME ZONE"),
]


def q(name: str) -> str:
    """Quote an identifier if it needs it."""
    if name.lower() in RESERVED or not re.fullmatch(r"[a-z_][a-z0-9_]*", name):
        return '"' + name.replace('"', '""') + '"'
    return name


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


def load_lookup(path, table):
    """Read (tag, m_index, output_name) for one table. Blank m_index -> None."""
    rows = []
    with open(path, encoding="utf-8-sig") as fh:
        for r in csv.DictReader(fh):
            if r["table_name"].strip().lower() != table.lower():
                continue
            m = (r["m_index"] or "").strip()
            rows.append((
                r["field_index"].strip(),
                int(m) if m else None,
                r["resolved_name_en"].strip(),
            ))
    return rows


def sort_key(row):
    tag, m, _ = row
    return (int(tag[1:]), m if m is not None else 0)


# ---------------------------------------------------------------------------
# Column expression templates -- the three branches of _build_select_expressions
# ---------------------------------------------------------------------------

def group_expr(tag: str) -> str:
    """The per-tag entry array, empty rather than NULL when the tag is absent."""
    return f"COALESCE(element_at(f, '{tag}'), {EMPTY_ENTRIES})"


def branch_1(g: str, m: int) -> str:
    """m pinned in the lookup -> ARRAY(VARCHAR) indexed by s."""
    sel = f"filter({g}, x -> x.m = {m})"
    return (
        f"CASE WHEN cardinality({sel}) = 0 THEN NULL ELSE transform("
        f"sequence(1, array_max(transform({sel}, x -> x.s))), "
        f"i -> element_at(transform(filter({g}, x -> x.m = {m} AND x.s = i), x -> x.val), 1)"
        f") END"
    )


def branch_3(g: str) -> str:
    """No m pinned, no s sub-values -> flat ARRAY(VARCHAR) indexed by m."""
    return (
        f"CASE WHEN cardinality({g}) = 0 THEN NULL ELSE transform("
        f"sequence(1, array_max(transform({g}, x -> x.m))), "
        f"i -> element_at(transform(filter({g}, x -> x.m = i), x -> x.val), 1)"
        f") END"
    )


def branch_2(g: str) -> str:
    """No m pinned, tag carries s sub-values -> ARRAY(ARRAY(VARCHAR)), outer m / inner s.

    The inner IF guard is load-bearing: T24 emits sparse m groups, and
    sequence(1, 0) throws in Trino when an intermediate group is missing.
    """
    return (
        f"CASE WHEN cardinality({g}) = 0 THEN NULL ELSE transform("
        f"sequence(1, array_max(transform({g}, x -> x.m))), "
        f"i -> IF(cardinality(filter({g}, x -> x.m = i)) = 0, "
        f"CAST(ARRAY[] AS ARRAY(VARCHAR)), "
        f"transform("
        f"sequence(1, array_max(transform(filter({g}, x -> x.m = i), x -> x.s))), "
        f"j -> element_at(transform(filter({g}, x -> x.m = i AND x.s = j), x -> x.val), 1)"
        f"))"
        f") END"
    )


def build_columns(rows, fields_with_s):
    """Return [(output_name, sql_expression, sql_type)] for every lookup row."""
    out = []
    seen = defaultdict(list)

    for tag, m, name in sorted(rows, key=sort_key):
        seen[name].append(tag)
        # c0 is the <row id="..."> attribute, i.e. the RECID -- there is no <c0> element.
        if tag == "c0":
            out.append((name, "recid", "VARCHAR"))
            continue
        g = group_expr(tag)
        if m is not None:
            out.append((name, branch_1(g, m), "ARRAY(VARCHAR)"))
        elif tag in fields_with_s:
            out.append((name, branch_2(g), "ARRAY(ARRAY(VARCHAR))"))
        else:
            out.append((name, branch_3(g), "ARRAY(VARCHAR)"))

    dupes = {k: v for k, v in seen.items() if len(v) > 1}
    if dupes:
        print(f"WARNING duplicate output names: {dupes}", file=sys.stderr)
    return out


# ---------------------------------------------------------------------------
# Shared CTE block
# ---------------------------------------------------------------------------

def parse_ctes(source: str, watermark_field: str) -> str:
    """raw -> tokens -> exploded -> grouped, producing one MAP per recid.

    Grouping by tag once with multimap_agg matters: without it every one of the
    ~290 output columns would filter() the full token array, which is
    O(columns x tokens) per row. After grouping, each column is an element_at
    against a short per-tag array.
    """
    val = unescape_expr("e[3]")
    return f"""WITH raw AS (
  SELECT
    recid,
    xmlrecord,
    to_hex(md5(to_utf8(xmlrecord))) AS xml_hash,
    -- Self-closing empty elements (<c100/>, Oracle's serialized form of an
    -- empty field) normalized to explicit-close form so the tokenizer below
    -- doesn't need a second, optional-group code path for them.
    regexp_replace(xmlrecord, '{SELF_CLOSE_RE}', '{SELF_CLOSE_REPLACEMENT}') AS xmlrecord_norm
  FROM {source}
),

-- Three regexp_extract_all calls over one pattern give parallel arrays of
-- (tag, attributes, value); zip() stitches them back into tuples. The document
-- is scanned 3 times total, not once per output column.
-- A missing m/s attribute means 1 (T24 omits it for the first occurrence), so
-- defaulting here saves a coalesce at every downstream access.
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

-- UNNEST of an empty array drops the row entirely, which would silently lose
-- records whose XML matched no tags. The sentinel keeps the recid alive under a
-- key no generated column ever looks up.
exploded AS (
  SELECT t.recid, t.xml_hash, u.tag, u.m, u.s, u.val
  FROM tokens t
  CROSS JOIN UNNEST(
    IF(cardinality(t.entries) = 0,
       ARRAY[CAST(ROW('__empty__', 1, 1, NULL) AS {TOKEN_ROW})],
       t.entries)
  ) AS u(tag, m, s, val)
),

grouped AS (
  SELECT
    recid,
    xml_hash,
    multimap_agg(tag, CAST(ROW(m, s, val) AS {ENTRY_ROW})) AS f
  FROM exploded
  GROUP BY recid, xml_hash
)"""


def parse_select(columns, watermark_field: str) -> str:
    """The projection over `grouped`: recid, business columns, then metadata."""
    wm = group_expr(watermark_field)
    lines = ["  recid"]
    lines += [f"  {expr} AS {q(name)}" for name, expr, _ in columns if name != "recid"]
    lines.append(
        f"  TRY(CAST(date_parse("
        f"element_at(transform(filter({wm}, x -> x.m = 1), x -> x.val), 1), '%Y%m%d')"
        f" AS DATE)) AS source_updated_date"
    )
    lines.append("  xml_hash")
    lines.append("  current_timestamp AS ingested_at")
    return "SELECT\n" + ",\n".join(lines) + "\nFROM grouped"


# ---------------------------------------------------------------------------
# Modes
# ---------------------------------------------------------------------------

def mode_ingest(a) -> str:
    """Land raw XML text. Oracle does zero field-level work -- getClobVal() is a
    serialization call, not an XPath query."""
    raw = f"{a.raw_target}"
    return f"""-- Generated by gen_sql.py --mode ingest --table {a.table}
-- Oracle is read-only. XMLTYPE is not natively mapped by the Trino Oracle
-- connector, so the value is serialized with getClobVal() inside a native
-- passthrough. This is the ONLY place getClobVal() appears -- everything
-- downstream reads Iceberg.

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

-- Truncation check: compare against Oracle's own length for the largest doc.
--   Oracle: SELECT recid, dbms_lob.getlength(xmlrecord.getclobval())
--           FROM {a.oracle_table} ORDER BY 2 DESC FETCH FIRST 5 ROWS ONLY;
SELECT recid, length(xmlrecord) AS xml_length
FROM {raw}
ORDER BY xml_length DESC
LIMIT 5;

-- Token completeness: these two counts must match once self-closing tags are
-- normalized (Oracle's XMLTYPE serializer renders empty elements as
-- self-closing, e.g. <c100/>, regardless of how they were written -- confirmed
-- against a live round-trip, not a hypothetical). If they still don't match,
-- the tag regex is missing some other element shape.
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
    return f"""-- Generated by gen_sql.py --mode ingest-refresh --table {a.table}
-- Re-reads all of Oracle and merges by recid. Oracle stays read-only; the
-- target is what becomes incremental. Deletes are intentionally not inferred --
-- that needs a source tombstone/CDC feed.

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
    FROM {a.oracle_table} a'
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


def mode_discover(a, rows) -> str:
    """Which unpinned tags actually carry s sub-values, plus max m/s cardinality.

    Equivalent to _detect_s_value_fields(). Driven by the lookup CSV rather than
    a hardcoded c1..c255 sweep, so it cannot drift from the mapping.
    """
    tags = sorted({t for t, m, _ in rows if m is None and t != "c0"},
                  key=lambda t: int(t[1:]))
    if not tags:
        sys.exit(f"no unpinned (m_index blank) lookup rows for table '{a.table}'")

    checks = []
    for tag in tags:
        g = group_expr(tag)
        checks.append(
            f"  max(CASE WHEN cardinality(filter({g}, x -> x.s > 1)) > 0"
            f" THEN true ELSE false END) AS {q(tag + '_has_s')}"
        )
        checks.append(
            f"  max(COALESCE(array_max(transform({g}, x -> x.m)), 0))"
            f" AS {q(tag + '_max_m')}"
        )

    checks_sql = ",\n".join(checks)
    return f"""-- Generated by gen_sql.py --mode discover --table {a.table}
-- Any *_has_s column returning true must be passed to --fields-with-s so that
-- field is generated as ARRAY(ARRAY(VARCHAR)) instead of a flat array.
-- The *_max_m columns show which fields are genuinely repeating (max_m > 1).

{parse_ctes(a.source, a.watermark_field)}
SELECT
{checks_sql}
FROM grouped;
"""


def mode_bootstrap(a, columns) -> str:
    return f"""-- Generated by gen_sql.py --mode bootstrap --table {a.table}
-- fields_with_s: {', '.join(sorted(a.fields_with_s)) or '(none)'}
-- {len(columns)} business columns parsed in Trino from raw XML text.

CREATE SCHEMA IF NOT EXISTS {a.schema}
WITH (location = 's3://warehouse/bronze');

DROP TABLE IF EXISTS {a.target};

CREATE TABLE {a.target}
WITH (format = 'PARQUET') AS
{parse_ctes(a.source, a.watermark_field)}
{parse_select(columns, a.watermark_field)};

SELECT count(*) AS row_count FROM {a.target};
"""


def mode_incremental(a, columns) -> str:
    names = ["recid"] + [n for n, _, _ in columns if n != "recid"]
    names += [n for n, _ in META_COLS]

    set_list = ",\n".join(
        f"  {q(n)} = s.{q(n)}" for n in names if n != "recid"
    )
    insert_cols = ",\n".join(f"  {q(n)}" for n in names)
    insert_vals = ",\n".join(f"  s.{q(n)}" for n in names)

    return f"""-- Generated by gen_sql.py --mode incremental --table {a.table}
-- fields_with_s: {', '.join(sorted(a.fields_with_s)) or '(none)'}
--
-- {a.watermark_field} (YYYYMMDD) is the source-change watermark. The
-- {a.overlap_days}-day overlap makes re-runs and same-day changes safe.
-- Change detection compares xml_hash, not the {len(names)} parsed columns:
-- every parsed column is a pure function of the raw XML, so one hash
-- comparison is both cheaper and exactly equivalent.
--
-- Prerequisite: run --mode bootstrap once, and --mode ingest-refresh before this.

DROP TABLE IF EXISTS {a.target}_stage;

CREATE TABLE {a.target}_stage
WITH (format = 'PARQUET') AS
{parse_ctes(a.source, a.watermark_field)}
{parse_select(columns, a.watermark_field)};

DROP TABLE IF EXISTS {a.target}_changed;

CREATE TABLE {a.target}_changed
WITH (format = 'PARQUET') AS
SELECT DISTINCT s.recid
FROM {a.target}_stage AS s
CROSS JOIN (
  SELECT COALESCE(
    date_add('day', -{a.overlap_days}, max(source_updated_date)),
    DATE '1900-01-01'
  ) AS lower_bound
  FROM {a.target}
) AS w
WHERE s.source_updated_date >= w.lower_bound
   OR s.source_updated_date IS NULL;

MERGE INTO {a.target} AS t
USING (
  SELECT s.*
  FROM {a.target}_stage AS s
  JOIN {a.target}_changed AS c ON c.recid = s.recid
) AS s
ON t.recid = s.recid
WHEN MATCHED AND t.xml_hash IS DISTINCT FROM s.xml_hash THEN UPDATE SET
{set_list}
WHEN NOT MATCHED THEN INSERT (
{insert_cols}
) VALUES (
{insert_vals}
);

SELECT
  (SELECT count(*) FROM {a.target}_changed) AS changed_records,
  (SELECT max(source_updated_date) FROM {a.target}) AS loaded_through;

DROP TABLE IF EXISTS {a.target}_stage;
DROP TABLE IF EXISTS {a.target}_changed;
"""


def mode_reconcile(a, columns) -> str:
    """The reconcile_iceberg_schema() equivalent, as reviewable DDL.

    ADD COLUMN IF NOT EXISTS is idempotent, so emitting every lookup column adds
    exactly the missing ones without needing to introspect the live schema.
    Type widening cannot be detected without introspection, so it ships as a
    commented template.
    """
    adds = "\n".join(
        f"ALTER TABLE {a.target} ADD COLUMN IF NOT EXISTS {q(name)} {typ};"
        for name, _, typ in columns if name != "recid"
    )
    return f"""-- Generated by gen_sql.py --mode reconcile --table {a.table}
-- Run BEFORE --mode incremental whenever lookup_metadata.csv has changed.
--
-- New lookup rows: the ADD COLUMN statements below are idempotent, so running
-- all of them adds only the columns that are actually missing.
--
-- Widening an existing column (a field that starts carrying real s sub-values,
-- so ARRAY(VARCHAR) must become ARRAY(ARRAY(VARCHAR))) cannot be detected from
-- a SQL script -- re-run --mode discover, then apply the template at the bottom
-- for each affected column and regenerate bootstrap/incremental with the tag
-- added to --fields-with-s.

{adds}

-- Widening template -- uncomment and substitute <col> per affected column:
--
-- ALTER TABLE {a.target} ADD COLUMN <col>_widened ARRAY(ARRAY(VARCHAR));
-- UPDATE {a.target} SET <col>_widened = transform(<col>, x -> ARRAY[x]);
-- ALTER TABLE {a.target} DROP COLUMN <col>;
-- ALTER TABLE {a.target} RENAME COLUMN <col>_widened TO <col>;

SELECT column_name, data_type
FROM {a.schema.split('.')[0]}.information_schema.columns
WHERE table_schema = '{a.schema.split('.')[-1]}'
  AND table_name = '{a.target.split('.')[-1]}'
ORDER BY column_name;
"""


# ---------------------------------------------------------------------------

def main():
    here = Path(__file__).resolve().parent
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mode", default="bootstrap",
                    choices=["ingest", "ingest-refresh", "discover",
                             "bootstrap", "incremental", "reconcile"])
    ap.add_argument("--lookup", default=str(here.parent / "reference" / "lookup_metadata.csv"),
                    help="lookup_metadata.csv path")
    ap.add_argument("--table", default="account",
                    help="table_name to select from the lookup CSV")
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
                    help="parsed output table (default: <schema>.<table>)")
    ap.add_argument("--fields-with-s", default="",
                    help="comma-separated tags carrying s sub-values, from --mode discover")
    ap.add_argument("--fields-with-s-file", default=None,
                    help="file with one such tag per line (alternative to the flag)")
    ap.add_argument("--watermark-field", default="c167",
                    help="XML tag holding the YYYYMMDD source-change date")
    ap.add_argument("--overlap-days", type=int, default=1)
    ap.add_argument("--out", default="-")
    a = ap.parse_args()

    a.oracle_table = a.oracle_table or f"{a.oracle_schema}.{a.table}"
    a.raw_target = f"{a.schema}.{a.table}_raw"
    a.source = a.source or a.raw_target
    a.target = a.target or f"{a.schema}.{a.table}"

    s_fields = {t.strip() for t in a.fields_with_s.split(",") if t.strip()}
    if a.fields_with_s_file:
        with open(a.fields_with_s_file, encoding="utf-8") as fh:
            s_fields |= {ln.strip() for ln in fh if ln.strip() and not ln.startswith("#")}
    a.fields_with_s = s_fields

    if a.mode == "ingest":
        sql = mode_ingest(a)
    elif a.mode == "ingest-refresh":
        sql = mode_ingest_refresh(a)
    else:
        rows = load_lookup(a.lookup, a.table)
        if not rows:
            sys.exit(f"no lookup rows for table '{a.table}' in {a.lookup}")
        unknown = a.fields_with_s - {t for t, _, _ in rows}
        if unknown:
            print(f"WARNING --fields-with-s tags not in lookup: {sorted(unknown)}",
                  file=sys.stderr)
        if a.mode == "discover":
            sql = mode_discover(a, rows)
        else:
            columns = build_columns(rows, a.fields_with_s)
            sql = {
                "bootstrap": mode_bootstrap,
                "incremental": mode_incremental,
                "reconcile": mode_reconcile,
            }[a.mode](a, columns)

    if a.out == "-":
        print(sql)
    else:
        out = Path(a.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(sql, encoding="utf-8")
        print(f"wrote {out}", file=sys.stderr)


if __name__ == "__main__":
    main()
