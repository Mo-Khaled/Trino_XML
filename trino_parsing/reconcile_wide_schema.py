#!/usr/bin/env python3
"""
Live schema reconciliation + pivot for iceberg.bronze.<table>_wide -- the
Trino port of python_parsing.py's _detect_s_value_fields() /
reconcile_iceberg_schema() / _migrate_table_column().

Everything else in this folder (gen_sql.py) is a pure text generator that
never connects to anything -- that's deliberate, and stays true for the
ordinary pivot. This script is the one place that can't work that way,
because matching Spark's actual behavior requires it: Spark inspects
df.schema and the live table's schema at run time and decides on the spot
whether a column needs to widen. There's no way to produce that from SQL
text generated ahead of time -- something has to connect, look at both
shapes, and act, the same way Spark's Python driver does every batch.

What happens, every run:
  1. detect_batch_shapes()  Does account_attributes currently have
                             s_index > 1 for a given lookup row's
                             (field_index, m_index)? Mirrors
                             _detect_s_value_fields() -- a pre-scan before
                             deciding shape. Only two shapes are possible
                             here (VARCHAR or ARRAY(VARCHAR)), not Spark's
                             three: every lookup row in this project is
                             already pinned to one m (see gen_sql.py's
                             load_lookup_csv), so there's no unpinned-m
                             ARRAY(ARRAY(VARCHAR)) branch to begin with.
  2. read_table_types()     account_wide's current column types, via
                             information_schema.columns.
  3. reconcile()             For any lookup row whose batch shape is wider
                             than the table's current column: add, copy,
                             drop, rename -- mirrors _migrate_table_column().
  4. build_column_expr()     Per lookup row, the pivot expression matching
                             the table's shape AFTER reconciliation: an
                             ordered array built from every s value, or a
                             plain scalar lookup.
  5. run_pivot()              Executes the wide bootstrap/incremental pivot
                             directly over this same connection, using
                             those expressions.
"""

import argparse
import sys
from pathlib import Path

import trino

sys.path.insert(0, str(Path(__file__).resolve().parent))
from gen_sql import load_lookup_csv  # noqa: E402


def connect(host, port, user, catalog, schema):
    return trino.dbapi.connect(
        host=host, port=port, user=user, catalog=catalog, schema=schema,
    )


def run(conn, sql):
    cur = conn.cursor()
    cur.execute(sql)
    try:
        return cur.fetchall()
    except trino.exceptions.TrinoQueryError:
        return None


def detect_batch_shapes(conn, source, lookup_rows):
    """{(field_index, m_index): max_s} for every lookup (tag, m) pair, from
    whatever's currently in `source`. max_s > 1 means that column needs
    ARRAY(VARCHAR); mirrors _detect_s_value_fields()'s pre-scan.
    """
    pairs = sorted({(tag, m) for tag, m, _ in lookup_rows if tag != "c0"})
    if not pairs:
        return {}
    where = " OR ".join(f"(field_index = '{t}' AND m_index = {m})" for t, m in pairs)
    rows = run(conn, f"""
        SELECT field_index, m_index, max(s_index) AS max_s
        FROM {source}
        WHERE {where}
        GROUP BY field_index, m_index
    """) or []
    return {(t, m): s for t, m, s in rows}


def read_table_types(conn, wide_target):
    catalog, schema, table = wide_target.split(".")
    rows = run(conn, f"""
        SELECT column_name, data_type
        FROM {catalog}.information_schema.columns
        WHERE table_schema = '{schema}' AND table_name = '{table}'
    """) or []
    return {name: dtype for name, dtype in rows}


def migrate_to_array(conn, wide_target, col_name):
    """add -> copy -> drop -> rename, mirroring _migrate_table_column().
    Iceberg supports all four natively -- this is real schema evolution,
    not a workaround.
    """
    tmp = col_name + "_v2"
    print(f"  migrating \"{col_name}\": VARCHAR -> ARRAY(VARCHAR)", file=sys.stderr)
    run(conn, f'ALTER TABLE {wide_target} ADD COLUMN "{tmp}" ARRAY(VARCHAR)')
    run(conn, f"""UPDATE {wide_target}
        SET "{tmp}" = CASE WHEN "{col_name}" IS NULL THEN NULL ELSE ARRAY["{col_name}"] END""")
    run(conn, f'ALTER TABLE {wide_target} DROP COLUMN "{col_name}"')
    run(conn, f'ALTER TABLE {wide_target} RENAME COLUMN "{tmp}" TO "{col_name}"')


def reconcile(conn, wide_target, lookup_rows, batch_shapes):
    """For every lookup row whose batch needs ARRAY(VARCHAR) but the table
    still has VARCHAR, migrate. Returns the set of (tag, m) that are
    ARRAY(VARCHAR) after this call -- either just-migrated or already were.
    """
    table_types = read_table_types(conn, wide_target)
    array_cols = set()
    name_by_tag_m = {(tag, m): name for tag, m, name in lookup_rows}

    for (tag, m), max_s in batch_shapes.items():
        name = name_by_tag_m.get((tag, m))
        if name is None:
            continue
        current_type = table_types.get(name)
        is_array_now = current_type is not None and current_type.upper().startswith("ARRAY")
        needs_array = max_s is not None and max_s > 1
        if needs_array:
            array_cols.add((tag, m))
            if current_type is not None and not is_array_now:
                migrate_to_array(conn, wide_target, name)
        elif is_array_now:
            # Table is already wider than this batch needs -- matches
            # Spark's "table wider than df" cases: never narrow, so the
            # data stays as an array (a length-1 array is fine).
            array_cols.add((tag, m))

    return array_cols


def build_columns(lookup_rows, array_cols, watermark_field):
    def scalar_expr(tag, m):
        return f"element_at(f, '{tag}_{m}')"

    def array_expr(tag, m):
        key = f"{tag}_{m}"
        g = f"element_at(g, '{key}')"
        return (
            f"CASE WHEN {g} IS NULL THEN NULL ELSE transform("
            f"sequence(1, array_max(transform({g}, x -> x.s))), "
            f"i -> element_at(transform(filter({g}, x -> x.s = i), x -> x.val), 1)"
            f") END"
        )

    columns = ["  recid"]
    names = ["recid"]
    for tag, m, name in lookup_rows:
        col = '"' + name.replace('"', '""') + '"'
        names.append(name)
        if tag == "c0":
            columns.append(f"  recid AS {col}")
        elif (tag, m) in array_cols:
            columns.append(f"  {array_expr(tag, m)} AS {col}")
        else:
            columns.append(f"  {scalar_expr(tag, m)} AS {col}")
    columns.append(
        f"  TRY(CAST(date_parse(element_at(f, '{watermark_field}_1'), "
        f"'%Y%m%d') AS DATE)) AS source_updated_date"
    )
    names.append("source_updated_date")
    return ",\n".join(columns), names


def pivot_cte(source):
    """Same map-once pattern as gen_sql.py's wide_pivot_cte, plus a second,
    multimap_agg-grouped structure (g) alongside the scalar one (f) --
    g keeps every (s, value) pair per key instead of collapsing to one,
    which scalar columns don't need but array columns do.
    """
    return f"""WITH grouped AS (
  SELECT
    recid,
    map_agg(field_index || '_' || CAST(m_index AS VARCHAR), field_value) AS f,
    multimap_agg(
      field_index || '_' || CAST(m_index AS VARCHAR),
      CAST(ROW(s_index, field_value) AS ROW(s INTEGER, val VARCHAR))
    ) AS g,
    max(xml_hash) AS xml_hash
  FROM {source}
  GROUP BY recid
)"""


def run_pivot_bootstrap(conn, wide_target, source, schema, columns_sql):
    run(conn, f"CREATE SCHEMA IF NOT EXISTS {schema} WITH (location = 's3://warehouse/{schema.split('.')[-1]}')")
    run(conn, f"DROP TABLE IF EXISTS {wide_target}")
    run(conn, f"""CREATE TABLE {wide_target} WITH (format = 'PARQUET') AS
{pivot_cte(source)}
SELECT
{columns_sql},
  xml_hash,
  current_timestamp AS ingested_at
FROM grouped""")
    row_count = run(conn, f"SELECT count(*) FROM {wide_target}")
    print(f"bootstrap: {row_count[0][0]} rows in {wide_target}", file=sys.stderr)


def run_pivot_incremental(conn, wide_target, source, columns_sql, names, start_date, end_date):
    if start_date:
        windowed_source = (
            f"(SELECT * FROM {source} "
            f"WHERE ingested_at >= date_parse('{start_date}', '%Y%m%d') "
            f"AND ingested_at < date_parse('{end_date}', '%Y%m%d') + INTERVAL '1' DAY) "
            f"AS windowed_attributes"
        )
    else:
        windowed_source = source

    stage, changed = f"{wide_target}_stage", f"{wide_target}_changed"
    run(conn, f"DROP TABLE IF EXISTS {stage}")
    run(conn, f"""CREATE TABLE {stage} WITH (format = 'PARQUET') AS
{pivot_cte(windowed_source)}
SELECT
{columns_sql},
  xml_hash,
  current_timestamp AS ingested_at
FROM grouped""")

    run(conn, f"DROP TABLE IF EXISTS {changed}")
    run(conn, f"""CREATE TABLE {changed} WITH (format = 'PARQUET') AS
SELECT DISTINCT s.recid
FROM {stage} AS s
LEFT JOIN (SELECT DISTINCT recid, xml_hash FROM {wide_target}) AS t
  ON t.recid = s.recid
WHERE t.recid IS NULL OR t.xml_hash IS DISTINCT FROM s.xml_hash""")

    # Explicit column list, not SELECT s.* -- INSERT matches positionally in
    # Trino, and a just-migrated column physically moves to the end of the
    # table (ADD COLUMN appends; DROP+RENAME doesn't restore its original
    # position), so the stage table's column order and the target table's
    # current column order can genuinely differ after a migration.
    all_names = names + ["xml_hash", "ingested_at"]
    insert_cols = ", ".join(f'"{n}"' for n in all_names)
    select_cols = ", ".join(f's."{n}"' for n in all_names)
    run(conn, f"DELETE FROM {wide_target} WHERE recid IN (SELECT recid FROM {changed})")
    run(conn, f"""INSERT INTO {wide_target} ({insert_cols})
SELECT {select_cols} FROM {stage} AS s JOIN {changed} AS c ON c.recid = s.recid""")
    changed_count = run(conn, f"SELECT count(*) FROM {changed}")
    print(f"incremental: {changed_count[0][0]} changed_records in {wide_target}", file=sys.stderr)

    run(conn, f"DROP TABLE IF EXISTS {stage}")
    run(conn, f"DROP TABLE IF EXISTS {changed}")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--table", default="account")
    ap.add_argument("--schema", default="iceberg.bronze")
    ap.add_argument("--source", default=None,
                    help="attributes table (default: <schema>.<table>_attributes)")
    ap.add_argument("--wide-target", default=None,
                    help="output table (default: <schema>.<table>_wide)")
    ap.add_argument("--lookup", default=str(Path(__file__).resolve().parent.parent
                                             / "reference" / "lookup_metadata.csv"))
    ap.add_argument("--watermark-field", default="c167")
    ap.add_argument("--apply", choices=["bootstrap", "incremental"], default="incremental")
    ap.add_argument("--start-date", default=None)
    ap.add_argument("--end-date", default=None)
    ap.add_argument("--trino-host", default="localhost")
    ap.add_argument("--trino-port", type=int, default=8080)
    ap.add_argument("--trino-user", default="reconcile_wide_schema")
    a = ap.parse_args()

    if bool(a.start_date) != bool(a.end_date):
        sys.exit("--start-date and --end-date must be given together")

    a.source = a.source or f"{a.schema}.{a.table}_attributes"
    a.wide_target = a.wide_target or f"{a.schema}.{a.table}_wide"
    catalog, schema = a.schema.split(".")

    rows = load_lookup_csv(a.lookup, a.table)
    if not rows:
        sys.exit(f"no lookup rows for table '{a.table}' in {a.lookup}")

    conn = connect(a.trino_host, a.trino_port, a.trino_user, catalog, schema)

    print("detecting batch shapes (s > 1 per lookup field)...", file=sys.stderr)
    batch_shapes = detect_batch_shapes(conn, a.source, rows)

    if a.apply == "bootstrap":
        # Nothing to reconcile against yet -- every array-shaped field in
        # this first batch just gets built as an array from the start.
        array_cols = {(t, m) for (t, m), s in batch_shapes.items() if s and s > 1}
    else:
        print("reconciling against live table schema...", file=sys.stderr)
        array_cols = reconcile(conn, a.wide_target, rows, batch_shapes)

    if array_cols:
        print(f"array-typed columns: {sorted(array_cols)}", file=sys.stderr)

    columns_sql, names = build_columns(rows, array_cols, a.watermark_field)

    if a.apply == "bootstrap":
        run_pivot_bootstrap(conn, a.wide_target, a.source, a.schema, columns_sql)
    else:
        run_pivot_incremental(conn, a.wide_target, a.source, columns_sql, names,
                              a.start_date, a.end_date)


if __name__ == "__main__":
    main()
