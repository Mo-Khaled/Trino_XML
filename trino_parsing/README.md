# trino_parsing — Trino-native T24 XML parsing

Parses T24 `XMLRECORD` blobs entirely in Trino (regex + array functions), instead
of Oracle's native `XMLTABLE` (see `init-scripts/`) or PySpark (see
`parsing/python_parsing.py`, the bank's production reference for the parsing
semantics — `m` multi-values, `s` sub-values). Output lands in the lakehouse
**bronze** layer as an Iceberg table.

`parsing/` is left untouched as reference/history. This
folder is the actual implementation.

## Why Trino instead of Oracle's XMLTABLE

Oracle stays read-only and does no field-level work. The only thing Oracle
does is serialize the XMLTYPE column to text via `getClobVal()` — a plain
`CLOB` conversion, not an XPath query. Every `c1`, `c2`, ... tag is parsed
inside Trino, using the same regex-tokenize approach the rest of this
pipeline is built around.

## How a table is generated

`gen_sql.py` prints Trino SQL — it never connects to anything itself. Copy
the output into DBeaver's Trino editor (same manual-runbook model the rest
of this repo uses; there is no orchestrator).

```
python gen_sql.py --mode <mode> --table account [flags] > out.sql
```

Every mode is genuinely table-agnostic. Output is one row per
`(recid, field_index, m_index, s_index)` — an EAV shape, not named business
columns — so the generated SQL has a fixed 7-column shape no matter what any
lookup table contains. `--table` only ever changes a `WHERE` filter or a
table-name substitution, never the SQL's shape. Business-field names are
never read by Python at all: only `--mode reconcile` joins against
`--lookup-table` (a live table, default `iceberg.bronze.lookup_metadata`),
and only inside the generated SQL itself, at query time.

| Mode | Produces |
|---|---|
| `ingest` | Bootstrap `iceberg.bronze.<table>_raw` from Oracle via `getClobVal()` |
| `ingest-refresh` | Windowed re-read of Oracle, `MERGE` into `<table>_raw` by recid |
| `bootstrap` | First parse: `CREATE TABLE iceberg.bronze.<table>_attributes AS <tokenized>` |
| `incremental` | Re-tokenize, delete+insert only recids whose `xml_hash` changed |
| `reconcile` | Lookup-coverage report: tags present in the data with no lookup row |
| `wide` | First pivot: `CREATE TABLE iceberg.bronze.<table>_wide` (named columns, from `--lookup`) |
| `wide-incremental` | Re-pivot, delete+insert only recids whose `xml_hash` changed |

### End-to-end workflow

```
1. gen_sql.py --mode ingest       --table account > sql/account/01_ingest_bootstrap.sql
   (run in DBeaver; check the length/token-completeness queries at the bottom)

2. gen_sql.py --mode bootstrap    --table account > sql/account/03_bootstrap.sql
   (run in DBeaver)

3. gen_sql.py --mode wide         --table account --lookup <CSV export of
     lookup_metadata> > sql/account/06_wide.sql
   (run in DBeaver; pivots account_attributes into named business columns)

4. Ongoing refresh (run daily with that day's window, e.g. from an Airflow DAG):
   gen_sql.py --mode ingest-refresh    --table account --start-date X --end-date Y
   gen_sql.py --mode incremental       --table account --start-date X --end-date Y
   gen_sql.py --mode wide-incremental  --table account --start-date X --end-date Y \
     --lookup <fresh CSV export of lookup_metadata>
```

`wide-incremental` windows its source scan by `account_attributes.ingested_at`
— which `incremental` only ever bumps for recids it actually changed — so it
never has to re-pivot the whole table. Like `incremental`, the window is an
optimization on what gets re-pivoted, not the correctness check: comparing
`xml_hash` against what's already in `<table>_wide` is what actually decides
which recids get replaced, so using the same window as the `incremental` run
that fed it (or a wider one) is required, but getting it slightly wide is
harmless — only genuinely different recids ever get touched.

### Schema drift: a field that starts carrying real `s` sub-values

`gen_sql.py --mode wide`/`wide-incremental` always pivot a lookup-pinned
`(field_index, m_index)` as a single scalar value — that's fine until T24
starts sending more than one `s` value for a position that used to only ever
have one (e.g. `c11`/`m=1` had a single value for the first 10 batches, then
batch 11 has two). At that point the column genuinely needs to become
`ARRAY(VARCHAR)`, and the existing table's column has to be widened in
place — this is the Trino port of `python_parsing.py`'s
`reconcile_iceberg_schema()`/`_migrate_table_column()`.

This can't be done from generated SQL text the way every other mode works,
for the same reason Spark's version isn't a static query either: the
decision (does this column need widening?) depends on live state — what
today's batch actually contains, and what the table's column type currently
is — inspected and acted on at run time. `trino_parsing/reconcile_wide_schema.py`
is a live-connected script (needs `pip install trino`) instead of a text
generator, run in place of `gen_sql.py --mode wide`/`wide-incremental`:

```
python reconcile_wide_schema.py --table account --apply bootstrap
python reconcile_wide_schema.py --table account --apply incremental \
  --start-date X --end-date Y
```

What it does, every run: pre-scans `account_attributes` for any lookup field
whose `s_index` now exceeds 1 (mirrors `_detect_s_value_fields`); for any
such field whose column is still `VARCHAR` in `<table>_wide`, runs the
add/copy/drop/rename sequence to widen it to `ARRAY(VARCHAR)` (mirrors
`_migrate_table_column`); then runs the pivot itself, building an ordered
array from every `s` value for columns that need it and a plain scalar
lookup for everything else. Only two shapes are possible here (`VARCHAR` or
`ARRAY(VARCHAR)`), not Spark's three — every lookup row in this project is
already pinned to one `m` (see `load_lookup_csv`), so there's no
unpinned-`m` `ARRAY(ARRAY(VARCHAR))` case to begin with.

Verified live: seeded a synthetic record with two `s` values for a
previously-always-scalar field, ran `--apply incremental` — the column
correctly migrated (`VARCHAR -> ARRAY(VARCHAR)`), the existing 10,001
records' prior scalar values came through safely wrapped as single-element
arrays (e.g. `[111]`, not lost or nulled), and the new record's two values
landed correctly as an ordered array (`[AAA, BBB]`).

`init-scripts/` only sets up the local Oracle fixture (`ACCOUNT` table +
sample data) -- it never writes to Iceberg. `account_xml_attributes` and the
other Oracle-`XMLTABLE`-built tables that used to live alongside this
pipeline have been removed; this pipeline's tables (`account_raw`,
`account_attributes`, `account_wide`) are the only ones now.

There is no `discover` step and no `--fields-with-s` flag. Those existed
only to decide, ahead of generation, between two possible column shapes for
a field with no pinned `m_index` — a decision the old wide-named-column
design was forced to make because SQL fixes a column's type when the query
is written. The EAV shape has no such decision: every value's `m_index` and
`s_index` are stored as plain data, for every field, unconditionally.
Anyone curious which fields repeat or carry real sub-values can just query
the resulting table directly:

```sql
SELECT field_index, max(m_index) AS max_m, bool_or(s_index > 1) AS has_s
FROM iceberg.bronze.account_attributes
GROUP BY field_index
```

### Windowed refresh (`--start-date`/`--end-date`)

`ingest-refresh` re-reads Oracle every run. Without a window, that means
transferring every XML document every time, regardless of whether it
changed -- at real data volumes that's the dominant cost of running this
daily. `--start-date`/`--end-date` (YYYYMMDD) add a `WHERE` clause to the
*Oracle-side* query itself, filtering on `--watermark-field` (default
`c167`) via `XMLQuery` *before* `getClobVal()` runs -- so rows outside the
window are never serialized or transferred, not just excluded from the
`MERGE` afterward. Must be given together; omitting both reads all of
Oracle (useful for an occasional full resync, not the daily path). Give the
scheduler's actual day (plus a day or two of overlap, since `c167` is a
source-reported date, not a guarantee) rather than computing a rolling
window automatically, so a specific day can always be re-run on demand with
an explicit, auditable window.

Whenever a table's real production data starts showing an XML tag shape
the tokenizer doesn't handle yet (see `01_ingest_bootstrap.sql`'s
token-completeness check), or the lookup dictionary gains fields nobody's
named yet (`--mode reconcile`), those are the two things worth periodically
re-checking -- neither requires touching `gen_sql.py` itself, since the
EAV table's schema never needs to change.

### Multi-table

Every mode takes `--table`. `sql/<table>/` holds each table's checked-in,
reviewable output. Unlike the CSV-based earlier design, no lookup file is
read to generate `ingest`/`ingest-refresh`/`bootstrap`/`incremental` at
all -- `--table` only changes which raw source table is read and which
attributes table is written. Only `reconcile` consults the lookup table,
and only as a live SQL join (`--lookup-table`), filtered by
`table_name = '<table>'`.

### Oracle connector user vs. schema owner

`ORACLE_APP_USER`/`ORACLE_APP_PASSWORD` in `.env` (used by both Trino's
Oracle connector and DBeaver) is currently `system` — Oracle's built-in
admin account, not `source_user`, the schema that actually owns `ACCOUNT`
and every other source table (named for what it is: the account that owns
the *source* data, not a lakehouse-layer name like the old `bronze_user`).
Since `system` isn't the owner, every `oracle.system.query(...)` passthrough
must schema-qualify table references (`source_user.account`, not bare
`account`) or Oracle returns `ORA-00942: table or view does not exist`.

`gen_sql.py` handles this by default: `--oracle-schema` (default
`source_user`) is prefixed onto `--table` to build the Oracle-side
reference, so generated SQL is qualified automatically regardless of which
user the connector runs as. Override `--oracle-table` directly if a
specific table lives in a different schema. The two hand-written
`init-scripts/*.sql` files were updated to match
(`FROM source_user.account a`) — if `ORACLE_APP_USER` ever changes back to
`source_user` itself, the qualification is harmless (a user can always
schema-qualify its own tables), so this doesn't need to be conditional on
which user is configured.

## Output shape

`iceberg.bronze.<table>_attributes` — one row per `(recid, field_index,
m_index, s_index)`:

| column | meaning |
|---|---|
| `recid` | the record's ID |
| `field_index` | the XML tag, e.g. `c20` |
| `m_index` | the tag's `m` attribute, defaulted to `1` when absent |
| `s_index` | the tag's `s` attribute, defaulted to `1` when absent |
| `field_value` | the tag's text content, entity-decoded, `NULL` if empty |
| `xml_hash` | `md5` of the record's raw XML (see "Change detection") |
| `ingested_at` | when this row was written |

Every XML element is tokenized once into `(tag, m, s, val)` — defaulting a
missing `m`/`s` attribute to `1` (T24 omits it for the first occurrence:
`<c20>` then `<c20 m="2">`, never `<c20 m="1">`) means nothing downstream
needs to special-case an absent attribute. `c0` is special-cased to `recid`
— it's the `<row id="...">` attribute, not a real XML element, so it never
appears as a `field_index`.

This preserves the full information `python_parsing.py`'s `_VALUE`/`_m`/`_s`
struct captures per element — nothing about `m` or `s` is discarded or
collapsed. What's different from that file's output is the *shape*: named
business columns there, generic attribute rows here. Turning
`field_index = 'c20' AND m_index = 18` into a column named `ac_amt_loc` is a
presentation-layer decision (a view, or a downstream pivot), not something
this pipeline's core tables do -- the same SQL-can't-produce-a-dynamic-
column-list constraint that made the old design need per-table codegen for
every mode applies to that view too, so it isn't built by default here.

## Change detection

Every attribute row is a pure function of the raw XML, so `incremental`
detects change with one `xml_hash` (`md5` of the raw text) per recid,
compared between a fresh re-tokenization and what's already stored --
cheaper than comparing `field_value` row by row, and exactly equivalent
since nothing can differ without the hash differing too.

Changed recids are handled by **delete then insert**, not a per-attribute
`MERGE`: a changed record can gain or lose tags entirely, not just change a
value, and delete+insert is the only way to also drop attribute rows for a
tag that disappeared from that record. Unchanged recids' rows are never
touched. There is no watermark or overlap-day window inside `incremental`
itself -- that only exists in `ingest-refresh`'s Oracle-side read, a
separate, source-side cost concern; the target-side diff here is a plain,
exact hash comparison with no date field or clock-skew tolerance involved.

## Schema evolution

The EAV table's schema is fixed -- 7 columns, always, regardless of what
the lookup table contains -- so there is no `ALTER TABLE` concept here at
all. A new field appearing in real data, or a new lookup row naming a field
that was already being tokenized, needs no migration: the data was already
being captured under its `field_index`, or starts being captured the next
time `ingest`/`incremental` runs. `--mode reconcile` instead reports the
one thing that *can* still drift -- a tag showing up in real, tokenized
data with no lookup row naming it yet -- as a plain SQL join against
`--lookup-table`, not a DDL generator.

## Validated end-to-end against the local stack

Every mode has been run against a live `docker compose` stack (Oracle XE +
Trino 483 + Iceberg REST + MinIO), not just generated and eyeballed:

- **Self-closing tags are real, not hypothetical.** The original fixture SQL
  writes empty fields as `<c100></c100>`, but Oracle's `XMLTYPE` serializer
  canonicalizes empty elements to self-closing form on `getClobVal()` —
  confirmed via a live round-trip, where it came back as `<c100/>`. The
  original tag regex cannot match that shape (`...>value</cNN>` requires an
  explicit close), which the token-completeness check in
  `01_ingest_bootstrap.sql` caught immediately. **Fixed**: `xmlrecord` is
  passed through `regexp_replace(xmlrecord, '<(c\d+)([^>]*)/>', '<$1$2></$1>')`
  before tokenizing, normalizing self-closing tags back to explicit-close
  form. Verified: matched-tag count equals raw-tag-open count exactly on
  the 10k-row bulk seed (1,650,165/1,650,165), and `iceberg.bronze.account_attributes`
  lands exactly 1,650,165 rows -- one per matched tag, none lost or
  duplicated.
- **Truncation check is a real, all-rows comparison, not a sampled one.**
  `01_ingest_bootstrap.sql` joins Iceberg's `length(xmlrecord)` against
  Oracle's own `dbms_lob.getlength()` for every recid in one query (a
  second, cheap Oracle passthrough -- `getlength()` on every row, not
  `getClobVal()` again); zero rows back means no truncation anywhere.
  Verified: 0 rows on the 10k-row seed.
- **A genuine Trino 483 planner bug**: `NULLIF(...)` used as a field inside a
  `ROW(...)` constructor inside a `transform(...)` lambda fails with
  `class io.trino.sql.ir.Bind cannot be cast to class io.trino.sql.ir.Lambda`
  — reproduced with a minimal literal-array query with no table involved, so
  it's not specific to this pipeline's data or CTE shape. Worked around with
  an equivalent `CASE WHEN v = '' THEN NULL ELSE v END` (see `blank_to_null()`
  in `gen_sql.py`), which does not trigger it.
- **Spot-checked values** against the known fixture record
  (`9000000112345001`): `c1`, and all five `c20` sub-values (including the
  empty `m=32` slot correctly landing as `NULL`) matched the source XML
  exactly.
- **Incremental correctness and isolation**: mutated one field
  (`c11: 999 -> 777`) directly in Oracle, ran `ingest-refresh`
  (`MERGE: 1 row`) then `incremental` — `changed_records: 1`,
  `DELETE: 165 rows` / `INSERT: 165 rows` (that one record's full attribute
  set), total table row count unchanged at 1,650,165, and the new value
  confirmed in place. The other 10,000 records' 1,650,000 rows were not
  touched.
- **`reconcile`**: verified as a 0-row report against the current lookup
  table (every tokenized tag has a naming row), confirming it correctly
  reports coverage gaps rather than always returning something.
- **Windowed `ingest-refresh`**: a 10-day test window
  (`c167 BETWEEN 20260101 AND 20260110`) pulled 360 of 10,001 rows from
  Oracle, confirming the `WHERE` clause genuinely restricts what
  `getClobVal()` runs against, not just what gets merged afterward.
- **Scale**: `seed_account_xml_bulk.sql`'s 10,000 generated rows (10,001
  total). `bootstrap`: ~1.65M rows in well under a minute.
  - Oracle XE's default `pga_aggregate_limit` (2048 MB, set at the CDB root)
    was too low to serialize all 10,000 `XMLTYPE` documents in one
    `oracle.system.query` passthrough (`ORA-04036`). Raised to 4G via
    `ALTER SYSTEM SET pga_aggregate_limit=4G SCOPE=BOTH` connected to the
    CDB root (`sqlplus sys/...@//host:1521/XE`, not the `XEPDB1` service —
    a PDB-level `ALTER SYSTEM` is capped by the root's limit and will error).
    This is an Oracle XE resource default, not a query design problem; a
    properly-sized Oracle instance shouldn't need this, but it's worth
    knowing about when reproducing scale tests against a fresh local volume.

## Known risks / things to verify against real production data

- **`s` sub-values are unverified locally** — `s="N"` does not appear
  anywhere in the local fixture or 10k-row bulk seed, only in
  `parsing/README.md`'s illustrative example. The EAV design stores
  `s_index` unconditionally for every value, so there's no separate code
  path that specifically needs testing the way the old design's nested-array
  branch did — but no real document exercising `s > 1` has actually been
  run through this pipeline yet.
- **CLOB truncation** on documents much larger than this fixture's ~4.3KB:
  the truncation check has been run against every row of the 10k-row seed
  and returns 0 mismatches, but no real document large enough to seriously
  risk truncation has been available to test against.
