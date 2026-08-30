# trino_parsing — Trino-native T24 XML parsing

Parses T24 `XMLRECORD` blobs entirely in Trino (regex + array functions), instead
of Oracle's native `XMLTABLE` (see `init-scripts/`) or PySpark (see
`parsing/python_parsing.py`, the bank's production reference for the parsing
semantics — `m` multi-values, `s` sub-values, schema drift). Output lands in
the lakehouse **bronze** layer as Iceberg tables.

`parsing/` is left untouched as reference/history. This
folder is the actual implementation.

## Why Trino instead of Oracle's XMLTABLE

Oracle stays read-only and does no field-level work. The only thing Oracle
does is serialize the XMLTYPE column to text via `getClobVal()` — a plain
`CLOB` conversion, not an XPath query. Every `c1`, `c2`, ... tag is parsed
inside Trino, using the same regex-tokenize approach the rest of this
pipeline is built around.

## How a table is generated

`gen_sql.py` reads `../reference/lookup_metadata.csv` (columns:
`table_name, field_index, m_index, resolved_name_en, dt`) filtered by
`--table`, and prints Trino SQL — it never connects to anything itself. Copy
the output into DBeaver's Trino editor (same manual-runbook model the rest
of this repo uses; there is no orchestrator).

```
python gen_sql.py --mode <mode> --table account [flags] > out.sql
```

| Mode | Produces |
|---|---|
| `ingest` | Bootstrap `iceberg.bronze.<table>_raw` from Oracle via `getClobVal()` |
| `ingest-refresh` | Re-read Oracle, `MERGE` into `<table>_raw` by recid |
| `discover` | Per-tag report of which fields carry real `s` sub-values |
| `bootstrap` | First parse: `CREATE TABLE iceberg.bronze.<table> AS <parsed>` |
| `incremental` | Watermarked stage + `MERGE` into the same table |
| `reconcile` | `ALTER TABLE` statements for lookup drift (new fields) |

### End-to-end workflow

```
1. gen_sql.py --mode ingest       --table account > sql/account/01_ingest_bootstrap.sql
   (run in DBeaver; check the length/token-completeness queries at the bottom)

2. gen_sql.py --mode discover     --table account > sql/account/03_discover_s_fields.sql
   (run in DBeaver; note every *_has_s column that returns true)

3. gen_sql.py --mode bootstrap    --table account --fields-with-s c28,c46,... \
     > sql/account/04_bootstrap.sql
   (run in DBeaver)

4. Cross-check iceberg.bronze.account against the existing
   iceberg.bronze.account_wide (built by init-scripts/ via Oracle XMLTABLE)
   for the same recids -- the two pipelines should agree value-for-value.

5. Ongoing refresh:
   gen_sql.py --mode ingest-refresh --table account
   gen_sql.py --mode incremental    --table account --fields-with-s c28,c46,...
```

Whenever `lookup_metadata.csv` changes for a table, run `--mode reconcile`
first (adds missing columns) before the next `--mode incremental`. If a
field's *shape* changes (starts carrying real `s` values it never had before),
re-run `discover`, regenerate `bootstrap`/`incremental` with the tag added to
`--fields-with-s`, and rebuild that table — this is a deliberate manual
step, not automatic runtime migration (see "Schema evolution" below).

### Multi-table

Every mode takes `--table`, sourced from the shared CSV. `sql/<table>/`
holds each table's checked-in, reviewable output.

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

## Parsing semantics (ported from `python_parsing.py`)

Every XML element is tokenized once into `(tag, m, s, val)`, defaulting a
missing `m`/`s` attribute to `1` (T24 omits it for the first occurrence —
`<c20>` then `<c20 m="2">`, never `<c20 m="1">`). Tokens are then grouped by
tag once via `multimap_agg` into a `MAP<VARCHAR, ARRAY<ROW(m,s,val)>>`, so
every one of the ~290 output columns does a single `element_at` against a
short per-tag array instead of scanning the full per-document token list.
This is the one deliberate structural difference from `parsing/gen_trino_sql.py`
(the earlier, buggy attempt in `parsing/`, which built a scalar map keyed by
`tag_m` — broken for any field with real `s` values, since two elements
sharing a `tag_m` collide as duplicate map keys).

Each lookup row is generated as one of three branches, matching
`_build_select_expressions` in `python_parsing.py` exactly:

- **Branch 1** — `m_index` pinned in the lookup (e.g. `c20`/`m=18` →
  `ac_amt_loc`). Output: `ARRAY(VARCHAR)` indexed by `s`.
- **Branch 2** — `m_index` blank, tag passed via `--fields-with-s` (e.g.
  `c28` when it genuinely carries sub-values). Output: `ARRAY(ARRAY(VARCHAR))`,
  outer indexed by `m`, inner by `s` within that `m`-group.
- **Branch 3** — `m_index` blank, tag not in `--fields-with-s` (the common
  case, e.g. `c29`). Output: flat `ARRAY(VARCHAR)` indexed by `m`, `s` ignored.

Worked example (same as `parsing/README.md`), for
`<c28 m="1" s="1">IN</c28><c28 m="1" s="2">PR</c28><c28 m="2" s="1">PE</c28>`
and `<c29 m="1">100</c29><c29 m="2">200</c29>`:

| field | branch | output |
|---|---|---|
| `c28` (in `--fields-with-s`) | 2 | `[["IN","PR"], ["PE"]]` |
| `c29` | 3 | `["100","200"]` |

`c0` is special-cased to `recid` — it's the `<row id="...">` attribute, not
an XML element.

## Change detection

Every parsed column is a pure function of the raw XML, so incremental
`MERGE` compares one `xml_hash` (`md5` of the raw text) rather than an
`IS DISTINCT FROM` predicate across ~290 array-typed columns — cheaper, and
exactly equivalent since nothing downstream of `xmlrecord` can differ
without the hash differing too.

The watermark (`c167` by default, `--watermark-field` to change) is read
through the same tokenized map as every other field, not a separate Oracle
`XMLQuery`. `--overlap-days` (default 1) matches the pattern in
`init-scripts/ingest_account_xml_incremental.sql`.

## Schema evolution

Spark's `reconcile_iceberg_schema()`/`normalize_arrays()` rely on live
runtime introspection (`df.schema`, a connected `spark.sql(ALTER TABLE ...)`)
that a stateless SQL generator doesn't have. This pipeline instead:

- Emits idempotent `ADD COLUMN IF NOT EXISTS` for every lookup row via
  `--mode reconcile` — safe to run every time the CSV changes, adds only
  what's actually missing.
- Treats a field's *shape* changing (e.g. first real `s` value on a
  previously-scalar-shaped field) as a manual event: re-run `discover`,
  regenerate with the tag added to `--fields-with-s`, rebuild. This is a
  documented operational boundary, the same way `init-scripts` documents
  "deletes are not inferred" rather than solving it automatically.
- Never collapses length-1 arrays to scalars (no `normalize_arrays`
  equivalent) — every Branch-3 column is always `ARRAY(VARCHAR)`, even when
  practically always length 1, specifically to avoid ever needing a
  "was scalar, now needs to be an array" migration in the first place.
  `--mode discover`'s `*_max_m` columns report per-field max cardinality if
  a caller wants to make that collapse decision downstream.

## Validated end-to-end against the local stack

Every mode has been run against a live `docker compose` stack (Oracle XE +
Trino 483 + Iceberg REST + MinIO), not just generated and eyeballed:

- **Self-closing tags are real, not hypothetical.** The original fixture SQL
  writes empty fields as `<c100></c100>`, but Oracle's `XMLTYPE` serializer
  canonicalizes empty elements to self-closing form on `getClobVal()` —
  confirmed via a live round-trip, where it came back as `<c100/>`. The
  original tag regex cannot match that shape (`...>value</cNN>` requires an
  explicit close), which the token-completeness check in
  `01_ingest_bootstrap.sql` caught immediately (163 matched vs. 165 raw tag
  opens on the single-row fixture). **Fixed**: `xmlrecord` is passed through
  `regexp_replace(xmlrecord, '<(c\d+)([^>]*)/>', '<$1$2></$1>')` before
  tokenizing, normalizing self-closing tags back to explicit-close form.
  This is not a cosmetic risk — every field that's ever empty on a given
  record hits this path, since Oracle always serializes empty elements this
  way. Verified afterward: matched-tag count equals raw-tag-open count
  exactly, both on the single-row fixture (165/165) and the 10k-row bulk
  seed (1,650,165/1,650,165).
- **A genuine Trino 483 planner bug**: `NULLIF(...)` used as a field inside a
  `ROW(...)` constructor inside a `transform(...)` lambda fails with
  `class io.trino.sql.ir.Bind cannot be cast to class io.trino.sql.ir.Lambda`
  — reproduced with a minimal literal-array query with no table involved, so
  it's not specific to this pipeline's data or CTE shape. Worked around with
  an equivalent `CASE WHEN v = '' THEN NULL ELSE v END` (see `blank_to_null()`
  in `gen_sql.py`), which does not trigger it.
- **Value-for-value cross-check** against the existing Oracle-`XMLTABLE`
  pipeline (`init-scripts/ingest_account_xml_to_iceberg.sql`) for the same
  `recid`: every field matched exactly, including the Arabic `account_title_1`
  text and the ordered `cap_date_charge`/`alt_acct_id` multivalue arrays
  (the latter correctly includes a leading `NULL` for the empty first
  occurrence — proof the self-closing-tag fix is actually feeding correct
  data downstream, not just satisfying the completeness count). The only
  difference is the deliberate one: this pipeline always emits `ARRAY(VARCHAR)`
  where the old one emits a bare scalar for single-valued fields (see
  "Schema evolution" above).
- **Incremental idempotency**: verified through the real sequence — bootstrap,
  then `incremental` with no source change (`MERGE: 0 rows`, despite the
  record being a re-check candidate inside the overlap window), then a live
  Oracle mutation (`UPDATE account a SET a.xmlrecord = ...`) followed by
  `ingest-refresh` + `incremental` (`MERGE: 1 row`, new value confirmed in
  place, `xml_hash` changed), then `incremental` again with no further change
  (`MERGE: 0 rows`).
- **`reconcile`**: verified both as a no-op against the existing schema and
  by adding a genuinely new lookup row and confirming the `ALTER TABLE ADD
  COLUMN` actually lands the new column.
- **Scale**: `seed_account_xml_bulk.sql`'s 10,000 generated rows (10,001
  total). `ingest`: ~11s. `bootstrap` (all 289 columns): ~20s. `incremental`
  with no real changes: ~16s, 48 re-check candidates (varied watermark dates
  across the seed), 0 actual writes — confirms the `multimap_agg`-by-tag
  grouping keeps per-column cost to a short per-tag array scan rather than
  the full per-document token list, and that idempotency holds at scale, not
  just on a single row.
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
  `parsing/README.md`'s illustrative example. Branch 2 has been validated in
  isolation (the literal-array regression tests above exercise the same
  expression shape) but not against a real document that actually carries
  sub-values. Build one by hand before trusting Branch 2 against production
  T24 data.
- **`MERGE` predicate cost**: mitigated by the `xml_hash` comparison; if
  `xml_hash` itself ever proves too coarse (vanishingly unlikely with `md5`)
  the fallback is comparing individual columns.
- **CLOB truncation** on documents much larger than this fixture's ~4.3KB:
  `01_ingest_bootstrap.sql`'s length cross-check query has been run and
  returns sane values locally, but no real document large enough to risk
  truncation has been available to test against.
