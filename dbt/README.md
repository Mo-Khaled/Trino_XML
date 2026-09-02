# dbt pipeline — Trino-native T24 XML parsing

`dbt run` reads T24 `XMLRECORD` blobs from Oracle (via Trino's `oracle.system.query`
passthrough) and lands the `account` table's Iceberg model: `iceberg.staging.account_raw`
→ `iceberg.staging.account_attributes` (EAV) → `iceberg.bronze.account_wide` (one
named column per `lookup_metadata` row). This project is a 1:1 port of
[`../trino_parsing/`](../trino_parsing/README.md)'s `gen_sql.py`-generated SQL and
`reconcile_wide_schema.py` — read that doc first for the semantics (the `m`/`s`
tokenizing model, self-closing tag normalization, worked examples). This doc covers
how to actually run it.

## Why a hybrid, not a pure dbt pipeline

Everything through the wide pivot is a plain `SELECT` + materialization, which is
dbt's sweet spot — including the wide table's dynamic column list, built at compile
time by a macro that queries the live `lookup_metadata` source (see
`macros/get_lookup_rows.sql`), removing the old "regenerate and recommit `.sql`
files" step entirely.

**One piece deliberately stays outside dbt's model layer**: schema widening (a field
that starts carrying a wider shape than its `account_wide` column currently holds
needs that column migrated in place). That's imperative, conditional DDL — `ADD
COLUMN` → `UPDATE` → `DROP COLUMN` → `RENAME COLUMN` — decided by comparing live
batch shapes against the table's current physical schema. dbt's only built-in answer
to a changed column type is `--full-refresh` (rebuild the whole table), which would
force reprocessing all of `account_attributes` on every widening event and defeat the
whole point of windowed incremental processing. So it's a `run-operation`
(`macros/reconcile_wide_schema.sql`) — a real, separate, individually-loggable step,
not hidden inside a model hook.

## Matching `python_parsing.py` exactly: unpinned `m_index`, and stale columns

Earlier versions of this pipeline coalesced every lookup row's blank `m_index` to
`1` — meaning a field with no pinned `m_index` only ever surfaced its *first*
occurrence in `account_wide`, silently dropping any later `m`-group. That diverged
from `python_parsing.py`'s actual semantics: a lookup row with no `m_index` means
"give me every `m`-group occurrence for this field," which Spark represents as
either a flat array (one value per `m`-group) or, when real `s` sub-values also
appear, a nested array (outer index = `m`-group, inner index = `s`-slot within it).
This mattered a lot in practice — **255 of the 289 current `account` lookup rows
have a blank `m_index`** — and reconciling against real data during this fix found
9 fields (`inputter`, `date_time`, `alt_acct_id`, ...) that had always carried a
second `m`-group occurrence, silently truncated by the old logic.

`macros/get_wide_select.sql` now implements all three of `_build_select_expressions()`'s
branches:
- **Branch 1** (`m_index` pinned) — unchanged: `element_at(f, 'tag_m')` scalar, or
  the `s`-indexed array, exactly as before.
- **Branch 2** (`m_index` unpinned, real `s`-values present) — `wide_branch2_expr()`:
  `ARRAY(ARRAY(VARCHAR))`, outer array over `m`-groups, inner array over `s`-slots.
  Reads from a third pivot map, `h` (`wide_pivot_cte()`), keyed by tag alone —
  every `(m, s, value)` triple for that field, since Branch 1's `f`/`g` maps (keyed
  by `tag_m`) can't represent "every `m`-group" for an unpinned field.
- **Branch 3** (`m_index` unpinned, no `s`-values) — `wide_branch3_array_expr()`:
  flat `ARRAY(VARCHAR)`, one value per `m`-group. Can still collapse to a plain
  scalar (`wide_branch3_scalar_expr()`) when every record in the batch only ever
  has one `m`-group for that field — mirroring `normalize_arrays()`'s single-element
  flatten, including its edge case: the collapse is `element_at()` of position 1 of
  the full array, so if a record's one-and-only occurrence isn't at `m=1`, the
  scalar comes back `NULL` rather than that value. T24's own convention (omit `m`
  entirely on a first/only occurrence, which the tokenizer already defaults to `1`)
  means this shouldn't bite in practice — it's called out here because it's a
  faithful port of Spark's actual behavior, not something invented for this port.

Which shape (`scalar`/`array`/`nested`) each column currently needs — and which
migration path to take when it needs to widen — is decided by
`macros/get_column_shapes.sql` and `reconcile_wide_schema`'s extended
`migrate_wide_column()`, which now covers all three of `reconcile_iceberg_schema()`'s
real migration cases: `scalar → array`, `scalar → nested`, and `array → nested`
(the fourth-through-sixth cases in the Spark reference — DataFrame narrower than the
table — don't need separate handling here, same as before: `get_wide_select` always
builds the expression matching the column's *current physical* shape, which already
produces the wider form Spark's Cases D/E/F would otherwise wrap into).

**Step 1 of `reconcile_iceberg_schema()`** (a column exists in the table but no
longer has a matching lookup row — renamed or removed from `lookup_metadata`) is
now handled explicitly too, via `get_stale_columns()`: rather than just omitting
such a column from the `SELECT` (which would still work — Trino's
`INSERT INTO target (subset of cols)` leaves the rest untouched on unaffected rows
and `NULL` on new/changed ones — but isn't what Spark's code actually does), every
stale column gets an explicit fill matching `reconcile_iceberg_schema()` exactly:
`NULL` for a scalar column, an empty array for an array-or-nested one
(`F.lit(None)` vs `F.array()` in the Spark reference).

Both were verified live against `account`: the 9 real fields above correctly
widened `scalar → array` with all prior values preserved; a synthetic record with
genuine multi-`m`/multi-`s` values correctly produced nested arrays matching
Spark's own worked example shape (`[['IN','PR','PE'], ...]`); a synthetic stale
column with existing data on every row was correctly wiped (`NULL`/`[]`) only on
the one row that got recomputed, leaving the other 10,000 rows' stale value
untouched.

## Setup

```powershell
python -m pip install dbt-trino
```

Runs from the host against Trino's exposed `localhost:8080`, same as
`trino_parsing`'s scripts — no new docker-compose service. Point dbt at the
repo-local profile:

```powershell
$env:DBT_PROFILES_DIR = "."   # from this dbt/ directory
dbt debug
```

`profiles.yml` reads `TRINO_HOST`/`TRINO_PORT` via `env_var()`, defaulting to
`localhost`/`8080` if unset. Those two vars live in the repo-root `.env` file
(same file the Oracle/MinIO containers use) — no manual loading needed:
`dbt-core` depends on `python-dotenv` and auto-loads a `.env` file (searching
upward from the current directory) on every invocation, the same way
`docker compose` does for the containers, just via a different mechanism.
Edit `.env`, run any `dbt` command, and it takes effect immediately. A real
environment variable already set in your shell still takes precedence over
`.env` (`override=False`), so this is only something to think about if you
ever need to point dbt at a different Trino host/port than this local stack.

`iceberg.staging` and `iceberg.bronze` must already exist (see
`trino_parsing/README.md`'s schema creation, or just run `dbt run` once —
the schemas are expected to exist already; if starting from nothing, create
them once via Trino: `CREATE SCHEMA iceberg.staging WITH (location =
's3://warehouse/staging')` and the same for `bronze`).

`iceberg.bronze.lookup_metadata` is treated as a **source** (externally/manually
owned, not created by this project) — see `models/bronze/_bronze__sources.yml`.
For local dev/CI where nothing else populates it, `seeds/lookup_metadata.csv`
(a copy of `reference/lookup_metadata.csv`) seeds it directly:

```powershell
dbt seed
```

## Day-to-day run (the Airflow-task sequence)

```powershell
# 1. Ingest + tokenize. Omit --vars for a full read (first run, or a full resync).
dbt run --select account_raw account_attributes --vars '{start_date: "20260901", end_date: "20260901"}'

# 2. Widen account_wide's schema BEFORE pivoting, if this batch needs it.
#    Always safe to run -- no-ops if account_wide doesn't exist yet or nothing changed.
dbt run-operation reconcile_wide_schema --args '{table_name: account}'

# 3. Pivot the (possibly windowed) changed rows into account_wide.
dbt run --select account_wide --vars '{start_date: "20260901", end_date: "20260901"}'

# 4. Verify.
dbt test
```

`start_date`/`end_date` (`YYYYMMDD`) are the same explicit, auditable window
`gen_sql.py` used via `--start-date`/`--end-date` — give the scheduler's actual
run date, not an auto-computed rolling window, so a specific day can always be
re-run on demand. They only take effect once a model's target table already
exists (`is_incremental()`); the first run of each model always does a full
build regardless of vars. To force a full rebuild later, use `--full-refresh`.

## Mode → model mapping

| `gen_sql.py` mode | dbt equivalent |
|---|---|
| `ingest` / `ingest-refresh` | `dbt run --select account_raw` (`is_incremental()` picks bootstrap vs. windowed refresh) |
| `bootstrap` / `incremental` | `dbt run --select account_attributes` |
| `reconcile` | `dbt test` (the `lookup_coverage` generic test on `account_attributes`) |
| `wide` / `wide-incremental` | `dbt run --select account_wide` |
| `reconcile_wide_schema.py` | `dbt run-operation reconcile_wide_schema --args '{table_name: account}'` |

## What's different from a naive dbt port (read before touching the incremental logic)

- **`account_raw` needs a custom incremental strategy** (`macros/conditional_merge.sql`,
  `config(incremental_strategy='conditional_merge')`). dbt-trino's built-in `merge`
  strategy always does an unconditional `WHEN MATCHED THEN UPDATE`. That's wrong here:
  `account_attributes` windows its own re-tokenization by `account_raw.ingested_at`,
  so an unconditional update would bump `ingested_at` on every unchanged row and
  force a full re-tokenize on every run. `conditional_merge` only updates a row
  `WHEN MATCHED AND xmlrecord IS DISTINCT FROM`, a 1:1 port of `gen_sql.py`'s
  `mode_ingest_refresh` MERGE.
- **`account_attributes` and `account_wide` add an explicit `changed` CTE
  before relying on dbt's built-in `delete+insert` strategy.** The `ingested_at`
  window is an optimization on what gets re-tokenized/re-pivoted, not the
  correctness check — same invariant `gen_sql.py`'s docstrings state explicitly.
  Found live during this migration: without the `changed` CTE, dbt's built-in
  `delete+insert` treats "every row in the windowed source" as "changed," which
  collapses to the same set in the common case but silently breaks whenever the
  window is coarser than that — e.g. a bootstrap and an incremental run landing
  on the same calendar day (day-granularity windowing can't tell them apart).
  Reproduced and fixed during validation; see the model files' comments.
- **`account_wide`'s array-vs-scalar column decision is read-only within the
  model.** `macros/get_array_columns.sql` trusts `account_wide`'s *physical*
  column types via `information_schema` when the table already exists — it does
  **not** re-derive "does this need widening." That decision lives in exactly one
  place, the `reconcile_wide_schema` run-operation. Skipping step 2 above before
  a batch that needs widening will fail loudly with a Trino `TYPE_MISMATCH`
  (writing an array expression into a still-`VARCHAR` column), not silently
  mis-type data.

## Local stack note: `iceberg-rest`'s SQLite catalog needs a busy_timeout

The reference `tabulario/iceberg-rest` image's default SQLite catalog has no
`busy_timeout` configured, so any brief overlap on its single connection fails
immediately with `SQLITE_BUSY` instead of waiting. This was 100% reproducible
building `account_wide` (289 columns) — even a schema-only `CREATE TABLE` with
zero data failed every time, while small tables succeeded fine; a wider schema
means more time on the connection, more contention. `docker-compose.yml`'s
`iceberg-rest` service now sets `CATALOG_URI` with `busy_timeout=30000` and
`journal_mode=WAL`, backed by a named volume (`iceberg-catalog-data`) so a
container recreate no longer silently loses the catalog's table registry the
way it used to (the SQLite file previously lived only in the container's
ephemeral writable layer). If you ever need to recreate the volume from
scratch, the new mount is owned by uid/gid `1000` (the image's `iceberg` user)
— `docker run --rm -v <volume>:/var/iceberg-catalog busybox chown -R 1000:1000 //var/iceberg-catalog`
before starting the container, or it'll crash-loop with `SQLITE_CANTOPEN`.

## Validated

Full `dbt build` (seed + all 3 models + all 5 tests) passes clean against the
local stack. Baselines match `trino_parsing/README.md`'s documented,
already-verified numbers exactly:

- `account_raw` bootstrap: 10,001 rows.
- `account_attributes` bootstrap: 1,650,165 rows.
- `account_wide` bootstrap: 10,001 rows, 289 lookup-driven columns.
- Single-record Oracle mutation → `account_raw` MERGE touches exactly 1 row,
  `account_attributes` incremental touches exactly 165 rows (that recid's
  attribute rows), rest of the table untouched.
- Idempotency: re-running the whole `dbt build` sequence with no source change
  reports 0 changed rows at every step.
- Schema drift: a synthetic record with a real `s=2` value for `account_officer`
  (`c11`) correctly triggers `reconcile_wide_schema` to migrate the column
  `VARCHAR` → `ARRAY(VARCHAR)`; every pre-existing scalar value survives wrapped
  (`999` → `[999]`); the new record's values land as an ordered array
  (`[JSMITH, RJONES]`).

## Not yet done

- Only `account` is onboarded. `models/staging/account_raw.sql`,
  `account_attributes.sql`, and `models/bronze/account_wide.sql` are
  each just `config(...)` plus one argument-free macro call —
  `raw_ingest_model()` / `attributes_model()` / `wide_model()`
  (`macros/table_pipeline.sql`). These macros take no `table_name` argument
  on purpose: they derive it from the model's own filename (`model.name`,
  stripping the known `_raw`/`_attributes`/`_wide` suffix) instead of taking
  it as a string argument. That's deliberate — with an argument, table
  identity would have to be typed correctly in two places (the filename
  *and* the argument), and a copy-pasted file that gets renamed but keeps a
  stale argument would silently build a table with the right name and the
  wrong data. With zero arguments there's exactly one place to get it right:
  the filename, which is what determines the resulting table name anyway.
  Onboarding a new table (there are 100+ in the source database) is:
  confirm `lookup_metadata` has rows for that `table_name`, then add three
  correctly-*named* one-line model files directly under `models/staging/`
  (`<table>_raw.sql`, `<table>_attributes.sql`) and `models/bronze/`
  (`<table>_wide.sql`) — no per-table subfolder; with each file now just
  two lines, a subfolder per table added nesting without payoff, and the
  filenames already sort together by table prefix in one flat directory.
  Each new file is just `config(...)` + the matching macro call, plus a
  schema `.yml` (copy `account__models.yml`) and the two raw-layer tests
  (copy `tests/account_raw_*.sql`, swap the `ref()`s). No macro changes needed —
  `table_pipeline.sql`'s macros and everything they call (`token_ctes`,
  `get_lookup_rows`, `get_array_columns`, `get_wide_select`,
  `reconcile_wide_schema`) already take the table name/relation as an
  argument (or derive it, for these three). At 100+ tables, generating
  those files with a small script (instead of hand-copying each) is worth
  building.
- No Airflow DAG yet — the 4-step sequence above is what a DAG's tasks would
  run, in that order.
- `trino_parsing/`'s `gen_sql.py`-generated `.sql` files and
  `reconcile_wide_schema.py` are kept as the frozen reference this was ported
  from; they're candidates for retirement once this path has run in
  production for a while, not removed yet.
- `lookup_metadata` ownership in a real (non-local) environment hasn't been
  confirmed — if it turns out this repo should own it rather than some
  external process, `dbt seed` should become the source of truth instead of
  a dev-only convenience (see `models/bronze/_bronze__sources.yml`).
