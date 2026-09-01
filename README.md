# Oracle XML to Trino Iceberg

This repository is a local Docker reference for reading T24-style Oracle
`XMLTYPE` account records and publishing an Iceberg reporting model in Trino.

Oracle is read-only during ingestion. XML parsing happens entirely in Trino
(regex + array functions) — see [trino_parsing/README.md](trino_parsing/README.md)
for the actual pipeline: how a table is generated, ingest/bootstrap/incremental/
wide modes, and the querying/troubleshooting details specific to it.

## Architecture

```text
Oracle ACCOUNT (RECID, CURRENCY, XMLTYPE)
        |
        | getClobVal() passthrough -- Oracle does no field-level work
        v
Trino (regex-tokenizes the XML text) -> Iceberg REST catalog + MinIO
        |
        +-- account_raw          (raw XML text, landed once)
        +-- account_attributes   (EAV: one row per tag occurrence)
        +-- account_wide         (pivoted, named business columns)
        +-- lookup_metadata      (tag -> business-field mapping)
```

`init-scripts/` only sets up the local Oracle fixture (`ACCOUNT` table +
sample data) — it does not write anything to Iceberg. The actual XML-parsing
pipeline, including how these tables are generated and kept up to date, lives
entirely in `trino_parsing/`.

## Services

| Service | Image | Port | Purpose |
|---|---|---:|---|
| Oracle XE | `gvenzl/oracle-xe:21-slim` | 1521 | Local XMLTYPE source fixture. |
| Trino | `trinodb/trino:483` | 8080 | SQL engine and Oracle/Iceberg connectors. |
| MinIO | `minio/minio:RELEASE.2025-04-22T22-12-26Z` | 9000, 9001 | Iceberg object storage and console. |
| Iceberg REST | `tabulario/iceberg-rest:1.6.0` | 8181 | Iceberg catalog. |

## Prerequisites

- Docker Desktop with Docker Compose.
- DBeaver with Oracle and Trino drivers.
- PowerShell for the Docker commands.

## Configure and start

Copy the template, then replace every placeholder with local credentials:

```powershell
Copy-Item .env.example .env
```

| Variable | Used by |
|---|---|
| `ORACLE_PASSWORD` | Oracle XE administrator setup. |
| `ORACLE_APP_USER` | Oracle application user and Trino Oracle connector. |
| `ORACLE_APP_PASSWORD` | Oracle application user and Trino Oracle connector. |
| `MINIO_ROOT_USER` | MinIO, Iceberg REST, and Trino Iceberg connector. |
| `MINIO_ROOT_PASSWORD` | MinIO, Iceberg REST, and Trino Iceberg connector. |

Start the stack:

```powershell
docker compose up -d
docker compose ps
```

If Oracle is still starting, follow its logs until it is ready:

```powershell
docker compose logs -f oracle-xe
```

Press `Ctrl+C` to stop following logs.

## DBeaver connections

Create separate Oracle and Trino connections. Run Oracle SQL only in the
Oracle editor and Trino SQL only in the Trino editor.

### Oracle connection

| Setting | Value |
|---|---|
| Driver | Oracle |
| Host / port | `localhost` / `1521` |
| Service name | `XEPDB1` |
| User | `ORACLE_APP_USER` from `.env` |
| Password | `ORACLE_APP_PASSWORD` from `.env` |

### Trino connection

| Setting | Value |
|---|---|
| Driver | Trino |
| Host / port | `localhost` / `8080` |
| User | `trino` |
| Catalog / schema | `iceberg` / `bronze` |
| Password | blank |

```text
jdbc:trino://localhost:8080/iceberg/bronze
```

## Local source fixture

This section is only for the sandbox; do not run it against a production Oracle
database.

1. In DBeaver's Oracle editor, run
   [create_account_table.sql](init-scripts/create_account_table.sql) with
   **Execute SQL Script**.
2. Optionally run
   [seed_account_xml_bulk.sql](init-scripts/seed_account_xml_bulk.sql) with
   **Execute SQL Script** to add 10,000 deterministic XML records.

The bulk seed skips its previously generated identifiers. Change
`rows_to_insert` in that script to change the local fixture size.

## Build and query the Iceberg model

The ingest/bootstrap/incremental/wide pipeline, how it's generated, how to run
it, and example queries all live in
[trino_parsing/README.md](trino_parsing/README.md) — that document is the
source of truth for the actual data pipeline; this file only covers getting
the local Docker stack running.

## Operational boundaries

- Oracle is read-only for every mode in the pipeline.
- Ingestion is target-incremental: `ingest-refresh` stages a read from Oracle
  (optionally windowed by `c167`), then merges changed records into Iceberg.
- Oracle deletes are not inferred. Use a source tombstone or CDC feed before
  enabling delete propagation.
- See `trino_parsing/README.md` for the specifics of watermarking, change
  detection, and known risks against real production data.

## Troubleshooting

Check stack configuration and logs:

```powershell
docker compose config
docker compose ps
docker compose logs trino
docker compose logs iceberg-rest
docker compose logs minio
```

If Trino cannot see the model, run `trino_parsing/sql/account/01_ingest_bootstrap.sql`
and `03_bootstrap.sql` (see `trino_parsing/README.md`), then check:

```sql
SHOW SCHEMAS FROM iceberg;
SHOW TABLES FROM iceberg.bronze;
```

If the catalog services are running but Trino cannot connect, restart Trino:

```powershell
docker compose restart trino
```

For XML extraction failures, verify the Oracle account user can read `ACCOUNT`
and each XML document has a `<row>` root element. XML parsing happens in
Trino itself via `getClobVal()` + regex tokenizing, not Oracle `XMLTABLE` —
see `trino_parsing/README.md`.

## Stop or reset

Stop services but keep local Oracle and MinIO data:

```powershell
docker compose down
```

Remove containers and named volumes, including all local Oracle and Iceberg
data:

```powershell
docker compose down -v
```

## Repository layout

```text
init-scripts/
  create_account_table.sql             Local Oracle XMLTYPE fixture
  seed_account_xml_bulk.sql            Optional local bulk fixture
trino_parsing/
  gen_sql.py                           Generates the Trino SQL below
  sql/account/                         Checked-in generated SQL, run in DBeaver
  README.md                            The actual pipeline: modes, workflow, queries
reference/
  account_xml_data_sample.xml          XML source sample
  account_oracle_schema.md             Source schema extract
  lookup_metadata.csv                  XML-to-business-field mappings
trino-catalog/
  oracle.properties                    Oracle connector configuration
  iceberg.properties                   Iceberg REST and MinIO configuration
docker-compose.yml                     Local service stack
```

## References

- [Trino Oracle connector documentation](https://trino.io/docs/current/connector/oracle.html)
- [Trino Iceberg connector documentation](https://trino.io/docs/current/connector/iceberg.html)
