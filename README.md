# Oracle XML to Trino Iceberg

This repository is a local Docker reference for reading T24-style Oracle
`XMLTYPE` account records and publishing an Iceberg reporting model in Trino.

Oracle is read-only during ingestion. Trino extracts XML with Oracle native
SQL, writes Parquet-backed Iceberg tables to MinIO, and exposes business fields
as columns in `iceberg.bronze.account_wide`.

## Architecture

```text
Oracle ACCOUNT (RECID, CURRENCY, XMLTYPE)
        |
        | read-only XMLTABLE extraction through oracle.system.query
        v
Trino -> Iceberg REST catalog + MinIO
        |
        +-- account_xml_attributes
        +-- account_field_lookup
        +-- account_xml_attributes_enriched
        +-- account_flat
        +-- account_wide
```

`account_wide` is a physical Iceberg table with one row per account. Repeated
XML fields are ordered arrays, avoiding mostly-null repeated columns.

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

## Bootstrap the Iceberg model

In the Trino editor, execute
[ingest_account_xml_to_iceberg.sql](init-scripts/ingest_account_xml_to_iceberg.sql)
once. This full bootstrap recreates the Iceberg reporting objects; it does not
create, alter, or delete Oracle objects.

| Object | Type | Purpose |
|---|---|---|
| `account_xml_attributes` | Iceberg table | One row per XML element with its index, optional `m` value, and value. |
| `account_field_lookup` | Iceberg table | XML element/index to business-field mapping. |
| `account_xml_attributes_enriched` | View | Attribute rows joined to their matching business name. |
| `account_flat` | Iceberg table | Small fixed set of common account fields. |
| `account_wide` | Iceberg table | Reporting table with named scalar columns and repeated-value arrays. |

The bootstrap fixes the known Arabic fixture encoding only in Iceberg. Oracle
source data is not changed.

## Run the incremental load

After bootstrap, execute
[ingest_account_xml_incremental.sql](init-scripts/ingest_account_xml_incremental.sql)
for normal loads.

The script reads XML field `c167` as the source-updated date in `YYYYMMDD`
format. It uses the maximum loaded date with a one-day overlap, merges changed
attributes into `account_xml_attributes`, and rebuilds only changed rows in
`account_flat` and `account_wide`.

The first incremental run adds `source_updated_date` to the existing attribute
table and populates it. Later runs are idempotent and safely reprocess the
overlap window.

## Query the reporting model

Use `account_wide` for reporting:

```sql
SELECT
  recid,
  account_title_1,
  date_last_update,
  c20_values,
  cap_date_charge_values
FROM iceberg.bronze.account_wide
ORDER BY recid
LIMIT 10;
```

`c20_values` preserves XML order: the unindexed value comes first, followed by
values ordered by their `m` attribute. This keeps the Arabic title inside the
array instead of creating a sparse column.

Inspect individual mapped attributes:

```sql
SELECT
  recid,
  field_index,
  multi_value_index,
  field_name,
  field_value
FROM iceberg.bronze.account_xml_attributes_enriched
WHERE recid = '9000000112345001'
ORDER BY field_index, multi_value_index;
```

Check the loaded watermark:

```sql
SELECT max(source_updated_date) AS loaded_through
FROM iceberg.bronze.account_xml_attributes;
```

## Operational boundaries

- Oracle is read-only for both bootstrap and incremental ingestion.
- The incremental script is target-incremental: it stages a complete Oracle
  XML read, then filters and merges changed records in Iceberg.
- To reduce Oracle read volume, an external scheduler must inject the saved
  `c167` watermark into the read-only native Oracle query.
- Oracle deletes are not inferred. Use a source tombstone or CDC feed before
  enabling delete propagation.
- `c167` is date-granular; the one-day overlap is required for retries and
  same-day changes.
- Staging tables are Iceberg tables replaced on every incremental run and can
  be inspected for diagnostics.

## Troubleshooting

Check stack configuration and logs:

```powershell
docker compose config
docker compose ps
docker compose logs trino
docker compose logs iceberg-rest
docker compose logs minio
```

If Trino cannot see the model, run the bootstrap script, then check:

```sql
SHOW SCHEMAS FROM iceberg;
SHOW TABLES FROM iceberg.bronze;
```

If the catalog services are running but Trino cannot connect, restart Trino:

```powershell
docker compose restart trino
```

For XML extraction failures, verify the Oracle account user can read `ACCOUNT`
and each XML document has a `<row>` root element. The ingestion scripts use
`oracle.system.query` and Oracle `XMLTABLE`, not direct Trino XMLTYPE mapping.

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
  ingest_account_xml_to_iceberg.sql    One-time Iceberg bootstrap
  ingest_account_xml_incremental.sql   Normal target-incremental load
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
