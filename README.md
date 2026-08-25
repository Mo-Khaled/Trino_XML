# Oracle `XMLTYPE` with Trino — DBeaver Test Guide

This sandbox demonstrates how Trino's Oracle connector handles the
`XMLTYPE` column on the Temenos T24-style `bronze_user.account` table.

The important result is that Oracle `XMLTYPE` is unsupported by the Trino
Oracle connector. With the supported default (`IGNORE`), Trino omits the
column. If `CONVERT_TO_VARCHAR` is enabled, Trino tries to read the value
through JDBC and can fail with `oracle/xdb/XMLType` because the Oracle XML
dependency is not installed in the Trino Oracle plugin.

Use DBeaver for every SQL step below. Docker is only required to start and
restart the sandbox.

> Before publishing this repository, review the supplied XML and metadata
> fixtures to confirm they are safe for the intended audience.

## Reference data

The [`reference/`](reference/) directory contains the source material for the
test fixture:

- [`account_xml_data_sample.xml`](reference/account_xml_data_sample.xml) — source XML document.
- [`account_oracle_schema.md`](reference/account_oracle_schema.md) — ACCOUNT schema extract.
- [`lookup_metadata.csv`](reference/lookup_metadata.csv) — T24 field lookup metadata.

## What the sandbox contains

| Service | Host port | Purpose |
|---|---:|---|
| Oracle XE 21 | `1521` | Source database, PDB service `XEPDB1` |
| Trino | `8080` | Query coordinator and Oracle catalog `oracle` |
| MinIO | `9000` / `9001` | Local Iceberg object storage / console |
| Iceberg REST | `8181` | Iceberg metadata catalog |

The seeded `ACCOUNT` table has these columns:

| Column | Oracle type |
|---|---|
| `recid` | `VARCHAR2(255)` primary key |
| `xmlrecord` | `XMLTYPE` |
| `currency` | `VARCHAR2(250)` |

## 1. Start the services

From the repository root, run:

```powershell
Copy-Item .env.example .env
# Edit .env and replace the placeholder passwords.
docker compose up -d
docker compose ps
docker compose logs -f oracle-xe
```

Wait for `DATABASE IS READY TO USE!`, then press `Ctrl+C` to stop following
the logs. Trino is available once the `trino` container is running.

## 2. Create DBeaver connections

Create two connections.

### Oracle XE connection

- Driver: **Oracle**
- Host: `localhost`
- Port: `1521`
- Service name: `XEPDB1`
- User: value of `ORACLE_APP_USER` in `.env`
- Password: value of `ORACLE_APP_PASSWORD` in `.env`

Test the connection, then open an SQL Editor for it.

### Trino connection

- Driver: **Trino**
- Host: `localhost`
- Port: `8080`
- Catalog: `oracle`
- Schema: `bronze_user` (optional)
- User: `trino`
- Password: leave blank

The equivalent JDBC URL is:

```text
jdbc:trino://localhost:8080/oracle
```

Test the connection, then open a separate SQL Editor for it. Run Oracle SQL
only in the Oracle editor and Trino SQL only in the Trino editor.

## 3. Seed the Oracle table

In DBeaver's **Oracle** connection, open
[`init-scripts/create_account_table.sql`](init-scripts/create_account_table.sql)
and execute it as an SQL script. It drops and recreates `ACCOUNT`, then
inserts one source row containing the XML sample.

Verify the Oracle data in the Oracle editor:

```sql
SELECT a.recid, a.currency, a.xmlrecord.getClobVal()
FROM account a
WHERE ROWNUM <= 1;
```

The result must include `EGP` and XML content. This verifies that the source
column is a real Oracle `XMLTYPE` before Trino queries it.

### Optional: add 10,000 more `XMLTYPE` rows

To produce a larger XML-focused test set, execute
[`init-scripts/seed_account_xml_bulk.sql`](init-scripts/seed_account_xml_bulk.sql)
in the same DBeaver Oracle editor. It clones the original XML payload into
10,000 new `XMLTYPE` values and preserves every XML element and attribute.
It creates deterministic test values for account IDs, customer IDs, titles,
currencies, balances, dates, and users; unmapped T24 fields retain the sample
value. The root XML `<row id>` is set to the generated `RECID`. Existing
generated IDs are skipped if the script is run again. Change `rows_to_insert`
in the `constants` CTE at the top of the script to adjust the volume.

The script is DBeaver-ready: execute it as an SQL script and do not add a
standalone `/` delimiter. That delimiter is for SQL*Plus and causes Oracle
error `ORA-00900` when DBeaver sends it as SQL.

## 4. Materialize the Iceberg reporting model

Set secure `MINIO_ROOT_USER` and `MINIO_ROOT_PASSWORD` values in `.env`, then
start the full stack:

```powershell
docker compose up -d
docker compose ps
```

In DBeaver's **Trino** editor, execute
[`init-scripts/ingest_account_xml_to_iceberg.sql`](init-scripts/ingest_account_xml_to_iceberg.sql).
The script reads Oracle only through `oracle.system.query`; it never creates or
alters Oracle objects. It recreates the following Iceberg objects in the
`iceberg.bronze` schema:

| Object | Purpose |
|---|---|
| `account_xml_attributes` | One row for every XML element, retaining `field_index`, optional `m` index, and value. |
| `account_field_lookup` | The 289 supplied mappings from XML index/index pair to field name. |
| `account_xml_attributes_enriched` | View joining attributes to their specific lookup field names. |
| `account_flat` | Small fixed set of common account fields. |
| `account_wide` | One row per account with observed scalar fields as named columns and repeated XML fields as ordered arrays. |

`account_wide` is the reporting table. Its repeated fields are arrays, so a
field such as `c20` is represented by `c20_values` rather than many mostly-null
columns. The primary XML value is first, followed by values ordered by `m`.

```sql
SELECT recid, account_title_1, c20_values, cap_date_charge_values
FROM iceberg.bronze.account_wide
LIMIT 5;
```

To refresh the Iceberg data after reseeding Oracle, run the ingestion script
again. It drops and recreates only Iceberg tables and views.

## 5. Test the supported behavior: `IGNORE`

Open [`trino-catalog/oracle.properties`](trino-catalog/oracle.properties) and
comment out this line if it is enabled:

```properties
# unsupported-type-handling=CONVERT_TO_VARCHAR
```

Trino reads catalog configuration at startup, so restart it after the change:

```powershell
docker compose restart trino
```

In DBeaver's **Trino** editor, run:

```sql
DESCRIBE oracle.bronze_user.account;

SELECT *
FROM oracle.bronze_user.account
LIMIT 5;
```

Expected result:

- `DESCRIBE` lists only `recid` and `currency`.
- `xmlrecord` is omitted.
- `SELECT *` returns the two non-XML columns.

`IGNORE` is the supported configuration for excluding an unsupported column.

## 6. Reproduce the `CONVERT_TO_VARCHAR` failure

In `trino-catalog/oracle.properties`, enable:

```properties
unsupported-type-handling=CONVERT_TO_VARCHAR
```

Restart Trino:

```powershell
docker compose restart trino
```

Reconnect the DBeaver Trino connection if its metadata is stale, then run:

```sql
DESCRIBE oracle.bronze_user.account;

SELECT *
FROM oracle.bronze_user.account
LIMIT 5;
```

Expected result:

- `DESCRIBE` exposes `xmlrecord` as `varchar`.
- `SELECT *` fails with an error containing `oracle/xdb/XMLType`.

This is a Trino coordinator/JDBC class-loading failure, not a DBeaver SQL
syntax error. The query ID included in the error identifies that single failed
attempt; a retry receives a new query ID.

Confirm that Trino can still read only supported columns:

```sql
SELECT recid, currency
FROM oracle.bronze_user.account
LIMIT 5;
```

This succeeds because it does not read `xmlrecord`.

When finished, comment out `CONVERT_TO_VARCHAR` and restart Trino to return
to the supported `IGNORE` behavior.

## 7. Read XML by converting it inside Oracle

In the DBeaver **Trino** editor, use the Oracle connector passthrough table
function. Oracle converts the `XMLTYPE` to `CLOB`; Trino receives that value
as `VARCHAR`.

```sql
SELECT *
FROM TABLE(
  oracle.system.query(
    query => 'SELECT a.recid, a.currency,
                      XMLSERIALIZE(DOCUMENT a.xmlrecord AS CLOB) AS xml_text
               FROM account a
               WHERE ROWNUM <= 5'
  )
);
```

Expected result: `recid`, `currency`, and `xml_text` containing the XML.

The following Oracle expression is also valid if it matches your existing SQL:

```sql
a.xmlrecord.getClobVal() AS xml_text
```

## 8. Recommended reusable interface: an Oracle view

For ongoing use, create a view in the **Oracle** editor instead of embedding
native SQL in every Trino query:

```sql
CREATE OR REPLACE VIEW account_trino AS
SELECT
  a.recid,
  a.currency,
  XMLSERIALIZE(DOCUMENT a.xmlrecord AS CLOB) AS xml_text
FROM account a;
```

Then query it in the **Trino** editor:

```sql
SELECT recid, currency, length(xml_text) AS xml_length
FROM oracle.bronze_user.account_trino
LIMIT 5;
```

If Trino does not see the new view, run this in the Trino editor and retry:

```sql
CALL oracle.system.flush_metadata_cache();
```

## 9. Cleanup

Stop the containers but retain the seeded Oracle data:

```powershell
docker compose down
```

Delete the containers and named Oracle data volume:

```powershell
docker compose down -v
```

## References

- [Trino Oracle connector documentation](https://trino.io/docs/current/connector/oracle.html)
