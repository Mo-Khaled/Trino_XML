#!/usr/bin/env bash
# Auto-run ONCE by gvenzl/oracle-xe on a fresh database (this file is mounted
# into /container-entrypoint-initdb.d by docker-compose.yml).
#
# Everything it needs comes from the container environment, which
# docker-compose.yml feeds from the repo-root .env -- nothing is hard-coded:
#
#   ORACLE_SCHEMA           schema/user that OWNS the source tables (account, ...)
#   ORACLE_SCHEMA_PASSWORD  password for that user
#   ORACLE_APP_USER         identity Trino/Spark connect as; created here too
#                           unless it is blank / SYS / SYSTEM
#   ORACLE_APP_PASSWORD     password for that user
#
# What it does:
#   1. create ORACLE_SCHEMA (owning user) if missing
#   2. optionally create ORACLE_APP_USER (read-only consumer)
#   3. run the fixture SQL *as ORACLE_SCHEMA* so every object lands in it
#   4. GRANT SELECT on the fixture tables to PUBLIC so any user can read them
#
# The raw .sql fixtures are mounted read-only at /opt/fixtures and are NOT in
# the init dir, so gvenzl does not run them itself (which would create the
# tables under SYS).
set -euo pipefail

SCHEMA="${ORACLE_SCHEMA:-source_table}"
SCHEMA_PWD="${ORACLE_SCHEMA_PASSWORD:-source_table}"
APP_USER="${ORACLE_APP_USER:-}"
APP_PWD="${ORACLE_APP_PASSWORD:-}"
PDB="XEPDB1"                       # fixed PDB name for Oracle XE 21c
FIXTURES="/opt/fixtures"

echo "[00_setup] owning schema = ${SCHEMA} ; app user = ${APP_USER:-<none>}"

# ── 1 & 2: users, as SYSDBA ────────────────────────────────────────────────────
sqlplus -s -L / as sysdba <<SQL
WHENEVER SQLERROR EXIT SQL.SQLCODE
ALTER SESSION SET CONTAINER = ${PDB};

DECLARE
  n INT;
BEGIN
  SELECT COUNT(*) INTO n FROM dba_users WHERE username = UPPER('${SCHEMA}');
  IF n = 0 THEN
    EXECUTE IMMEDIATE 'CREATE USER ${SCHEMA} IDENTIFIED BY "${SCHEMA_PWD}"';
  END IF;
  EXECUTE IMMEDIATE 'GRANT CONNECT, RESOURCE TO ${SCHEMA}';
  EXECUTE IMMEDIATE 'ALTER USER ${SCHEMA} QUOTA UNLIMITED ON USERS';
END;
/

DECLARE
  u VARCHAR2(128) := UPPER('${APP_USER}');
  n INT;
BEGIN
  IF u IS NOT NULL AND u NOT IN ('SYS', 'SYSTEM') THEN
    SELECT COUNT(*) INTO n FROM dba_users WHERE username = u;
    IF n = 0 THEN
      EXECUTE IMMEDIATE 'CREATE USER ${APP_USER} IDENTIFIED BY "${APP_PWD}"';
    END IF;
    EXECUTE IMMEDIATE 'GRANT CONNECT TO ${APP_USER}';
  END IF;
END;
/
SQL

# ── 3: fixture, as the owning schema so unqualified names land in it ──────────
echo "[00_setup] loading fixture as ${SCHEMA}"
sqlplus -s -L "${SCHEMA}/${SCHEMA_PWD}@localhost:1521/${PDB}" <<SQL
WHENEVER SQLERROR EXIT SQL.SQLCODE
SET DEFINE OFF
@${FIXTURES}/create_account_table.sql
@${FIXTURES}/seed_account_xml_bulk.sql
SQL

# ── 4: let everyone read the source tables ───────────────────────────────────
echo "[00_setup] GRANT SELECT ON ${SCHEMA}.* TO PUBLIC"
sqlplus -s -L / as sysdba <<SQL
WHENEVER SQLERROR EXIT SQL.SQLCODE
ALTER SESSION SET CONTAINER = ${PDB};
BEGIN
  FOR t IN (SELECT table_name FROM dba_tables WHERE owner = UPPER('${SCHEMA}')) LOOP
    EXECUTE IMMEDIATE 'GRANT SELECT ON ${SCHEMA}.' || t.table_name || ' TO PUBLIC';
  END LOOP;
END;
/
SQL

echo "[00_setup] done"
