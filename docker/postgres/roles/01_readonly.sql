-- The read-only role gbmi_ro (week 2 plan 3.3 and WP-08). The service postgres-roles applies the
-- file as the superuser to the database gbmi at every start, after the migrations, so every
-- statement is idempotent and a change reaches an existing volume without `make reset`.
--
-- The role can log in and read the schema ops, and nothing else: ClickHouse reads the ledger
-- with the role, through the named collection pg_ops, so no dbt model carries a password.
-- No migration file names the role: a migration which grants to a role would fail on every
-- PostgreSQL without the role, for example in the workflow ci.

\getenv ro_password GBMI_RO_DB_PASSWORD

SELECT 'CREATE ROLE gbmi_ro LOGIN'
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'gbmi_ro') \gexec
-- The password follows .env on every start.
ALTER ROLE gbmi_ro LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD :'ro_password';

-- The database gbmi grants CONNECT to its owner only (docker/postgres/init).
GRANT CONNECT ON DATABASE gbmi TO gbmi_ro;
GRANT USAGE ON SCHEMA ops TO gbmi_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA ops TO gbmi_ro;
-- Tables which a later migration creates as the role gbmi are readable too.
ALTER DEFAULT PRIVILEGES FOR ROLE gbmi IN SCHEMA ops GRANT SELECT ON TABLES TO gbmi_ro;
