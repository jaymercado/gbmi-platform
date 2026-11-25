-- Runs once, when the data directory is first initialised (blueprint 9.3). It creates one role
-- and one database for each tenant of this PostgreSQL instance:
--
--   airflow   Airflow metadata
--   gbmi      ledger and operational state (schema ops, from phase P1)
--   metabase  Metabase application data
--
-- This is a .sql file on purpose. On Docker Desktop for macOS a bind-mounted .sh file tests
-- as executable but cannot be executed, which makes the image's entrypoint fail. The
-- passwords are read from the container's environment with psql's \getenv; if one is
-- missing, the statement that uses it fails and initialisation stops.

\getenv airflow_password AIRFLOW_DB_PASSWORD
\getenv gbmi_password GBMI_DB_PASSWORD
\getenv metabase_password METABASE_DB_PASSWORD

CREATE ROLE airflow LOGIN PASSWORD :'airflow_password';
CREATE DATABASE airflow OWNER airflow;
REVOKE ALL ON DATABASE airflow FROM PUBLIC;

CREATE ROLE gbmi LOGIN PASSWORD :'gbmi_password';
CREATE DATABASE gbmi OWNER gbmi;
REVOKE ALL ON DATABASE gbmi FROM PUBLIC;

CREATE ROLE metabase LOGIN PASSWORD :'metabase_password';
CREATE DATABASE metabase OWNER metabase;
REVOKE ALL ON DATABASE metabase FROM PUBLIC;
