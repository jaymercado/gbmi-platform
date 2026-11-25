-- Roles by duty (blueprint 5.8).
--
--   dbt_rw       builds models: reads lake, owns the contents of staging, intermediate, marts,
--                ops and seeds
--   bi_ro        reads marts and ops only, within a quota
--   pipeline_rw  the gbmi tasks: re-creates the pinned lake tables and reads every layer for
--                its count assertions
--
-- ClickHouse reads the lake through whichever table the pipeline pinned; dbt and Metabase never
-- name a file path themselves.

CREATE ROLE IF NOT EXISTS dbt_rw;
GRANT SELECT ON lake.* TO dbt_rw;
GRANT SELECT, INSERT, ALTER, CREATE TABLE, CREATE VIEW, CREATE DICTIONARY, DROP TABLE, DROP VIEW,
      DROP DICTIONARY, TRUNCATE, OPTIMIZE, SHOW ON staging.* TO dbt_rw;
GRANT SELECT, INSERT, ALTER, CREATE TABLE, CREATE VIEW, CREATE DICTIONARY, DROP TABLE, DROP VIEW,
      DROP DICTIONARY, TRUNCATE, OPTIMIZE, SHOW ON intermediate.* TO dbt_rw;
GRANT SELECT, INSERT, ALTER, CREATE TABLE, CREATE VIEW, CREATE DICTIONARY, DROP TABLE, DROP VIEW,
      DROP DICTIONARY, TRUNCATE, OPTIMIZE, SHOW ON marts.* TO dbt_rw;
GRANT SELECT, INSERT, ALTER, CREATE TABLE, CREATE VIEW, CREATE DICTIONARY, DROP TABLE, DROP VIEW,
      DROP DICTIONARY, TRUNCATE, OPTIMIZE, SHOW ON ops.* TO dbt_rw;
GRANT SELECT, INSERT, ALTER, CREATE TABLE, CREATE VIEW, CREATE DICTIONARY, DROP TABLE, DROP VIEW,
      DROP DICTIONARY, TRUNCATE, OPTIMIZE, SHOW ON seeds.* TO dbt_rw;
-- dbt issues CREATE DATABASE IF NOT EXISTS for each schema it builds into.
GRANT CREATE DATABASE ON staging.* TO dbt_rw;
GRANT CREATE DATABASE ON intermediate.* TO dbt_rw;
GRANT CREATE DATABASE ON marts.* TO dbt_rw;
GRANT CREATE DATABASE ON ops.* TO dbt_rw;
GRANT CREATE DATABASE ON seeds.* TO dbt_rw;

CREATE ROLE IF NOT EXISTS bi_ro;
GRANT SELECT, SHOW ON marts.* TO bi_ro;
GRANT SELECT, SHOW ON ops.* TO bi_ro;

CREATE ROLE IF NOT EXISTS pipeline_rw;
-- INSERT is required to create a table with an Iceberg engine, because the engine can write.
-- The pipeline never inserts through ClickHouse, and the lake mount is read-only for it.
GRANT SELECT, INSERT, CREATE TABLE, DROP TABLE, SHOW ON lake.* TO pipeline_rw;
-- ClickHouse 26.8 requires a grant per table engine that reaches outside the server. Only
-- the pipeline may create Iceberg tables, and only in the lake database (spike SK2, Q4).
GRANT TABLE ENGINE ON IcebergLocal TO pipeline_rw;
GRANT TABLE ENGINE ON IcebergS3 TO pipeline_rw;
GRANT SELECT, SHOW ON staging.* TO pipeline_rw;
GRANT SELECT, SHOW ON intermediate.* TO pipeline_rw;
GRANT SELECT, SHOW ON marts.* TO pipeline_rw;
GRANT SELECT, SHOW ON ops.* TO pipeline_rw;

-- Dashboards get a bounded share of the server. No read-only profile is set: the role holds
-- SELECT grants only, and Metabase's ClickHouse driver needs to set a session setting.
CREATE SETTINGS PROFILE IF NOT EXISTS bi_profile
    SETTINGS max_execution_time = 30, max_memory_usage = 1000000000, max_result_rows = 1000000
    TO bi_ro;
CREATE QUOTA IF NOT EXISTS bi_quota
    FOR INTERVAL 1 hour MAX queries = 5000, execution_time = 1800
    TO bi_ro;
