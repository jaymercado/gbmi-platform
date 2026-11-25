-- Databases of the warehouse (blueprint 7.1). Applied by the clickhouse-init service on every
-- start, so every statement is idempotent.
CREATE DATABASE IF NOT EXISTS lake COMMENT 'Pinned Iceberg tables: one per silver table, bound to an exact metadata file';
CREATE DATABASE IF NOT EXISTS staging COMMENT 'dbt staging models';
CREATE DATABASE IF NOT EXISTS intermediate COMMENT 'dbt intermediate models';
CREATE DATABASE IF NOT EXISTS marts COMMENT 'dbt marts: dimensions, facts and aggregates served to Metabase';
CREATE DATABASE IF NOT EXISTS ops COMMENT 'Operational marts';
CREATE DATABASE IF NOT EXISTS seeds COMMENT 'dbt seeds';
