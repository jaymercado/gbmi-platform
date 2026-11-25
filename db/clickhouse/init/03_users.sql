-- One user per role. The passwords are query parameters supplied by the clickhouse-init
-- service from the values that `make setup` generated; they never appear in this file.
-- ALTER USER keeps each password in step with .env on every start.

CREATE USER IF NOT EXISTS dbt IDENTIFIED WITH sha256_password BY {dbt_password:String};
ALTER USER dbt IDENTIFIED WITH sha256_password BY {dbt_password:String};
GRANT dbt_rw TO dbt;
SET DEFAULT ROLE dbt_rw TO dbt;

CREATE USER IF NOT EXISTS bi IDENTIFIED WITH sha256_password BY {bi_password:String};
ALTER USER bi IDENTIFIED WITH sha256_password BY {bi_password:String};
GRANT bi_ro TO bi;
SET DEFAULT ROLE bi_ro TO bi;

CREATE USER IF NOT EXISTS gbmi IDENTIFIED WITH sha256_password BY {gbmi_password:String};
ALTER USER gbmi IDENTIFIED WITH sha256_password BY {gbmi_password:String};
GRANT pipeline_rw TO gbmi;
SET DEFAULT ROLE pipeline_rw TO gbmi;
