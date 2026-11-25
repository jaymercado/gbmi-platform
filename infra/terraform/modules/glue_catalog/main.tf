# The Glue database of the silver tables (blueprint 7.1, 9.2).
#
# The database has an explicit location. PyIceberg's Glue catalog puts a table at
# <database location>/<table> when one is set, which gives the layout warehouse/silver.db/<table>
# on S3, the same as on the local backend (week 1 plan, finding F6).

resource "aws_glue_catalog_database" "silver" {
  name         = var.database_name
  description  = "Silver Iceberg tables of the GBMI platform: source-shaped, typed, deduplicated."
  location_uri = "${var.warehouse_uri}/silver.db"
}
