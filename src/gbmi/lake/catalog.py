"""One catalog factory for both lake backends (blueprint 5.1, principle 7).

The local backend is a PyIceberg SQL catalog on SQLite with a warehouse on the filesystem; the
AWS backend is the Glue Data Catalog with a warehouse on S3. Callers ask for a catalog and a
namespace and never branch on the environment.

On both backends the silver namespace has an explicit location, ``<warehouse>/silver.db``, so a
table always lands at ``<warehouse>/silver.db/<table>`` (blueprint 7.1) whatever default the
catalog implementation would otherwise choose.
"""

from __future__ import annotations

from pathlib import Path

from pyiceberg.catalog import Catalog
from pyiceberg.catalog.sql import SqlCatalog

from gbmi.config import LakeBackend, Settings, get_settings

CATALOG_NAME = "gbmi"
_SQLITE_PREFIX = "sqlite:///"
_GLUE_PREFIX = "glue://"


def load_lake_catalog(settings: Settings | None = None) -> Catalog:
    """Return the Iceberg catalog of the configured backend."""
    settings = settings or get_settings()
    if settings.lake_backend is LakeBackend.LOCAL:
        database = Path(settings.catalog_uri.removeprefix(_SQLITE_PREFIX))
        database.parent.mkdir(parents=True, exist_ok=True)
        return SqlCatalog(CATALOG_NAME, uri=settings.catalog_uri, warehouse=settings.warehouse_uri)

    # Imported here so that the local backend does not need boto3 at import time.
    from pyiceberg.catalog.glue import GlueCatalog

    region = settings.catalog_uri.removeprefix(_GLUE_PREFIX)
    # Credentials come from the environment (short-lived ones on a laptop, the instance role
    # on the host); nothing long-lived is configured here (blueprint 5.9).
    return GlueCatalog(
        CATALOG_NAME,
        **{"glue.region": region, "s3.region": region, "warehouse": settings.warehouse_uri},
    )


def silver_namespace_location(settings: Settings | None = None) -> str:
    """Where the silver namespace keeps its tables: ``<warehouse>/silver.db``."""
    settings = settings or get_settings()
    return f"{settings.warehouse_uri}/silver.db"


def ensure_silver_namespace(catalog: Catalog, settings: Settings | None = None) -> str:
    """Create the silver namespace with its location if it is missing; return its name.

    The catalog is asked first. On the AWS host Terraform has created the Glue database, and
    the role of the host may read the database and may not create one: a create request in
    every build would be refused (week 3 plan, finding F19 k).
    """
    settings = settings or get_settings()
    namespace = settings.silver_namespace
    if not catalog.namespace_exists(namespace):
        catalog.create_namespace_if_not_exists(
            namespace, {"location": silver_namespace_location(settings)}
        )
    return namespace
