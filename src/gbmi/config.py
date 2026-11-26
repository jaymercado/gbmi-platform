"""Typed settings read from ``GBMI_*`` environment variables (blueprint 9.5).

Application code never branches on the environment name. The lake backend, its
location and the catalog are plain configuration (blueprint 5.1, principle 7), so the
same code runs against the local filesystem and against S3 with Glue.
"""

from __future__ import annotations

from enum import StrEnum
from functools import lru_cache
from typing import Self

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from gbmi import __version__


class Environment(StrEnum):
    """Deployment environment (blueprint 9.5)."""

    LOCAL = "local"
    CI = "ci"
    PROD = "prod"


class LakeBackend(StrEnum):
    """Where the lake lives and which catalog tracks its tables."""

    LOCAL = "local"  # filesystem with a SQLite catalog
    AWS = "aws"  # S3 with the Glue Data Catalog


_LAKE_SCHEME = {LakeBackend.LOCAL: "file://", LakeBackend.AWS: "s3://"}
_CATALOG_SCHEME = {LakeBackend.LOCAL: "sqlite:///", LakeBackend.AWS: "glue://"}


class Settings(BaseSettings):
    """All runtime configuration of the ``gbmi`` package."""

    model_config = SettingsConfigDict(env_prefix="GBMI_", extra="ignore", frozen=True)

    env: Environment = Environment.LOCAL
    lake_backend: LakeBackend = LakeBackend.LOCAL
    #: Root of the lake: ``raw/``, ``quarantine/`` and ``warehouse/`` live below it (blueprint 7.1).
    lake_uri: str = "file:///lake"
    #: ``sqlite:///<path>`` for the local backend, ``glue://<region>`` for AWS.
    catalog_uri: str = "sqlite:////lake/catalog/catalog.db"
    pg_dsn: SecretStr | None = None
    #: The SQL files which ``gbmi db migrate`` applies to the schema ``ops`` (week 2 plan 3.3).
    #: The default fits a checkout; the images set an absolute path.
    migrations_dir: str = "db/postgres/migrations"
    ch_url: SecretStr | None = None
    #: Recorded GDELT files for the source ``fixture`` (make demo and CI).
    fixtures_dir: str = "/fixtures/gdelt/v2"
    #: One ingest run downloads at most this many files (blueprint 8.1: bounded batches).
    ingest_batch_limit: int = Field(default=200, ge=1)
    #: Parallel downloads of one ingest run (blueprint 8.3: 4 to 8).
    ingest_concurrency: int = Field(default=4, ge=1, le=8)
    sec_user_agent: str | None = None
    watchlist_size: int = Field(default=50, ge=1, le=10_000)
    alert_webhook: SecretStr | None = None
    #: Written to the ``_pipeline_version`` provenance column (blueprint 7.12). The image sets
    #: it to the Git commit; outside an image it falls back to the package version.
    pipeline_version: str = __version__

    @model_validator(mode="after")
    def _backend_matches_uris(self) -> Self:
        lake_scheme = _LAKE_SCHEME[self.lake_backend]
        if not self.lake_uri.startswith(lake_scheme):
            msg = (
                f"GBMI_LAKE_URI must start with {lake_scheme!r} when "
                f"GBMI_LAKE_BACKEND={self.lake_backend.value}; got {self.lake_uri!r}"
            )
            raise ValueError(msg)
        catalog_scheme = _CATALOG_SCHEME[self.lake_backend]
        if not self.catalog_uri.startswith(catalog_scheme):
            msg = (
                f"GBMI_CATALOG_URI must start with {catalog_scheme!r} when "
                f"GBMI_LAKE_BACKEND={self.lake_backend.value}; got {self.catalog_uri!r}"
            )
            raise ValueError(msg)
        return self

    @property
    def lake_root(self) -> str:
        """The lake URI without a trailing slash."""
        return self.lake_uri.rstrip("/")

    @property
    def bronze_uri(self) -> str:
        """Immutable raw files, exactly as served (blueprint 7.1)."""
        return f"{self.lake_root}/raw"

    @property
    def quarantine_uri(self) -> str:
        """Files and rows rejected by gates G1 and G2."""
        return f"{self.lake_root}/quarantine"

    @property
    def warehouse_uri(self) -> str:
        """Root of the Iceberg tables."""
        return f"{self.lake_root}/warehouse"

    @property
    def silver_namespace(self) -> str:
        """``silver`` on the local catalog, ``gbmi_<env>_silver`` on Glue (blueprint 7.1)."""
        if self.lake_backend is LakeBackend.AWS:
            return f"gbmi_{self.env.value}_silver"
        return "silver"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide settings, read once from the environment."""
    return Settings()
