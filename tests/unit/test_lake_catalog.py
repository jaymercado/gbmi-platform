"""The silver namespace is asked for first and created only when it is missing."""

from __future__ import annotations

from typing import Any

from gbmi.config import Environment, LakeBackend, Settings
from gbmi.lake.catalog import ensure_silver_namespace, silver_namespace_location


class FakeCatalog:
    """Records the calls which a build sends to the catalog."""

    def __init__(self, existing: set[str]) -> None:
        self.existing = existing
        self.calls: list[tuple[Any, ...]] = []

    def namespace_exists(self, namespace: str) -> bool:
        self.calls.append(("namespace_exists", namespace))
        return namespace in self.existing

    def create_namespace_if_not_exists(self, namespace: str, properties: dict[str, str]) -> None:
        self.calls.append(("create_namespace_if_not_exists", namespace, properties))
        self.existing.add(namespace)


HOST = Settings(
    env=Environment.PROD,
    lake_backend=LakeBackend.AWS,
    lake_uri="s3://gbmi-prod-lake-000000000000",
    catalog_uri="glue://us-east-1",
)


def test_an_existing_namespace_is_never_created_again() -> None:
    # Week 3 plan, finding F19 k: on the host Terraform owns the Glue database, and the role
    # of the host may not create a database.
    catalog = FakeCatalog({"gbmi_prod_silver"})

    name = ensure_silver_namespace(catalog, HOST)  # type: ignore[arg-type]

    assert name == "gbmi_prod_silver"
    assert catalog.calls == [("namespace_exists", "gbmi_prod_silver")]


def test_a_missing_namespace_is_created_with_its_location() -> None:
    catalog = FakeCatalog(set())

    name = ensure_silver_namespace(catalog, HOST)  # type: ignore[arg-type]

    location = "s3://gbmi-prod-lake-000000000000/warehouse/silver.db"
    assert silver_namespace_location(HOST) == location
    assert catalog.calls == [
        ("namespace_exists", name),
        ("create_namespace_if_not_exists", name, {"location": location}),
    ]
