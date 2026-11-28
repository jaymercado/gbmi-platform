"""``gbmi lake check`` on a running stack: the checks of the smoke test of a deployment.

The local half runs with ``make integration``. The half on S3 and Glue runs with
``make integration-aws``: ClickHouse then reads the table with the keys of the caller, because
the lake role exists on the AWS host only.
"""

from __future__ import annotations

import uuid

import pytest
from clickhouse_connect.driver.client import Client

from gbmi.lake.check import check_catalog, check_object, check_pin
from gbmi.lake.tables import GDELT_EVENT
from tests.integration.conftest import SHARED_LAKE, Lake
from tests.integration.lake.test_lake_objects import stored

pytestmark = pytest.mark.integration


def test_the_catalog_and_the_object_store_answer(lake: Lake) -> None:
    scratch = f"quarantine/_check{uuid.uuid4().hex[:8]}"

    assert check_catalog(lake.settings)["namespace"] == lake.namespace
    assert check_object(f"{lake.settings.lake_uri}/{scratch}") == {"bytes": 52, "deleted": True}
    # The check leaves nothing in the lake: no object and, on S3, no marker object for the
    # scratch folder.
    assert stored(lake, scratch) == {}


def test_clickhouse_reads_a_table_which_the_check_pins_and_drops(
    local_lake: Lake, clickhouse: Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    if not local_lake.settings.lake_uri.startswith(f"file://{SHARED_LAKE}/"):
        pytest.skip("needs the lake mount of the Compose stack (run with make integration)")
    before = set(clickhouse.query("SHOW TABLES FROM lake").result_rows)

    result = check_pin(local_lake.settings.model_copy(update=_stack_url()), GDELT_EVENT)

    # The lake of the test is new: the check created the table empty and read no row.
    assert result == {"table": GDELT_EVENT, "rows": 0, "created": True, "access": "none"}
    # The table of the check is gone, and the pinned tables of the stack are untouched.
    assert set(clickhouse.query("SHOW TABLES FROM lake").result_rows) == before


def _stack_url() -> dict[str, object]:
    from gbmi.config import get_settings

    return {"ch_url": get_settings().ch_url}
