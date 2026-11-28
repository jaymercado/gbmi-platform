"""One object of the lake is written whole: a move on the file system, one PUT request on S3."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import boto3
import pytest
from botocore.stub import Stubber

from gbmi.lake.objects import is_s3, s3_location, write_object

KEY = "raw/gdelt/v2/export/dt=2026-11-25/20261125080000.export.CSV.zip"


@pytest.fixture
def s3() -> Any:
    """An S3 client which sends no request: the stub answers and checks each call."""
    client = boto3.client(
        "s3", region_name="us-east-1", aws_access_key_id="test", aws_secret_access_key="test"
    )
    with Stubber(client) as stub:
        yield client, stub
        stub.assert_no_pending_responses()


def test_the_local_write_moves_a_finished_file_to_its_name(tmp_path: Path) -> None:
    seen: list[list[str]] = []

    def look() -> None:
        seen.append(sorted(path.name for path in tmp_path.rglob("*") if path.is_file()))

    write_object(f"file://{tmp_path}", KEY, b"zip bytes", before_visible=look)

    # At the last moment before the move only the temporary file exists.
    assert seen == [["20261125080000.export.CSV.zip.part"]]
    assert (tmp_path / KEY).read_bytes() == b"zip bytes"
    assert not list(tmp_path.rglob("*.part"))


def test_a_local_write_which_stops_before_the_move_leaves_no_file_under_the_name(
    tmp_path: Path,
) -> None:
    def crash() -> None:
        raise RuntimeError("the process stops here")

    with pytest.raises(RuntimeError, match="stops here"):
        write_object(f"file://{tmp_path}", KEY, b"zip bytes", before_visible=crash)

    assert not (tmp_path / KEY).exists()


def test_the_write_on_s3_is_one_put_request_for_the_final_key(s3: Any) -> None:
    client, stub = s3
    # The stub refuses every other call: no marker object, no temporary object, no copy, no
    # delete and no multipart upload.
    stub.add_response(
        "put_object",
        {},
        {"Bucket": "gbmi-prod-lake-000000000000", "Key": KEY, "Body": b"zip bytes"},
    )

    write_object("s3://gbmi-prod-lake-000000000000", KEY, b"zip bytes", client=client)


def test_a_lake_below_a_prefix_keeps_the_prefix_in_the_key(s3: Any) -> None:
    client, stub = s3
    stub.add_response(
        "put_object", {}, {"Bucket": "bucket", "Key": f"ci/run-1/{KEY}", "Body": b"1"}
    )

    write_object("s3://bucket/ci/run-1/", KEY, b"1", client=client)

    assert s3_location("s3://bucket/ci/run-1/", KEY) == ("bucket", f"ci/run-1/{KEY}")
    assert s3_location("s3://bucket", KEY) == ("bucket", KEY)


def test_the_last_moment_before_the_object_is_visible_comes_before_the_request(s3: Any) -> None:
    client, _ = s3

    def crash() -> None:
        raise RuntimeError("the process stops here")

    # No response is queued: a request would fail the test with another error.
    with pytest.raises(RuntimeError, match="stops here"):
        write_object("s3://bucket", KEY, b"1", before_visible=crash, client=client)


def test_only_an_s3_address_takes_the_s3_path() -> None:
    assert is_s3("s3://bucket/prefix")
    assert not is_s3("file:///lake")
