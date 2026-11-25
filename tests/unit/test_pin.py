"""The pin statement names exactly one metadata file, on either backend."""

from __future__ import annotations

import pytest

from gbmi.warehouse.pin import (
    S3Credentials,
    engine_clause,
    pin_statement,
    relative_metadata_file,
)

LOCAL_TABLE = "file:///lake/warehouse/silver.db/gdelt_event"
LOCAL_METADATA = f"{LOCAL_TABLE}/metadata/00003-8b2f.metadata.json"
S3_TABLE = "s3://gbmi-prod-lake-000000000000/warehouse/silver.db/gdelt_event"


def test_local_table_is_pinned_with_iceberg_local() -> None:
    statement = pin_statement("gdelt_event", LOCAL_TABLE, LOCAL_METADATA)

    assert statement == (
        "CREATE OR REPLACE TABLE `lake`.`gdelt_event` "
        "ENGINE = IcebergLocal('/lake/warehouse/silver.db/gdelt_event/') "
        "SETTINGS iceberg_metadata_file_path = 'metadata/00003-8b2f.metadata.json'"
    )


def test_s3_table_without_credentials_relies_on_the_server_role() -> None:
    assert engine_clause(S3_TABLE) == (
        "IcebergS3('s3://gbmi-prod-lake-000000000000/warehouse/silver.db/gdelt_event/')"
    )


def test_s3_table_with_short_lived_credentials() -> None:
    credentials = S3Credentials("AKIAEXAMPLE", "secret", "token")

    assert engine_clause(S3_TABLE + "/", credentials) == (
        "IcebergS3('s3://gbmi-prod-lake-000000000000/warehouse/silver.db/gdelt_event/', "
        "'AKIAEXAMPLE', 'secret', 'token')"
    )


def test_metadata_file_must_belong_to_the_table() -> None:
    assert (
        relative_metadata_file(LOCAL_TABLE + "/", LOCAL_METADATA)
        == "metadata/00003-8b2f.metadata.json"
    )
    with pytest.raises(ValueError, match="is not under table"):
        relative_metadata_file(
            LOCAL_TABLE, "file:///lake/warehouse/silver.db/other/metadata/1.metadata.json"
        )


def test_quotes_cannot_break_out_of_the_statement() -> None:
    statement = pin_statement(
        "odd`name", "file:///lake/it's", "file:///lake/it's/metadata/1.metadata.json"
    )

    assert "`odd``name`" in statement
    assert "IcebergLocal('/lake/it\\'s/')" in statement


def test_unsupported_location_is_rejected() -> None:
    with pytest.raises(ValueError, match="unsupported table location"):
        engine_clause("gs://bucket/table")
