"""The pin statement names exactly one metadata file, on either backend."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import boto3
import pytest
from botocore.stub import Stubber

from gbmi.config import Environment, LakeAccess, LakeBackend, Settings
from gbmi.warehouse.pin import (
    ROLE_SESSION_NAME,
    LakeGrant,
    PinError,
    S3Credentials,
    assume_lake_role,
    engine_clause,
    hide_keys,
    lake_grant,
    pin_statement,
    pin_table,
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


# --- the lake on S3: a role or keys (week 3 plan 3.3, finding F13 and difference D42) ---------

ROLE = "arn:aws:iam::000000000000:role/gbmi-clickhouse-lake-ro"
S3_METADATA = f"{S3_TABLE}/metadata/00007-1c9d.metadata.json"
KEYS = S3Credentials("ASIAEXAMPLEKEY", "secret-value-of-the-key", "token-value-of-the-session")


def host(access: LakeAccess = LakeAccess.ROLE, role: str | None = ROLE) -> Settings:
    return Settings(
        env=Environment.PROD,
        lake_backend=LakeBackend.AWS,
        lake_uri="s3://gbmi-prod-lake-000000000000",
        catalog_uri="glue://us-east-1",
        ch_lake_access=access,
        ch_lake_role_arn=role,
    )


def test_the_statement_of_the_host_names_the_role_which_clickhouse_assumes() -> None:
    statement = pin_statement("sec_xbrl_fact", S3_TABLE, S3_METADATA, role_arn=ROLE)

    # No key stands in the statement, and no NOSIGN: with NOSIGN ClickHouse skips the role.
    assert statement == (
        "CREATE OR REPLACE TABLE `lake`.`sec_xbrl_fact` "
        "ENGINE = IcebergS3('s3://gbmi-prod-lake-000000000000/warehouse/silver.db/gdelt_event/', "
        "extra_credentials(role_arn = 'arn:aws:iam::000000000000:role/gbmi-clickhouse-lake-ro', "
        "role_session_name = 'gbmi-clickhouse-lake')) "
        "SETTINGS iceberg_metadata_file_path = 'metadata/00007-1c9d.metadata.json'"
    )


def test_the_fallback_statement_carries_the_three_values_of_the_assumed_role() -> None:
    statement = pin_statement("sec_xbrl_fact", S3_TABLE, S3_METADATA, credentials=KEYS)

    assert statement == (
        "CREATE OR REPLACE TABLE `lake`.`sec_xbrl_fact` "
        "ENGINE = IcebergS3('s3://gbmi-prod-lake-000000000000/warehouse/silver.db/gdelt_event/', "
        "'ASIAEXAMPLEKEY', 'secret-value-of-the-key', 'token-value-of-the-session') "
        "SETTINGS iceberg_metadata_file_path = 'metadata/00007-1c9d.metadata.json'"
    )


def test_a_statement_takes_keys_or_a_role_and_never_both() -> None:
    with pytest.raises(ValueError, match="keys or a role, not both"):
        engine_clause(S3_TABLE, KEYS, role_arn=ROLE)
    # A table of the file system ignores both: the laptop has no role.
    assert engine_clause(LOCAL_TABLE, role_arn=ROLE) == (
        "IcebergLocal('/lake/warehouse/silver.db/gdelt_event/')"
    )


def test_the_local_backend_needs_no_grant_and_the_host_names_the_role() -> None:
    laptop = Settings()

    assert lake_grant(laptop) == LakeGrant()
    assert lake_grant(host()) == LakeGrant(role_arn=ROLE)
    assert host().ch_lake_access is LakeAccess.ROLE


def test_the_aws_backend_without_the_role_is_refused() -> None:
    with pytest.raises(RuntimeError, match="GBMI_CH_LAKE_ROLE_ARN is not set"):
        lake_grant(host(role=None))
    # Compose passes an unset variable as an empty text.
    with pytest.raises(RuntimeError, match="GBMI_CH_LAKE_ROLE_ARN is not set"):
        lake_grant(host(role="  "))


def test_the_fallback_assumes_the_role_for_one_hour() -> None:
    client = boto3.client(
        "sts", region_name="us-east-1", aws_access_key_id="test", aws_secret_access_key="test"
    )
    answer = {
        "Credentials": {
            "AccessKeyId": "ASIAEXAMPLEKEY12345",
            "SecretAccessKey": "secret-value-of-the-key",
            "SessionToken": "token-value-of-the-session",
            "Expiration": "2026-11-28T12:00:00Z",
        }
    }
    request = {"RoleArn": ROLE, "RoleSessionName": ROLE_SESSION_NAME, "DurationSeconds": 3600}
    with Stubber(client) as stub:
        stub.add_response("assume_role", answer, request)
        stub.add_response("assume_role", answer, request)

        direct = assume_lake_role(ROLE, client=client)
        grant = lake_grant(host(LakeAccess.KEYS), sts_client=client)

        stub.assert_no_pending_responses()
    expected = S3Credentials(
        "ASIAEXAMPLEKEY12345", "secret-value-of-the-key", "token-value-of-the-session"
    )
    assert direct == expected
    assert grant == LakeGrant(credentials=expected)


class Refusing:
    """A ClickHouse client which refuses every statement and quotes the statement."""

    def command(self, statement: str) -> Any:
        raise RuntimeError(f"Code: 499. S3 said no. In scope {statement}")


def iceberg_table() -> Any:
    return SimpleNamespace(
        name=lambda: ("gbmi_prod_silver", "sec_xbrl_fact"),
        location=lambda: S3_TABLE,
        metadata_location=S3_METADATA,
        current_snapshot=lambda: None,
    )


def test_an_error_of_a_statement_with_keys_names_no_key() -> None:
    # A run record stores the text of an error, and a log line shows it.
    with pytest.raises(PinError) as caught:
        pin_table(Refusing(), iceberg_table(), credentials=KEYS)  # type: ignore[arg-type]

    text = str(caught.value)
    assert "S3 said no" in text
    for value in ("ASIAEXAMPLEKEY", "secret-value-of-the-key", "token-value-of-the-session"):
        assert value not in text
    assert text.count("[hidden]") == 3
    # The error has no cause attached: a traceback would show the statement again.
    assert caught.value.__cause__ is None and caught.value.__suppress_context__ is True


def test_an_error_of_a_statement_with_a_role_stays_as_clickhouse_sent_it() -> None:
    with pytest.raises(RuntimeError, match="S3 said no") as caught:
        pin_table(Refusing(), iceberg_table(), role_arn=ROLE)  # type: ignore[arg-type]

    # The address of a role is no secret.
    assert ROLE in str(caught.value)


def test_hide_keys_replaces_each_value() -> None:
    text = "key ASIAEXAMPLEKEY secret secret-value-of-the-key token token-value-of-the-session"

    assert hide_keys(text, KEYS) == "key [hidden] secret [hidden] token [hidden]"
    assert hide_keys("nothing to hide", S3Credentials("a-key", "a-secret")) == "nothing to hide"
