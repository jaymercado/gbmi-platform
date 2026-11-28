"""make host-secrets: the parameters of the AWS host are created once and never printed."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

import boto3
import pytest
from botocore.stub import ANY, Stubber

REPO = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("host_secrets", REPO / "tools" / "host_secrets.py")
assert SPEC is not None and SPEC.loader is not None
host_secrets = importlib.util.module_from_spec(SPEC)
sys.modules["host_secrets"] = host_secrets
SPEC.loader.exec_module(host_secrets)

IDENTITY = "Sample Company Name contact@example.org"
GENERATED = (
    "POSTGRES_PASSWORD",
    "AIRFLOW_DB_PASSWORD",
    "GBMI_DB_PASSWORD",
    "GBMI_RO_DB_PASSWORD",
    "METABASE_DB_PASSWORD",
    "CLICKHOUSE_ADMIN_PASSWORD",
    "CH_DBT_PASSWORD",
    "CH_BI_PASSWORD",
    "CH_GBMI_PASSWORD",
    "AIRFLOW_ADMIN_PASSWORD",
    "AIRFLOW_FERNET_KEY",
    "AIRFLOW_JWT_SECRET",
    "METABASE_ADMIN_PASSWORD",
    "METABASE_ENCRYPTION_KEY",
)
FILTER = {"ParameterFilters": [{"Key": "Name", "Option": "BeginsWith", "Values": ["/gbmi/prod/"]}]}


@pytest.fixture
def ssm() -> Any:
    client = boto3.client(
        "ssm", region_name="us-east-1", aws_access_key_id="test", aws_secret_access_key="test"
    )
    with Stubber(client) as stub:
        yield client, stub
        stub.assert_no_pending_responses()


@pytest.fixture
def laptop(tmp_path: Path) -> dict[str, Path]:
    env = tmp_path / ".env"
    env.write_text(f'POSTGRES_PASSWORD=local-value\nGBMI_SEC_USER_AGENT="{IDENTITY}"\n', "utf-8")
    makefile = tmp_path / "Makefile"
    makefile.write_text("SEC_SOURCE := live\nelse\nSEC_SOURCE := fixture\n", "utf-8")
    return {"env": env, "makefile": makefile}


def described(*names: str) -> dict[str, Any]:
    return {
        "Parameters": [{"Name": f"/gbmi/prod/{name}", "Type": "SecureString"} for name in names]
    }


def run(client: Any, laptop: dict[str, Path], *arguments: str) -> int:
    code: int = host_secrets.main(
        ["--env", str(laptop["env"]), "--makefile", str(laptop["makefile"]), *arguments],
        client=client,
    )
    return code


def test_the_template_names_the_fourteen_generated_secrets_of_the_host() -> None:
    # compose/host/render_env.py requires the same fourteen names on the host.
    assert tuple(host_secrets.generated_names(REPO / ".env.example")) == GENERATED
    # No bytecode folder beside the scripts of the host: a release copies the folder.
    sys.path.insert(0, str(REPO / "compose" / "host"))
    write_bytecode = sys.dont_write_bytecode
    sys.dont_write_bytecode = True
    try:
        import render_env
    finally:
        sys.dont_write_bytecode = write_bytecode
        sys.path.pop(0)
    assert render_env.REQUIRED == GENERATED


def test_create_writes_each_missing_entry_once_and_prints_names_only(
    ssm: Any, laptop: dict[str, Path], capsys: pytest.CaptureFixture[str]
) -> None:
    client, stub = ssm
    sent: list[str] = []
    real = client.put_parameter

    def recording(**arguments: Any) -> Any:
        sent.append(arguments["Value"])
        return real(**arguments)

    client.put_parameter = recording
    stub.add_response("describe_parameters", described(), FILTER)
    for name in (*GENERATED, "GBMI_SEC_USER_AGENT"):
        stub.add_response(
            "put_parameter",
            {"Version": 1},
            {
                "Name": f"/gbmi/prod/{name}",
                "Value": ANY,
                "Type": "SecureString",
                # An entry which appears between the list and the write is not replaced.
                "Overwrite": False,
                "Description": ANY,
            },
        )

    assert run(client, laptop, "create") == 0

    output = capsys.readouterr().out
    assert output.splitlines() == [
        *(f"created  {name}" for name in GENERATED),
        "created  GBMI_SEC_USER_AGENT",
    ]
    # Fourteen new values of 48 hexadecimal characters or a Fernet key, each different, and
    # the declared identity as the .env of the laptop holds it, without the quotes.
    assert len(sent) == 15 and len(set(sent)) == 15
    assert sent[-1] == IDENTITY
    assert all(len(value) >= 44 for value in sent[:-1])
    # The value of the laptop for a secret is not copied: the host gets its own secrets.
    assert "local-value" not in sent
    for value in sent:
        assert value not in output


def test_create_never_overwrites_an_entry(
    ssm: Any, laptop: dict[str, Path], capsys: pytest.CaptureFixture[str]
) -> None:
    client, stub = ssm
    stub.add_response("describe_parameters", described(*GENERATED, "GBMI_SEC_USER_AGENT"), FILTER)

    # No put_parameter is queued: a write would fail the test.
    assert run(client, laptop, "create") == 0

    lines = capsys.readouterr().out.splitlines()
    assert lines == [f"exists   {name}" for name in (*GENERATED, "GBMI_SEC_USER_AGENT")]


def test_a_failed_write_is_reported_by_its_code_and_not_by_its_text(
    ssm: Any, laptop: dict[str, Path], capsys: pytest.CaptureFixture[str]
) -> None:
    client, stub = ssm
    stub.add_response("describe_parameters", described(*GENERATED), FILTER)
    stub.add_client_error(
        "put_parameter", service_error_code="AccessDeniedException", service_message=IDENTITY
    )

    assert run(client, laptop, "create") == 1

    output = capsys.readouterr().out
    assert output.splitlines()[-1] == "FAILED (AccessDeniedException) GBMI_SEC_USER_AGENT"
    assert IDENTITY not in output


def test_the_source_live_needs_the_laptop_off_and_the_declared_identity(
    ssm: Any, laptop: dict[str, Path], capsys: pytest.CaptureFixture[str]
) -> None:
    client, stub = ssm

    # The working stack of the laptop still asks the live SEC: refused before any call.
    assert run(client, laptop, "sec-source", "live") == 1
    assert "has the SEC source 'live' in the Makefile" in capsys.readouterr().out

    laptop["makefile"].write_text("SEC_SOURCE := off\nelse\nSEC_SOURCE := fixture\n", "utf-8")
    stub.add_response("describe_parameters", described(*GENERATED), FILTER)
    assert run(client, laptop, "sec-source", "live") == 1
    assert "the entry GBMI_SEC_USER_AGENT is missing" in capsys.readouterr().out

    stub.add_response("describe_parameters", described("GBMI_SEC_USER_AGENT"), FILTER)
    stub.add_response(
        "put_parameter",
        {"Version": 2},
        {
            "Name": "/gbmi/prod/GBMI_SEC_SOURCE",
            "Value": "live",
            "Type": "String",
            "Overwrite": True,
            "Description": ANY,
        },
    )
    assert run(client, laptop, "sec-source", "live") == 0
    assert capsys.readouterr().out.startswith("updated  GBMI_SEC_SOURCE = live")


def test_the_source_off_needs_no_condition(
    ssm: Any, laptop: dict[str, Path], capsys: pytest.CaptureFixture[str]
) -> None:
    client, stub = ssm
    stub.add_response(
        "put_parameter",
        {"Version": 3},
        {
            "Name": "/gbmi/prod/GBMI_SEC_SOURCE",
            "Value": "off",
            "Type": "String",
            "Overwrite": True,
            "Description": ANY,
        },
    )

    assert run(client, laptop, "sec-source", "off") == 0
    assert capsys.readouterr().out.startswith("updated  GBMI_SEC_SOURCE = off")


def test_the_list_shows_names_and_types_only(
    ssm: Any, laptop: dict[str, Path], capsys: pytest.CaptureFixture[str]
) -> None:
    client, stub = ssm
    stub.add_response(
        "describe_parameters", described("CH_BI_PASSWORD", "AIRFLOW_JWT_SECRET"), FILTER
    )

    assert run(client, laptop, "list") == 0

    lines = [line.split() for line in capsys.readouterr().out.splitlines()]
    assert lines == [
        ["AIRFLOW_JWT_SECRET", "SecureString"],
        ["CH_BI_PASSWORD", "SecureString"],
        ["2", "entries", "below", "/gbmi/prod/"],
    ]
