"""The Compose files of the AWS host, as Compose renders them (week 3 plan, WP-10 step 6).

``compose/compose.prod.yaml`` is an overlay on ``compose/compose.yaml``: the laptop and the host
run the same service definitions (decision TD-15), and the overlay changes what differs on the
host. The test renders both files with ``docker compose config`` and made-up values, and checks
each difference. Nothing is started.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest

REPO = Path(__file__).resolve().parents[2]
COMMIT = "0123456789abcdef0123456789abcdef01234567"
HOST_VALUES = {
    "GBMI_RELEASE": COMMIT,
    "GBMI_LAKE_BUCKET": "gbmi-prod-lake-000000000000",
    "GBMI_OPS_BUCKET": "gbmi-prod-ops-000000000000",
    "GBMI_LOG_GROUP": "/gbmi/prod/containers",
    "GBMI_CH_LAKE_ROLE_ARN": "arn:aws:iam::000000000000:role/gbmi-clickhouse-lake-ro",
    "GBMI_SEC_SOURCE": "live",
    "AWS_DEFAULT_REGION": "us-east-1",
}
#: The services which run the Airflow image; the toolbox is the eighth.
AIRFLOW_IMAGE_SERVICES = (
    "ops-migrate",
    "airflow-init",
    "dbt-manifest",
    "airflow-apiserver",
    "airflow-scheduler",
    "airflow-dag-processor",
    "airflow-triggerer",
)
LONG_RUNNING = (
    "postgres",
    "clickhouse",
    "airflow-apiserver",
    "airflow-scheduler",
    "airflow-dag-processor",
    "airflow-triggerer",
)
ONE_SHOT = ("clickhouse-init", "ops-migrate", "postgres-roles", "airflow-init", "dbt-manifest")

_STACK = ("GBMI_", "COMPOSE_", "AIRFLOW_", "DBT_", "MEM_", "CH_", "METABASE_", "POSTGRES_", "AWS_")


def _compose_is_installed() -> bool:
    docker = shutil.which("docker")
    if docker is None:
        return False
    return subprocess.run([docker, "compose", "version"], capture_output=True).returncode == 0


pytestmark = pytest.mark.skipif(
    not _compose_is_installed(), reason="Docker with the Compose plugin is not installed"
)


def render(tmp_path: Path, *files: str, values: dict[str, str] | None = None) -> dict[str, Any]:
    """The services of the given Compose files, as Compose renders them with an env file."""
    template = (REPO / ".env.example").read_text(encoding="utf-8")
    made_up = template.replace("__GENERATE_FERNET__", "fernet").replace("__GENERATE__", "abc123")
    lines = [f"{name}={value}" for name, value in (values or {}).items()]
    env_file = tmp_path / "env"
    env_file.write_text(made_up + "\n" + "\n".join(lines) + "\n", encoding="utf-8")
    command = [
        shutil.which("docker") or "docker",
        "compose",
        "--project-name",
        "gbmi",
        "--project-directory",
        str(REPO),
        "--env-file",
        str(env_file),
    ]
    for name in files:
        command += ["-f", str(REPO / "compose" / name)]
    command += ["--profile", "tools", "config", "--format", "json"]
    # A variable of the shell wins over the env file in Compose, so the stack variables of the
    # caller (the Makefile exports some) are removed first.
    base = {name: value for name, value in os.environ.items() if not name.startswith(_STACK)}
    result = subprocess.run(command, capture_output=True, text=True, check=False, env=base)
    assert result.returncode == 0, result.stderr
    services: dict[str, Any] = json.loads(result.stdout)["services"]
    return services


@pytest.fixture(scope="module")
def host(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    return render(
        tmp_path_factory.mktemp("host"), "compose.yaml", "compose.prod.yaml", values=HOST_VALUES
    )


@pytest.fixture(scope="module")
def laptop(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    return render(tmp_path_factory.mktemp("laptop"), "compose.yaml")


def mounts(service: dict[str, Any]) -> list[tuple[str, str, str]]:
    return [
        (volume["type"], volume.get("source", ""), volume["target"])
        for volume in service.get("volumes", [])
    ]


def test_the_overlay_adds_no_service_and_drops_none(
    host: dict[str, Any], laptop: dict[str, Any]
) -> None:
    assert sorted(host) == sorted(laptop)
    assert set(host) == {*AIRFLOW_IMAGE_SERVICES, "toolbox", "postgres", "clickhouse",
                         "clickhouse-init", "postgres-roles"}  # fmt: skip


def test_the_airflow_image_comes_from_the_registry_by_the_commit(host: dict[str, Any]) -> None:
    for name in (*AIRFLOW_IMAGE_SERVICES, "toolbox"):
        assert host[name]["image"] == f"ghcr.io/jaymercado/gbmi-airflow:{COMMIT}", name
    # No service is built on the host, and every other image is pinned by its digest.
    assert [name for name, service in host.items() if "build" in service] == []
    for name in ("postgres", "clickhouse", "clickhouse-init", "postgres-roles"):
        assert "@sha256:" in host[name]["image"], name


def test_no_service_mounts_a_source_folder_the_fixtures_or_a_lake_folder(
    host: dict[str, Any],
) -> None:
    for name in AIRFLOW_IMAGE_SERVICES:
        assert mounts(host[name]) == [("volume", "airflow-logs", "/opt/airflow/logs")], name
    assert mounts(host["toolbox"]) == []
    targets = {target for service in host.values() for _, _, target in mounts(service)}
    assert not {target for target in targets if target.startswith(("/lake", "/fixtures"))}
    assert not {target for target in targets if target.startswith("/opt/gbmi")}
    # A bind mount of the host is a file of the release folder, and the mount is read-only.
    for name, service in host.items():
        for volume in service.get("volumes", []):
            if volume["type"] == "bind":
                assert volume["source"].startswith(str(REPO)), name
                assert volume.get("read_only") is True, (name, volume["target"])


def test_clickhouse_on_the_host_has_no_lake_folder_and_no_local_lake_setting(
    host: dict[str, Any], laptop: dict[str, Any]
) -> None:
    def config_files(service: dict[str, Any]) -> list[str]:
        return sorted(
            Path(target).name for _, _, target in mounts(service) if target.startswith("/etc/")
        )

    assert config_files(laptop["clickhouse"]) == ["gbmi.xml", "gbmi.xml", "lake-local.xml"]
    assert config_files(host["clickhouse"]) == ["gbmi.xml", "gbmi.xml"]
    assert ("/lake" in {target for _, _, target in mounts(laptop["clickhouse"])}) is True
    assert host["clickhouse"]["environment"]["CH_MAX_SERVER_MEMORY_USAGE"] == "1717986918"


def test_the_databases_publish_no_port_and_every_published_port_is_local(
    host: dict[str, Any],
) -> None:
    published = {
        name: [(port["host_ip"], port["published"]) for port in service.get("ports", [])]
        for name, service in host.items()
        if service.get("ports")
    }

    # The security group of the host has no inbound rule; an operator reaches the API server
    # through a port forward of Session Manager (week 3 plan, exit-gate row 4).
    assert published == {"airflow-apiserver": [("127.0.0.1", "8080")]}


def test_the_package_settings_of_the_host_name_s3_and_glue(host: dict[str, Any]) -> None:
    for name in (*AIRFLOW_IMAGE_SERVICES, "toolbox"):
        environment = host[name]["environment"]
        wanted = {
            "GBMI_ENV": "prod",
            "GBMI_LAKE_BACKEND": "aws",
            "GBMI_LAKE_URI": "s3://gbmi-prod-lake-000000000000",
            "GBMI_CATALOG_URI": "glue://us-east-1",
            "GBMI_TASK_RETRIES": "3",
            "GBMI_DAG_SCHEDULES": "on",
            "GBMI_SEC_SOURCE": "live",
            "DBT_TARGET": "prod",
            "AWS_DEFAULT_REGION": "us-east-1",
            "GBMI_CH_LAKE_ACCESS": "role",
            "GBMI_CH_LAKE_ROLE_ARN": HOST_VALUES["GBMI_CH_LAKE_ROLE_ARN"],
            "AIRFLOW__API__WORKERS": "1",
        }
        assert {key: environment.get(key) for key in wanted} == wanted, name
        # The image carries the commit as the pipeline version; no file of the host sets it.
        assert "GBMI_PIPELINE_VERSION" not in environment, name


def test_the_task_logs_go_to_the_ops_bucket(host: dict[str, Any]) -> None:
    for name in AIRFLOW_IMAGE_SERVICES:
        environment = host[name]["environment"]
        assert environment["AIRFLOW__LOGGING__REMOTE_LOGGING"] == "true", name
        assert environment["AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER"] == (
            "s3://gbmi-prod-ops-000000000000/airflow-logs"
        )
        assert environment["AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID"] == "aws_default"
        # The connection names no key: the provider uses the role of the host.
        assert environment["AIRFLOW_CONN_AWS_DEFAULT"] == "aws://"


def test_the_long_running_services_start_again_and_the_one_shot_services_do_not(
    host: dict[str, Any], laptop: dict[str, Any]
) -> None:
    assert {name: host[name]["restart"] for name in LONG_RUNNING} == dict.fromkeys(
        LONG_RUNNING, "unless-stopped"
    )
    assert {name: host[name]["restart"] for name in ONE_SHOT} == dict.fromkeys(ONE_SHOT, "no")
    # On the laptop a failure stays visible.
    assert {service["restart"] for service in laptop.values()} == {"no"}


def test_the_containers_log_to_cloudwatch(host: dict[str, Any], laptop: dict[str, Any]) -> None:
    for name in (*LONG_RUNNING, *ONE_SHOT):
        logging = host[name]["logging"]
        assert logging["driver"] == "awslogs", name
        assert logging["options"] == {
            "awslogs-region": "us-east-1",
            "awslogs-group": "/gbmi/prod/containers",
            "tag": "{{.Name}}",
            "mode": "non-blocking",
            "max-buffer-size": "4m",
        }
    # The output of a one-off command of an operator stays on the terminal.
    assert "logging" not in host["toolbox"]
    assert [name for name, service in laptop.items() if "logging" in service] == []


def test_the_memory_limits_fit_a_host_with_eight_gigabytes(host: dict[str, Any]) -> None:
    mebibyte = 1024 * 1024
    limits = {
        name: int(service["mem_limit"]) // mebibyte
        for name, service in host.items()
        if service.get("mem_limit")
    }

    assert limits == {
        "airflow-apiserver": 700,
        "airflow-scheduler": 2000,
        "airflow-dag-processor": 400,
        "airflow-triggerer": 400,
        "postgres": 400,
        "clickhouse": 2000,
    }
    # 5.9 GiB for the six services; Metabase starts on demand only (week 3 plan, WP-10 step 4).
    assert sum(limits.values()) == 5900


def test_the_toolbox_of_the_host_carries_no_variable_of_the_tests(host: dict[str, Any]) -> None:
    environment = host["toolbox"]["environment"]

    for name in (
        "GBMI_TEST_PG_ADMIN_DSN",
        "GBMI_TEST_AWS",
        "GBMI_TEST_AWS_LAKE_BUCKET",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
    ):
        assert name not in environment, name


def test_the_overlay_refuses_to_render_without_the_values_of_the_host(tmp_path: Path) -> None:
    for missing in ("GBMI_RELEASE", "GBMI_LAKE_BUCKET", "GBMI_OPS_BUCKET", "GBMI_LOG_GROUP"):
        values = {name: value for name, value in HOST_VALUES.items() if name != missing}
        with pytest.raises(AssertionError, match=r"required variable|is missing a value"):
            render(tmp_path, "compose.yaml", "compose.prod.yaml", values=values)


def test_metabase_starts_on_demand_with_a_limit_of_its_own(tmp_path: Path) -> None:
    services = render(
        tmp_path,
        "compose.yaml",
        "compose.bi.yaml",
        "compose.prod.yaml",
        "compose.prod.bi.yaml",
        values=HOST_VALUES,
    )
    metabase = services["metabase"]

    assert int(metabase["mem_limit"]) == 1300 * 1024 * 1024
    assert metabase["logging"]["driver"] == "awslogs"
    assert metabase["logging"]["options"] == services["postgres"]["logging"]["options"]
    assert [(port["host_ip"], port["published"]) for port in metabase["ports"]] == [
        ("127.0.0.1", "3000")
    ]
    assert "@sha256:" in metabase["image"]
    # Metabase runs while an operator looks at a dashboard: no restart on its own.
    assert metabase["restart"] == "no"
    total = sum(int(service.get("mem_limit", 0)) for service in services.values())
    assert total == 7200 * 1024 * 1024
