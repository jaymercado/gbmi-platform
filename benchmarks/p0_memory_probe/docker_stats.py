"""Sample memory and CPU of the Compose stack with `docker stats` (week 1 plan, WP-05 and WP-10).

Two modes:

  sample   take N samples a fixed interval apart and report the median per container
           (the idle measurement: five samples, 30 seconds apart, after ten idle minutes)
  watch    sample continuously while a command runs and report the peak per container
           (the measurement under load)

Both also read each container's cgroup `memory.peak`, the high-water mark since the
container started, which catches spikes that fall between two samples.

This is a measurement script, not package code. It uses the standard library only.

Usage:
    python benchmarks/p0_memory_probe/docker_stats.py sample --label idle --out result.json
    python benchmarks/p0_memory_probe/docker_stats.py watch --label demo --out result.json \\
        -- make demo
"""

from __future__ import annotations

import argparse
import json
import statistics
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

UNITS = {
    "B": 1,
    "kB": 1000,
    "MB": 1000**2,
    "GB": 1000**3,
    "KiB": 1024,
    "MiB": 1024**2,
    "GiB": 1024**3,
}


def run(*args: str) -> str:
    return subprocess.run(args, check=True, capture_output=True, text=True).stdout  # noqa: S603


def to_bytes(text: str) -> int:
    """Parse a docker size such as '123.4MiB' into bytes."""
    text = text.strip()
    for unit in sorted(UNITS, key=len, reverse=True):
        if text.endswith(unit):
            return int(float(text[: -len(unit)]) * UNITS[unit])
    raise ValueError(f"unrecognised size: {text!r}")


def containers(project: str) -> dict[str, str]:
    """Map container id to Compose service name for the running containers of a project."""
    output = run(
        "docker", "ps",
        "--filter", f"label=com.docker.compose.project={project}",
        "--format", '{{.ID}}\t{{.Label "com.docker.compose.service"}}',
    )  # fmt: skip
    return dict(line.split("\t") for line in output.splitlines() if line)


def take_sample(services: dict[str, str]) -> dict[str, dict[str, float]]:
    """One `docker stats` reading: memory in use (working set), its limit and CPU percent.

    Every running container is read and the project's ones are picked out afterwards. Naming
    the containers on the command line would fail whenever a one-shot container exits between
    listing and sampling.
    """
    output = run("docker", "stats", "--no-stream", "--no-trunc", "--format", "{{json .}}")
    sample: dict[str, dict[str, float]] = {}
    for line in output.splitlines():
        row = json.loads(line)
        service = next(
            (name for container_id, name in services.items() if row["ID"].startswith(container_id)),
            None,
        )
        if service is None or " / " not in row["MemUsage"]:
            continue
        used, _, limit = row["MemUsage"].partition(" / ")
        sample[service] = {
            "mem_bytes": to_bytes(used),
            "limit_bytes": to_bytes(limit),
            "cpu_percent": float(row["CPUPerc"].rstrip("%")),
        }
    return sample


def cgroup_peaks(services: dict[str, str]) -> dict[str, int | None]:
    """memory.peak of each container's cgroup (cgroup v2), or None where it cannot be read."""
    peaks: dict[str, int | None] = {}
    for container_id, service in services.items():
        try:
            peaks[service] = int(
                run("docker", "exec", container_id, "cat", "/sys/fs/cgroup/memory.peak")
            )
        except (subprocess.CalledProcessError, ValueError):
            peaks[service] = None
    return peaks


def docker_host() -> dict[str, Any]:
    info = json.loads(run("docker", "info", "--format", "{{json .}}"))
    return {
        "server_version": info.get("ServerVersion"),
        "mem_total_bytes": info.get("MemTotal"),
        "cpus": info.get("NCPU"),
        "architecture": info.get("Architecture"),
        "operating_system": info.get("OperatingSystem"),
    }


def summarise(
    samples: list[dict[str, dict[str, float]]], peaks: dict[str, int | None]
) -> dict[str, Any]:
    services = sorted({service for sample in samples for service in sample})
    summary: dict[str, Any] = {}
    for service in services:
        memory = [sample[service]["mem_bytes"] for sample in samples if service in sample]
        cpu = [sample[service]["cpu_percent"] for sample in samples if service in sample]
        limits = [sample[service]["limit_bytes"] for sample in samples if service in sample]
        summary[service] = {
            "samples": len(memory),
            "mem_median_bytes": int(statistics.median(memory)),
            "mem_min_bytes": int(min(memory)),
            "mem_max_bytes": int(max(memory)),
            "cpu_median_percent": round(statistics.median(cpu), 2),
            "cpu_max_percent": round(max(cpu), 2),
            "limit_bytes": int(limits[-1]),
            "cgroup_peak_bytes": peaks.get(service),
        }
    return summary


def print_table(summary: dict[str, Any], host_memory: int | None) -> None:
    def mib(value: int | None) -> str:
        return "-" if value is None else f"{value / 1024**2:,.0f}"

    header = f"{'service':<24}{'median MiB':>12}{'max MiB':>10}{'cgroup peak':>13}"
    print(header + f"{'limit MiB':>11}{'cpu max %':>11}")
    total_median = total_max = 0
    for service, row in summary.items():
        # docker reports the host's memory as the limit of an unlimited container.
        unlimited = host_memory is not None and row["limit_bytes"] >= host_memory * 0.99
        limit = "none" if unlimited else mib(row["limit_bytes"])
        print(
            f"{service:<24}{mib(row['mem_median_bytes']):>12}{mib(row['mem_max_bytes']):>10}"
            f"{mib(row['cgroup_peak_bytes']):>13}{limit:>11}{row['cpu_max_percent']:>11}"
        )
        total_median += row["mem_median_bytes"]
        total_max += row["mem_max_bytes"]
    print(f"{'total':<24}{mib(total_median):>12}{mib(total_max):>10}")


def write_result(
    args: argparse.Namespace,
    samples: list[dict[str, dict[str, float]]],
    services: dict[str, str],
    started: datetime,
    extra: dict[str, Any],
) -> None:
    host = docker_host()
    summary = summarise(samples, cgroup_peaks(services))
    result = {
        "label": args.label,
        "mode": args.mode,
        "project": args.project,
        "started_at": started.isoformat(timespec="seconds"),
        "finished_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "interval_seconds": args.interval,
        "docker": host,
        **extra,
        "containers": summary,
    }
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print_table(summary, host.get("mem_total_bytes"))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("mode", choices=["sample", "watch"])
    parser.add_argument("--project", default="gbmi", help="Compose project name")
    parser.add_argument("--label", required=True, help="name of this measurement")
    parser.add_argument("--out", type=Path, help="write the result as JSON")
    parser.add_argument("--samples", type=int, default=5, help="sample mode: number of samples")
    parser.add_argument(
        "--interval",
        type=float,
        default=None,
        help="seconds between samples (default: 30 for sample, 2 for watch)",
    )
    # Everything after "--" is the command to watch; argparse never sees it.
    argv = sys.argv[1:]
    command: list[str] = []
    if "--" in argv:
        split = argv.index("--")
        argv, command = argv[:split], argv[split + 1 :]
    args = parser.parse_args(argv)
    if args.interval is None:
        args.interval = 30.0 if args.mode == "sample" else 2.0

    services = containers(args.project)
    if not services:
        print(f"no running containers for Compose project {args.project!r}", file=sys.stderr)
        return 2
    started = datetime.now(UTC)
    samples: list[dict[str, dict[str, float]]] = []

    if args.mode == "sample":
        for index in range(args.samples):
            if index:
                time.sleep(args.interval)
            samples.append(take_sample(services))
        write_result(args, samples, services, started, {})
        return 0

    if not command:
        parser.error("watch mode needs a command after --")
    process = subprocess.Popen(command)  # noqa: S603
    while process.poll() is None:
        tick = time.monotonic()
        services = containers(args.project) or services  # containers may start during the run
        samples.append(take_sample(services))
        time.sleep(max(0.0, args.interval - (time.monotonic() - tick)))
    samples.append(take_sample(containers(args.project) or services))
    extra = {
        "command": command,
        "exit_code": process.returncode,
        "duration_seconds": round((datetime.now(UTC) - started).total_seconds(), 1),
    }
    write_result(args, samples, containers(args.project) or services, started, extra)
    return process.returncode


if __name__ == "__main__":
    raise SystemExit(main())
