"""The report of one Trivy scan as Markdown for the summary of a workflow job.

    python3 docker/airflow/trivy_summary.py <report.json>

The scan lists the findings of the severities HIGH and CRITICAL which have a fixed version.
The summary counts the findings and shows the first rows; the scan never fails the workflow
in phase P2 (week 3 plan 3.3).
"""

from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

ROWS = 40


def summary(report: dict[str, Any]) -> list[str]:
    """The lines of the summary for one report of Trivy."""
    findings = sorted(
        {
            (
                finding.get("Severity", ""),
                finding.get("PkgName", ""),
                finding.get("InstalledVersion", ""),
                finding.get("FixedVersion", ""),
                finding.get("VulnerabilityID", ""),
                result.get("Type", ""),
            )
            for result in report.get("Results") or []
            for finding in result.get("Vulnerabilities") or []
        }
    )
    counts = Counter(finding[0] for finding in findings)
    lines = [
        "### Trivy scan of the runtime image",
        "",
        f"Image: `{report.get('ArtifactName', '')}`",
        "",
        f"Findings with a fixed version: {counts.get('CRITICAL', 0)} critical, "
        f"{counts.get('HIGH', 0)} high. The scan is a report: no finding fails the workflow.",
    ]
    if findings:
        lines += [
            "",
            "| Severity | Package | Installed | Fixed | Id | Kind |",
            "|---|---|---|---|---|---|",
        ]
        lines += ["| " + " | ".join(finding) + " |" for finding in findings[:ROWS]]
        if len(findings) > ROWS:
            lines += ["", f"{len(findings) - ROWS} more rows are not shown."]
    return lines


def main(path: str) -> int:
    report = json.loads(Path(path).read_text(encoding="utf-8"))
    sys.stdout.write("\n".join(summary(report)) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
