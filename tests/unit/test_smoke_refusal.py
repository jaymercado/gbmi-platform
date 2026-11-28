"""The smoke test of a deployment recognises a refused write of ClickHouse to the lake.

The first deployment on the AWS host (2026-11-28) failed on one check of twenty. S3 had refused
the write with the lake role, as wanted, but the check knew the refusal only by the words of
an error code, and ClickHouse 26.8 shows the message of AWS without the code. The test keeps
the two real answers beside the pattern of ``compose/smoke.sh``.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
SMOKE = (REPO / "compose" / "smoke.sh").read_text(encoding="utf-8")

#: The answer of the host on 2026-11-28, with a made-up account number.
REFUSED_BY_S3 = (
    "Received exception from server (version 26.8.49):\n"
    "Code: 499. DB::Exception: Received from localhost:9000. DB::Exception: Message: User: "
    "arn:aws:sts::000000000000:assumed-role/gbmi-clickhouse-lake-ro/ClickHouseSession is not "
    'authorized to perform: s3:PutObject on resource: "arn:aws:s3:::gbmi-prod-lake-'
    '000000000000/warehouse/_smoke/refused.csv" because no identity-based policy allows the '
    "s3:PutObject action"
)
#: The answer of ClickHouse 26.8.49 on the host to a query which names no credentials.
REFUSED_BY_CLICKHOUSE = (
    "Received exception from server (version 26.8.49):\n"
    "Code: 497. DB::Exception: Received from localhost:9000. DB::Exception: S3 access from "
    "user queries is not allowed to use the server's own credentials (environment variables, "
    "instance metadata, IRSA, instance profile, or AWS config files). Provide explicit "
    "credentials, use NOSIGN for public buckets, grant access through an IAM role passed as "
    "extra_credentials(role_arn = '...'), or enable the setting "
    "`s3_allow_server_credentials_in_user_queries`.. (ACCESS_DENIED)"
)


def pattern() -> re.Pattern[str]:
    found = re.search(r"^REFUSAL='([^']+)'$", SMOKE, re.MULTILINE)
    assert found is not None, "compose/smoke.sh no longer defines REFUSAL"
    # grep -qiE in the script: an extended expression, without regard to case.
    return re.compile(found.group(1), re.IGNORECASE)


@pytest.mark.parametrize("answer", [REFUSED_BY_S3, REFUSED_BY_CLICKHOUSE, "AccessDenied"])
def test_a_refusal_is_recognised(answer: str) -> None:
    assert pattern().search(answer)


@pytest.mark.parametrize(
    "answer",
    [
        "Code: 210. DB::NetException: Connection refused (localhost:9000)",
        "Code: 499. DB::Exception: The specified bucket does not exist. (S3_ERROR)",
        "Code: 62. DB::Exception: Syntax error",
        "User: x is not authorized to perform: s3:GetObject on resource",
    ],
)
def test_another_failure_is_no_refusal(answer: str) -> None:
    assert not pattern().search(answer)


def test_the_check_uses_the_pattern() -> None:
    assert 'grep -qiE "$REFUSAL"' in SMOKE
