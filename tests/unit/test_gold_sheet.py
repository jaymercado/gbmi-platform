"""The labelling sheet keeps the format which the gold-set commands of phase P3 read.

The rules are those of ``config/labelling/README.md``.
"""

from __future__ import annotations

import csv
import re
from collections import Counter
from pathlib import Path

import pytest

SHEET = Path(__file__).resolve().parents[2] / "config" / "labelling" / "gold_sheet_v0.csv"

COLUMNS = [
    "org_string", "stratum", "doc_count",
    "title_1", "title_2", "title_3", "title_4", "title_5",
    "label", "cik", "note",
]  # fmt: skip
LABELS = {"company_in_universe", "company_not_in_universe", "not_a_company", "ambiguous"}
#: A CIK is written without leading zeros and has at most ten digits.
CIK = re.compile(r"[1-9][0-9]{0,9}")
#: Markers at the start of a note. Only a row with the label company_in_universe has one.
UNIVERSE_MARKERS = ("brand:", "subsidiary:", "otc:")


@pytest.fixture(scope="module")
def rows() -> list[dict[str, str]]:
    with SHEET.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        assert reader.fieldnames == COLUMNS
        return list(reader)


def test_the_sample_is_the_one_that_was_drawn(rows: list[dict[str, str]]) -> None:
    # A row with too many or too few fields shows up as a None key or a None value.
    assert all(None not in row and None not in row.values() for row in rows)
    assert Counter(row["stratum"] for row in rows) == {"top": 100, "random": 100}
    assert len({row["org_string"] for row in rows}) == len(rows)
    assert all(row["doc_count"].isdigit() for row in rows)


def test_every_label_is_one_of_the_four(rows: list[dict[str, str]]) -> None:
    unknown = {row["label"] for row in rows} - LABELS - {""}

    assert not unknown, f"labels outside the README: {sorted(unknown)}"


def test_a_cik_goes_with_a_universe_label(rows: list[dict[str, str]]) -> None:
    for row in rows:
        name, label, cik = row["org_string"], row["label"], row["cik"]
        if label == "company_in_universe":
            assert CIK.fullmatch(cik), f"{name!r} needs a CIK, digits only"
        elif label == "ambiguous":
            assert cik == "" or CIK.fullmatch(cik), f"{name!r}: the CIK is not digits only"
        else:
            assert cik == "", f"{name!r} has a CIK but the label {label!r}"


def test_universe_markers_stay_on_universe_rows(rows: list[dict[str, str]]) -> None:
    for row in rows:
        if row["note"].startswith(UNIVERSE_MARKERS):
            assert row["label"] == "company_in_universe", row["org_string"]
