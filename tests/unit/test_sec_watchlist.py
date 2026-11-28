"""The seed of the watchlist as the pipelines read the seed (week 3 plan 3.3)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from gbmi.config import Settings
from gbmi.sources.sec.watchlist import COLUMNS, Entry, WatchlistError, read_watchlist, watchlist

REPO = Path(__file__).resolve().parents[2]
SEED = REPO / "dbt" / "seeds" / "watchlist.csv"
HEADER = ",".join(COLUMNS) + "\n"


def write(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "watchlist.csv"
    path.write_text(text, encoding="utf-8")
    return path


def test_the_seed_of_the_repository_starts_with_the_five_demo_companies() -> None:
    entries = read_watchlist(SEED)

    assert [entry.cik for entry in entries[:5]] == [1326801, 1045810, 789019, 63276, 1075124]
    assert entries[0] == Entry(
        rank=1,
        cik=1326801,
        name_at_selection="Meta Platforms, Inc.",
        reason="demo company: the name change of exit-gate row 2",
        added_on=date(2026, 11, 28),
    )
    assert 5 <= len(entries) <= 50


def test_a_pipeline_reads_the_first_rows_of_the_seed(tmp_path: Path) -> None:
    path = write(
        tmp_path,
        HEADER
        + "".join(f"{rank},{rank * 10},Name {rank},test,2026-11-28\n" for rank in range(1, 8)),
    )

    def settings(size: int) -> Settings:
        return Settings(_env_file=None, watchlist_file=str(path), watchlist_size=size)

    assert watchlist(settings(3)) == [10, 20, 30]
    assert watchlist(settings(50)) == [10, 20, 30, 40, 50, 60, 70]


@pytest.mark.parametrize(
    ("text", "problem"),
    [
        ("cik,rank\n1,1\n", "needs the columns rank, cik"),
        (HEADER + "1,10,A,test,2026-11-28\n3,20,B,test,2026-11-28\n", "count from 1 without a gap"),
        (HEADER + "2,10,A,test,2026-11-28\n1,20,B,test,2026-11-28\n", "count from 1 without a gap"),
        (HEADER + "1,10,A,test,2026-11-28\n2,10,B,test,2026-11-28\n", "is repeated"),
        (HEADER + "1,0,A,test,2026-11-28\n", "no positive number"),
        (HEADER + "1,ten,A,test,2026-11-28\n", "holds a row which is no company"),
        (HEADER + "1,10,A,test,soon\n", "holds a row which is no company"),
        (HEADER + "1,10\n", "holds a row which is no company"),
    ],
)
def test_a_seed_which_is_no_list_of_companies_is_refused(
    tmp_path: Path, text: str, problem: str
) -> None:
    with pytest.raises(WatchlistError, match=problem):
        read_watchlist(write(tmp_path, text))


def test_a_missing_seed_is_refused_and_an_empty_seed_is_an_empty_list(tmp_path: Path) -> None:
    with pytest.raises(WatchlistError, match="cannot be read"):
        read_watchlist(tmp_path / "nowhere.csv")
    assert read_watchlist(write(tmp_path, HEADER)) == []
