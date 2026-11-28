"""The watchlist: the companies whose filings and facts the platform follows (week 3 plan 3.3).

The list is the seed ``dbt/seeds/watchlist.csv``: one row for each company with its rank. dbt
reads the file as a seed, and the pipelines read the first ``GBMI_WATCHLIST_SIZE`` rows. One
file gives both readers the same list.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from gbmi.config import Settings, get_settings

COLUMNS = ("rank", "cik", "name_at_selection", "reason", "added_on")


class WatchlistError(ValueError):
    """The seed of the watchlist cannot be read as a list of companies."""


@dataclass(frozen=True)
class Entry:
    rank: int
    cik: int
    name_at_selection: str
    reason: str
    added_on: date


def read_watchlist(path: Path) -> list[Entry]:
    """Every row of the seed, in the order of the rank.

    The ranks count from 1 without a gap, and a company stands in the list once.
    """
    try:
        with path.open(encoding="utf-8", newline="") as stream:
            reader = csv.DictReader(stream)
            if tuple(reader.fieldnames or ()) != COLUMNS:
                raise WatchlistError(f"{path} needs the columns {', '.join(COLUMNS)}")
            entries = [
                Entry(
                    rank=int(row["rank"]),
                    cik=int(row["cik"]),
                    name_at_selection=row["name_at_selection"],
                    reason=row["reason"],
                    added_on=date.fromisoformat(row["added_on"]),
                )
                for row in reader
            ]
    except OSError as error:
        raise WatchlistError(f"the seed of the watchlist cannot be read: {error}") from None
    except (KeyError, TypeError, ValueError) as error:
        if isinstance(error, WatchlistError):
            raise
        raise WatchlistError(f"{path} holds a row which is no company: {error}") from None
    if [entry.rank for entry in entries] != list(range(1, len(entries) + 1)):
        raise WatchlistError(f"the ranks of {path} must count from 1 without a gap")
    if len({entry.cik for entry in entries}) != len(entries) or any(e.cik <= 0 for e in entries):
        raise WatchlistError(f"a CIK of {path} is repeated or is no positive number")
    return entries


def watchlist(settings: Settings | None = None) -> list[int]:
    """The CIKs of the first ``GBMI_WATCHLIST_SIZE`` rows of the seed, in the order of the rank."""
    settings = settings or get_settings()
    entries = read_watchlist(Path(settings.watchlist_file))
    return [entry.cik for entry in entries[: settings.watchlist_size]]
