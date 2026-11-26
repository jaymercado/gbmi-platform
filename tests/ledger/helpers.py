"""Small builders for the ledger tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from gbmi.core.gdelt import Feed, Slot
from gbmi.ledger.repository import Accounting, Ledger, Listing, SlotKey

T0 = datetime(2026, 11, 25, 8, 0, tzinfo=UTC)


def key(minutes: int = 0, feed: Feed = Feed.EXPORT) -> SlotKey:
    """The slot which starts ``minutes`` after 2026-11-25 08:00 UTC."""
    return SlotKey(feed, Slot(T0 + timedelta(minutes=minutes)))


def listing(slot_key: SlotKey, size: int = 10, md5: str = "0" * 32) -> Listing:
    name = slot_key.slot.file_name(slot_key.feed)
    return Listing(slot_key, f"https://data.gdeltproject.org/gdeltv2/{name}", size, md5)


def accounting(
    slot_key: SlotKey, raw: int = 10, parsed: int = 9, quarantined: int = 1
) -> Accounting:
    return Accounting(slot_key, raw, parsed, quarantined)


def listed(ledger: Ledger, *keys: SlotKey) -> None:
    ledger.ensure_expected(list(keys))
    assert ledger.mark_listed([listing(slot_key) for slot_key in keys]) == set(keys)


def verified(ledger: Ledger, *keys: SlotKey) -> None:
    listed(ledger, *keys)
    for slot_key in keys:
        ledger.record_download(
            slot_key, bronze_uri=f"file:///lake/{slot_key.slot.bronze_key(slot_key.feed)}"
        )


def loaded(ledger: Ledger, *keys: SlotKey) -> None:
    verified(ledger, *keys)
    assert ledger.mark_loaded([accounting(slot_key) for slot_key in keys], snapshot_id=1) == set(
        keys
    )
