"""Small rows for the five SEC tables, for the tests of the lake."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pyarrow as pa

from gbmi.lake.tables import (
    SEC_COMPANY_FORMER_NAME,
    SEC_COMPANY_SNAPSHOT,
    SEC_FILING_DETAIL,
    SEC_FILING_INDEX,
    SEC_SCHEMAS,
    SEC_XBRL_FACT,
)

META, NVIDIA, MATTEL = 1326801, 1045810, 63276
DAY = date(2026, 11, 25)
INGESTED_AT = datetime(2026, 11, 26, 8, 30, tzinfo=UTC)
PROVENANCE: dict[str, Any] = {
    "_source_file": "raw/sec/test/object.json",
    "_source_md5": "0" * 32,
    "_ingested_at": INGESTED_AT,
    "_run_id": "integration",
    "_pipeline_version": "test",
}


def table(name: str, records: list[dict[str, Any]]) -> pa.Table:
    """Rows of one SEC table with the Arrow schema of the table; provenance is filled in."""
    schema = SEC_SCHEMAS[name].as_arrow()
    return pa.Table.from_pylist([{**PROVENANCE, **record} for record in records], schema=schema)


def snapshot(day: date, ciks: list[int], *, suffix: str = "") -> pa.Table:
    return table(
        SEC_COMPANY_SNAPSHOT,
        [
            {
                "snapshot_date": day,
                "cik": cik,
                "name": f"Company {cik}{suffix}",
                "tickers": [f"T{cik}", f"T{cik}W"] if cik == META else [f"T{cik}"],
                "exchanges": ["Nasdaq"] if cik != MATTEL else [],
                "primary_ticker": f"T{cik}",
                "primary_exchange": "Nasdaq" if cik != MATTEL else None,
                "sic": "7370",
                "attr_hash": f"{cik:016x}",
                "attributes_as_of": day,
                "is_stale": False,
                "is_inactive": False,
            }
            for cik in ciks
        ],
    )


def former_names(ciks: list[int]) -> pa.Table:
    return table(
        SEC_COMPANY_FORMER_NAME,
        [
            {
                "cik": cik,
                "former_name": f"Old Name {cik}",
                "valid_from": date(2005, 5, 6),
                "valid_to": date(2021, 10, 27) if cik == META else None,
            }
            for cik in ciks
        ],
    )


def index(day: date, ciks: list[int]) -> pa.Table:
    return table(
        SEC_FILING_INDEX,
        [
            {
                "index_date": day,
                "accession_number": f"{cik:010d}-26-{line:06d}",
                "cik": cik,
                "form": "8-K",
                "company_name": f"Company {cik}",
                "filing_date": day,
                "file_name": f"edgar/data/{cik}/{cik:010d}-26-{line:06d}.txt",
                "line_number": line,
            }
            for line, cik in enumerate(ciks, start=1)
        ],
    )


def detail(ciks: list[int], *, filings: int = 2, form: str = "8-K") -> pa.Table:
    return table(
        SEC_FILING_DETAIL,
        [
            {
                "cik": cik,
                "accession_number": f"{cik:010d}-26-{number:06d}",
                "form": form,
                "filing_date": DAY,
                "report_date": DAY if number % 2 else None,
                "acceptance_ts": datetime(2026, 11, 25, 21, 54, 12, tzinfo=UTC),
                "items": ["2.02", "9.01"] if number % 2 else [],
                "size_bytes": 1000 + number,
                "is_xbrl": True,
                "is_xbrl_numeric": None,
                "primary_document": "doc.htm",
            }
            for cik in ciks
            for number in range(1, filings + 1)
        ],
    )


def facts(ciks: list[int], *, count: int = 3, value: str = "1.5") -> pa.Table:
    return table(
        SEC_XBRL_FACT,
        [
            {
                "cik": cik,
                "taxonomy": "us-gaap",
                "concept": f"Concept{number}",
                "unit": "USD",
                "period_start": date(2026, 1, 1) if number % 2 else DAY,
                "period_end": DAY,
                "accession_number": f"{cik:010d}-26-000001",
                "is_instant": number % 2 == 0,
                "value": Decimal(value) + number,
                "value_text": str(Decimal(value) + number),
                "fiscal_year": 2026,
                "fiscal_period": "Q3" if number % 2 else None,
                "form": "10-Q",
                "filed_date": DAY,
            }
            for cik in ciks
            for number in range(1, count + 1)
        ],
    )
