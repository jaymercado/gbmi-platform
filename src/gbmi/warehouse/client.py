"""ClickHouse connection built from ``GBMI_CH_URL`` (blueprint 9.5)."""

from __future__ import annotations

from urllib.parse import unquote, urlsplit

import clickhouse_connect
from clickhouse_connect.driver.client import Client

from gbmi.config import Settings, get_settings


def connect(url: str | None = None, settings: Settings | None = None) -> Client:
    """Open a client for a URL such as ``http://user:password@clickhouse:8123``.

    Without an explicit URL the one in the settings is used.
    """
    if url is None:
        settings = settings or get_settings()
        if settings.ch_url is None:
            raise RuntimeError("GBMI_CH_URL is not set")
        url = settings.ch_url.get_secret_value()
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise ValueError("GBMI_CH_URL must look like http://user:password@host:8123")
    secure = parts.scheme == "https"
    return clickhouse_connect.get_client(
        host=parts.hostname,
        port=parts.port or (8443 if secure else 8123),
        username=unquote(parts.username or "default"),
        password=unquote(parts.password or ""),
        secure=secure,
    )


def quote_string(value: str) -> str:
    """A ClickHouse string literal."""
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def quote_identifier(value: str) -> str:
    """A ClickHouse identifier in backticks."""
    return "`" + value.replace("`", "``") + "`"
