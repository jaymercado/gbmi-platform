"""DuckDB connections for routine processing."""

from __future__ import annotations

import duckdb


def connect(
    *, memory_limit: str = "1GB", temp_directory: str | None = None, threads: int = 2
) -> duckdb.DuckDBPyConnection:
    """An in-memory DuckDB with the session settings every transformation relies on.

    The session time zone is UTC. DuckDB hands TIMESTAMPTZ columns to Arrow in the session
    time zone, which follows the machine by default; without this setting the same code would
    write ``Asia/Manila`` timestamps on a laptop and UTC ones in a container (blueprint 7.11).

    With a spill directory DuckDB may exceed ``memory_limit`` by writing to disk, which keeps a
    large build inside the task-process budget (blueprint 9.1).
    """
    connection = duckdb.connect(":memory:")
    connection.execute("SET TimeZone = 'UTC'")
    connection.execute(f"SET memory_limit = '{memory_limit}'")
    connection.execute(f"SET threads = {int(threads)}")
    if temp_directory is not None:
        connection.execute(f"SET temp_directory = '{temp_directory}'")
    return connection
