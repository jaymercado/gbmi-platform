"""Provision Metabase through its REST API (week 1 plan, WP-09 step 8).

Metabase OSS has no file export for dashboards, so this script is the committed definition
of what the instance shows (week 1 plan, difference D10). It is idempotent: every run brings
the instance to the same state, whatever it finds.

    1. first-run setup: the administrator from METABASE_ADMIN_EMAIL / METABASE_ADMIN_PASSWORD
    2. a ClickHouse connection as the read-only user ``bi`` (role bi_ro: marts and ops only)
    3. one question: events by CAMEO root code, from the mart fct_gdelt_event_daily
    4. one dashboard with that chart and the GDELT attribution (blueprint 4.7)

Run on the Compose network:
    make toolbox CMD="python -m monitoring.metabase.provision"
"""

from __future__ import annotations

import json
import os
import time
from typing import Any

import httpx

DATABASE_NAME = "GBMI warehouse"
CARD_NAME = "GDELT events by CAMEO root code"
DASHBOARD_NAME = "GDELT events (tracer)"

CARD_QUERY = """\
SELECT event_root_code, sum(events) AS events
FROM marts.fct_gdelt_event_daily
GROUP BY event_root_code
ORDER BY event_root_code"""

ATTRIBUTION = (
    "**Source:** [The GDELT Project](https://www.gdeltproject.org/). GDELT 2.0 Events, English "
    "feed. Events are counted by the UTC day on which GDELT published them and grouped by the "
    "root code of the CAMEO event taxonomy (01 make public statement ... 20 use unconventional "
    "mass violence). Only GDELT metadata is stored; no article text is fetched."
)


class MetabaseClient:
    """The few Metabase API calls the platform needs."""

    def __init__(
        self, base_url: str | None = None, email: str | None = None, password: str | None = None
    ) -> None:
        self.base_url = (base_url or os.environ.get("METABASE_URL", "http://metabase:3000")).rstrip(
            "/"
        )
        self.email = email or os.environ.get("METABASE_ADMIN_EMAIL", "admin@example.com")
        self._password = password or os.environ["METABASE_ADMIN_PASSWORD"]
        self._http = httpx.Client(base_url=self.base_url, timeout=120)

    # --- session ------------------------------------------------------------------------

    def wait_until_healthy(self, timeout: float = 300) -> None:
        deadline = time.monotonic() + timeout
        while True:
            try:
                if self._http.get("/api/health").json().get("status") == "ok":
                    return
            except (httpx.HTTPError, ValueError):
                pass
            if time.monotonic() > deadline:
                raise TimeoutError(
                    f"Metabase at {self.base_url} is not healthy after {timeout:.0f} s"
                )
            time.sleep(3)

    def set_up_and_log_in(self) -> None:
        """Create the administrator on a fresh instance, then open a session."""
        token = self._http.get("/api/session/properties").json().get("setup-token")
        if token:
            self._http.post(
                "/api/setup",
                json={
                    "token": token,
                    "user": {
                        "email": self.email,
                        "password": self._password,
                        "first_name": "GBMI",
                        "last_name": "Admin",
                    },
                    "prefs": {"site_name": "GBMI Platform", "site_locale": "en"},
                },
            ).raise_for_status()
        response = self._http.post(
            "/api/session", json={"username": self.email, "password": self._password}
        )
        response.raise_for_status()
        self._http.headers["X-Metabase-Session"] = response.json()["id"]

    def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        response = self._http.request(method, f"/api{path}", **kwargs)
        if response.status_code >= 400:
            raise RuntimeError(
                f"Metabase {method} {path}: HTTP {response.status_code}: {response.text[:500]}"
            )
        return response.json() if response.content else None

    # --- database -----------------------------------------------------------------------

    def ensure_database(self, details: dict[str, Any]) -> int:
        existing = [
            database
            for database in self._request("GET", "/database")["data"]
            if database["name"] == DATABASE_NAME
        ]
        body = {
            "engine": "clickhouse",
            "name": DATABASE_NAME,
            "details": details,
            "is_full_sync": True,
        }
        if existing:
            database_id = int(existing[0]["id"])
            self._request("PUT", f"/database/{database_id}", json=body)
        else:
            database_id = int(self._request("POST", "/database", json=body)["id"])
        return database_id

    def sync_until_table_is_known(self, database_id: int, table: str, timeout: float = 180) -> None:
        """Metabase learns about a new table only when it syncs the schema."""
        deadline = time.monotonic() + timeout
        while True:
            self._request("POST", f"/database/{database_id}/sync_schema")
            metadata = self._request("GET", f"/database/{database_id}/metadata")
            if any(known["name"] == table for known in metadata.get("tables", [])):
                return
            if time.monotonic() > deadline:
                raise TimeoutError(f"Metabase does not see table {table!r} after {timeout:.0f} s")
            time.sleep(3)

    # --- content ------------------------------------------------------------------------

    def find_card(self, name: str = CARD_NAME) -> int | None:
        cards = [
            card
            for card in self._request("GET", "/card")
            if card["name"] == name and not card.get("archived")
        ]
        return int(cards[0]["id"]) if cards else None

    def find_dashboard(self, name: str = DASHBOARD_NAME) -> int | None:
        dashboards = [
            dashboard
            for dashboard in self._request("GET", "/dashboard")
            if dashboard["name"] == name and not dashboard.get("archived")
        ]
        return int(dashboards[0]["id"]) if dashboards else None

    def ensure_card(self, database_id: int) -> int:
        body = {
            "name": CARD_NAME,
            "description": "GDELT events per CAMEO root code. Source: The GDELT Project.",
            "dataset_query": {
                "database": database_id,
                "type": "native",
                "native": {"query": CARD_QUERY},
            },
            "display": "bar",
            "visualization_settings": {
                "graph.dimensions": ["event_root_code"],
                "graph.metrics": ["events"],
                "graph.x_axis.title_text": "CAMEO root code",
                "graph.y_axis.title_text": "Events",
            },
            "collection_id": None,
        }
        card_id = self.find_card()
        if card_id is None:
            return int(self._request("POST", "/card", json=body)["id"])
        self._request("PUT", f"/card/{card_id}", json=body)
        return card_id

    def ensure_dashboard(self, card_id: int) -> int:
        dashboard_id = self.find_dashboard()
        if dashboard_id is None:
            created = self._request(
                "POST",
                "/dashboard",
                json={
                    "name": DASHBOARD_NAME,
                    "description": "Phase P0 tracer: one mart, one chart.",
                },
            )
            dashboard_id = int(created["id"])
        # The list is the whole desired layout; negative ids mean "new", and anything the
        # dashboard held before is dropped.
        dashcards = [
            {
                "id": -1,
                "card_id": card_id,
                "row": 0,
                "col": 0,
                "size_x": 24,
                "size_y": 8,
                "parameter_mappings": [],
                "visualization_settings": {},
            },
            {
                "id": -2,
                "card_id": None,
                "row": 8,
                "col": 0,
                "size_x": 24,
                "size_y": 3,
                "parameter_mappings": [],
                "visualization_settings": {
                    "virtual_card": {
                        "name": None,
                        "display": "text",
                        "visualization_settings": {},
                        "dataset_query": {},
                        "archived": False,
                    },
                    "text": ATTRIBUTION,
                },
            },
        ]
        self._request("PUT", f"/dashboard/{dashboard_id}", json={"dashcards": dashcards})
        return dashboard_id

    # --- reading ------------------------------------------------------------------------

    def card_rows(self, card_id: int) -> list[list[Any]]:
        """Run a saved question and return its rows."""
        result = self._request("POST", f"/card/{card_id}/query")
        if result.get("status") != "completed":
            raise RuntimeError(f"card {card_id} did not complete: {str(result.get('error'))[:500]}")
        rows: list[list[Any]] = result["data"]["rows"]
        return rows

    def dashboard(self, dashboard_id: int) -> dict[str, Any]:
        dashboard: dict[str, Any] = self._request("GET", f"/dashboard/{dashboard_id}")
        return dashboard


def provision(client: MetabaseClient | None = None) -> dict[str, Any]:
    """Bring Metabase to the committed state and return the ids of what it holds."""
    client = client or MetabaseClient()
    client.wait_until_healthy()
    client.set_up_and_log_in()
    database_id = client.ensure_database(
        {
            "host": os.environ.get("DBT_CH_HOST", "clickhouse"),
            "port": int(os.environ.get("DBT_CH_PORT", "8123")),
            "user": "bi",
            "password": os.environ["CH_BI_PASSWORD"],
            "dbname": "marts",
            "ssl": False,
        }
    )
    client.sync_until_table_is_known(database_id, "fct_gdelt_event_daily")
    card_id = client.ensure_card(database_id)
    dashboard_id = client.ensure_dashboard(card_id)
    return {
        "database_id": database_id,
        "card_id": card_id,
        "dashboard_id": dashboard_id,
        "dashboard_path": f"/dashboard/{dashboard_id}",
    }


def main() -> None:
    print(json.dumps(provision(), indent=2))


if __name__ == "__main__":
    main()
