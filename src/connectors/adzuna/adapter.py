"""
Adzuna MCP Connector

Native connector wrapping the Adzuna v1 Jobs REST API. auth_mode="none": the
credential is an `app_id` + `app_key` pair (Adzuna's free-tier auth) supplied through
the broker environment; both ride as query parameters on every request. Read-only — the
Adzuna API is a search/aggregation service with no write surface.

Least-privilege + hardened like the gcp/tailscale/gitlab connectors:
- read only: job search, salary histogram, salary history (comp trends), top employers,
  category list.
- default country pinned from env (`ADZUNA_COUNTRY`, default "gb"); an optional per-call
  `country` is validated against Adzuna's fixed allow-list before it enters the URL path.
- numeric params clamped; upstream errors sanitized. The request URL carries the
  app_key, so errors NEVER echo the URL — only Adzuna's message field.
"""

from __future__ import annotations

import json
import os
from typing import Any

import httpx

from broker.connectors.native import NativeConnector, NativeToolMeta, native_tool
from broker.models.connector_config import ConnectorMeta

# === CONSTANTS ===

_API = "https://api.adzuna.com/v1/api"

_HTTP_TIMEOUT = 30.0
_HTTP_ERROR_FLOOR = 400

_RESULTS_DEFAULT = 20
_RESULTS_MAX = 50
_MONTHS_DEFAULT = 12
_MONTHS_MAX = 24

# Adzuna's supported country codes (path segment). Validated to prevent path injection
# and to fail fast on typos.
_COUNTRIES = frozenset(
    {
        "at",
        "au",
        "be",
        "br",
        "ca",
        "ch",
        "de",
        "es",
        "fr",
        "gb",
        "in",
        "it",
        "mx",
        "nl",
        "nz",
        "pl",
        "sg",
        "us",
        "za",
    }
)


# === ENV (read at call time so tests can monkeypatch) ===


def _credentials() -> tuple[str, str]:
    app_id = os.environ.get("ADZUNA_APP_ID", "")
    app_key = os.environ.get("ADZUNA_APP_KEY", "")
    if not app_id or not app_key:
        raise ValueError("Adzuna app_id/app_key are not configured on the broker")
    return app_id, app_key


def _default_country() -> str:
    return os.environ.get("ADZUNA_COUNTRY", "gb").lower()


# === VALIDATION ===


def _validate_country(country: str | None) -> str:
    c = (country or _default_country()).lower()
    if c not in _COUNTRIES:
        raise ValueError(f"Unsupported country {c!r} (allowed: {', '.join(sorted(_COUNTRIES))})")
    return c


def _clamp(value: Any, lo: int, hi: int, label: str) -> int:
    try:
        n = int(value)
    except (TypeError, ValueError):
        raise ValueError(f"Invalid {label}: {value!r}") from None
    return max(lo, min(n, hi))


# === HTTP ===


def _error_message(response: Any) -> str:
    """Adzuna's message only — never the URL (it carries app_key) or the body."""
    try:
        body = response.json()
    except Exception:  # noqa: BLE001 -- any parse failure means no detail available
        return "no detail"
    if isinstance(body, dict):
        return str(body.get("display") or body.get("exception") or body.get("error") or "no detail")
    return "no detail"


async def _get(path: str, params: dict[str, Any] | None = None) -> Any:
    app_id, app_key = _credentials()
    query: dict[str, Any] = {"app_id": app_id, "app_key": app_key}
    # Drop None/empty optional params so we don't send blank filters.
    for k, v in (params or {}).items():
        if v is not None and v != "":
            query[k] = v
    async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
        response = await client.get(f"{_API}{path}", params=query)
    if response.status_code >= _HTTP_ERROR_FLOOR:
        # Deliberately no URL in the message — it contains the app_key.
        raise ValueError(f"Adzuna API error {response.status_code}: {_error_message(response)}")
    if not response.content:
        return {}
    return response.json()


# === SERIALIZATION ===


def _summarize_job(j: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": j.get("id"),
        "title": j.get("title"),
        "company": (j.get("company") or {}).get("display_name"),
        "location": (j.get("location") or {}).get("display_name"),
        "category": (j.get("category") or {}).get("label"),
        "salary_min": j.get("salary_min"),
        "salary_max": j.get("salary_max"),
        "contract_type": j.get("contract_type"),
        "created": j.get("created"),
        "url": j.get("redirect_url"),
    }


def _mcp_text_content(payload: Any) -> list[dict[str, Any]]:
    return [{"type": "text", "text": json.dumps(payload, default=str)}]


# === TOOL METADATA ===

_COUNTRY_SCHEMA = {
    "type": "string",
    "description": "2-letter Adzuna country code (e.g. gb, us). Defaults to the broker's pinned country.",
}
_WHAT_SCHEMA = {
    "type": "string",
    "description": "Keywords to match in the job title/description, e.g. 'python engineer'.",
}
_WHERE_SCHEMA = {"type": "string", "description": "Location, e.g. 'London' or 'remote'."}
_CATEGORY_SCHEMA = {
    "type": "string",
    "description": "Adzuna category tag, e.g. 'it-jobs' (see list_categories).",
}

_SEARCH_JOBS_META = NativeToolMeta(
    name="search_jobs",
    description=(
        "Search live job listings aggregated across many boards. Returns title, company, "
        "location, salary range, category, and a link per result."
    ),
    input_schema={
        "type": "object",
        "properties": {
            "what": _WHAT_SCHEMA,
            "where": _WHERE_SCHEMA,
            "category": _CATEGORY_SCHEMA,
            "salary_min": {"type": "integer", "description": "Minimum annual salary filter."},
            "results_per_page": {
                "type": "integer",
                "description": f"1-{_RESULTS_MAX} (default {_RESULTS_DEFAULT}).",
            },
            "page": {"type": "integer", "description": "1-based page number (default 1)."},
            "country": _COUNTRY_SCHEMA,
        },
    },
)

_SALARY_HISTOGRAM_META = NativeToolMeta(
    name="salary_histogram",
    description=(
        "Salary distribution (histogram of salary bands → number of postings) for a query "
        "— the comp-benchmarking snapshot for a role/location right now."
    ),
    input_schema={
        "type": "object",
        "properties": {
            "what": _WHAT_SCHEMA,
            "where": _WHERE_SCHEMA,
            "category": _CATEGORY_SCHEMA,
            "country": _COUNTRY_SCHEMA,
        },
    },
)

_SALARY_HISTORY_META = NativeToolMeta(
    name="salary_history",
    description=(
        "Average salary over time (month → mean salary) for a query — comp trend / history "
        "for benchmarking how pay for a role has moved."
    ),
    input_schema={
        "type": "object",
        "properties": {
            "what": _WHAT_SCHEMA,
            "where": _WHERE_SCHEMA,
            "category": _CATEGORY_SCHEMA,
            "months": {
                "type": "integer",
                "description": f"How many months back, 1-{_MONTHS_MAX} (default {_MONTHS_DEFAULT}).",
            },
            "country": _COUNTRY_SCHEMA,
        },
    },
)

_TOP_COMPANIES_META = NativeToolMeta(
    name="top_companies",
    description="Top employers hiring for a query (company → posting count / avg salary) — employer research.",
    input_schema={
        "type": "object",
        "properties": {
            "what": _WHAT_SCHEMA,
            "where": _WHERE_SCHEMA,
            "category": _CATEGORY_SCHEMA,
            "country": _COUNTRY_SCHEMA,
        },
    },
)

_LIST_CATEGORIES_META = NativeToolMeta(
    name="list_categories",
    description="List Adzuna job categories (tag + label), e.g. 'it-jobs' — use a tag to filter the other tools.",
    input_schema={
        "type": "object",
        "properties": {"country": _COUNTRY_SCHEMA},
    },
)


# === CONNECTOR ===


class AdzunaConnector(NativeConnector):
    """Adzuna native connector — read-only job search, salary trends, employer research."""

    meta = ConnectorMeta(
        name="adzuna",
        display_name="Adzuna (jobs)",
        auth_mode="none",  # credential is app_id/app_key from the broker env
    )

    @native_tool(_SEARCH_JOBS_META)
    async def search_jobs(  # noqa: PLR0913 -- Adzuna search exposes many optional filters
        self,
        *,
        access_token: str = "",
        what: str = "",
        where: str = "",
        category: str = "",
        salary_min: int | None = None,
        results_per_page: int = _RESULTS_DEFAULT,
        page: int = 1,
        country: str | None = None,
    ) -> list[dict[str, Any]]:
        c = _validate_country(country)
        pg = _clamp(page, 1, 1000, "page")
        params = {
            "what": what,
            "where": where,
            "category": category,
            "results_per_page": _clamp(results_per_page, 1, _RESULTS_MAX, "results_per_page"),
            "sort_by": "date",
        }
        if salary_min is not None:
            params["salary_min"] = _clamp(salary_min, 0, 10_000_000, "salary_min")
        data = await _get(f"/jobs/{c}/search/{pg}", params)
        results = [_summarize_job(j) for j in data.get("results", [])]
        return _mcp_text_content({"count": data.get("count"), "results": results})

    @native_tool(_SALARY_HISTOGRAM_META)
    async def salary_histogram(  # noqa: PLR0913 -- many optional search filters
        self,
        *,
        access_token: str = "",
        what: str = "",
        where: str = "",
        category: str = "",
        country: str | None = None,
    ) -> list[dict[str, Any]]:
        c = _validate_country(country)
        data = await _get(
            f"/jobs/{c}/histogram", {"what": what, "where": where, "category": category}
        )
        return _mcp_text_content(data.get("histogram", data))

    @native_tool(_SALARY_HISTORY_META)
    async def salary_history(  # noqa: PLR0913 -- many optional search filters
        self,
        *,
        access_token: str = "",
        what: str = "",
        where: str = "",
        category: str = "",
        months: int = _MONTHS_DEFAULT,
        country: str | None = None,
    ) -> list[dict[str, Any]]:
        c = _validate_country(country)
        params = {
            "what": what,
            "where": where,
            "category": category,
            "months": _clamp(months, 1, _MONTHS_MAX, "months"),
        }
        data = await _get(f"/jobs/{c}/history", params)
        return _mcp_text_content(data.get("month", data))

    @native_tool(_TOP_COMPANIES_META)
    async def top_companies(  # noqa: PLR0913 -- many optional search filters
        self,
        *,
        access_token: str = "",
        what: str = "",
        where: str = "",
        category: str = "",
        country: str | None = None,
    ) -> list[dict[str, Any]]:
        c = _validate_country(country)
        data = await _get(
            f"/jobs/{c}/top_companies", {"what": what, "where": where, "category": category}
        )
        return _mcp_text_content(data.get("leaderboard", data))

    @native_tool(_LIST_CATEGORIES_META)
    async def list_categories(
        self, *, access_token: str = "", country: str | None = None
    ) -> list[dict[str, Any]]:
        c = _validate_country(country)
        data = await _get(f"/jobs/{c}/categories")
        cats = [{"tag": r.get("tag"), "label": r.get("label")} for r in data.get("results", [])]
        return _mcp_text_content(cats)
