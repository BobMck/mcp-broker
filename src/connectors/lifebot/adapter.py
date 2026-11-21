"""
Lifebot MCP Connector

Native connector wrapping the Lifebot read-only agent API (compact, pre-computed
summaries of the owner's personal health data). auth_mode="none": the credential
is a static agent key supplied through the broker environment
(`LIFEBOT_AGENT_KEY`); it is sent only in the `x-lifebot-agent-key` request
header — never logged, never returned in a tool result or error message.

Least-privilege by design (mirrors the gcp/tailscale/gitlab connectors):
- read only: daily brief, metric time series, workouts, goals. The upstream API
  is GET-only, so there are no write tools.
- API pinned from env (`LIFEBOT_API_URL`, https:// only).
"""

from __future__ import annotations

import datetime
import json
import os
import re
from typing import Any

import httpx

from broker.connectors.native import NativeConnector, NativeToolMeta, native_tool
from broker.models.connector_config import ConnectorMeta

# === CONSTANTS ===

_AGENT_HEADER = "x-lifebot-agent-key"

_HTTP_TIMEOUT = 30.0
_HTTP_BAD_REQUEST = 400
_HTTP_UNAUTHORIZED = 401
_HTTP_RATE_LIMITED = 429
_HTTP_UNAVAILABLE = 503
_HTTP_ERROR_FLOOR = 400

_DETAIL_MAX_CHARS = 200

_DAYS_MIN = 1
_DAYS_MAX = 365
_NAMES_MAX = 10

# ASCII-only: a bare \d also matches non-ASCII Unicode digits. The patterns also
# keep anything but plain tokens out of the query string.
_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}", re.ASCII)
_METRIC_NAME_RE = re.compile(r"[a-z0-9_]{1,64}")
# https://host[:port][/base/path] — no whitespace, query or fragment.
_API_URL_RE = re.compile(r"https://[^\s/?#]+(/[^\s?#]*)?")


# === ENV (read at call time so tests can monkeypatch) ===


def _api_base() -> str:
    url = os.environ.get("LIFEBOT_API_URL", "")
    if not url:
        raise ValueError("Lifebot API URL is not configured on the broker")
    url = url.rstrip("/")
    if not _API_URL_RE.fullmatch(url):
        raise ValueError("LIFEBOT_API_URL on the broker must be an https:// URL")
    return url


def _agent_key() -> str:
    key = os.environ.get("LIFEBOT_AGENT_KEY", "")
    if not key:
        raise ValueError("Lifebot agent key is not configured on the broker")
    return key


# === VALIDATION ===


def _validate_date(date: str) -> str:
    if not isinstance(date, str) or not _DATE_RE.fullmatch(date):
        raise ValueError(f"Invalid date: {date!r} (expected YYYY-MM-DD)")
    try:
        datetime.date.fromisoformat(date)
    except ValueError:
        raise ValueError(f"Invalid date: {date!r} (not a real calendar date)") from None
    return date


def _validate_days(days: int) -> int:
    # bool is an int subclass — reject it explicitly so True doesn't mean 1.
    if isinstance(days, bool) or not isinstance(days, int) or not _DAYS_MIN <= days <= _DAYS_MAX:
        raise ValueError(f"Invalid days: {days!r} (expected an integer {_DAYS_MIN}-{_DAYS_MAX})")
    return days


def _validate_detail(detail: bool) -> bool:
    if not isinstance(detail, bool):
        raise ValueError(f"Invalid detail: {detail!r} (expected true or false)")
    return detail


def _validate_names(names: list[str] | str) -> str:
    """Return the validated, de-duplicated names as the API's comma-separated string."""
    if isinstance(names, str):
        # Strip spaces around comma-separated pieces only, so "a, b" works but a
        # stray newline is still rejected by the pattern below.
        items = [part.strip(" ") for part in names.split(",")]
    elif isinstance(names, list):
        items = names
    else:
        raise ValueError(f"Invalid names: {names!r} (expected a list or comma-separated string)")
    for name in items:
        if not isinstance(name, str) or not _METRIC_NAME_RE.fullmatch(name):
            raise ValueError(f"Invalid metric name: {name!r} (expected [a-z0-9_], 1-64 chars)")
    unique = list(dict.fromkeys(items))
    if not 1 <= len(unique) <= _NAMES_MAX:
        raise ValueError(f"Invalid names: provide 1-{_NAMES_MAX} distinct metric names")
    return ",".join(unique)


# === HTTP HELPERS ===


def _bad_request_detail(response: Any) -> str:
    """The contract's `detail` string for a 400 — nothing else from the body."""
    try:
        detail = response.json().get("detail")
    except Exception:  # noqa: BLE001 -- any parse failure means no detail available
        return "no detail"
    if not isinstance(detail, str) or not detail:
        return "no detail"
    return detail[:_DETAIL_MAX_CHARS]


def _raise_for_status(response: Any) -> None:
    """Map upstream errors to short messages — no URLs, bodies, or the agent key."""
    status = response.status_code
    if status < _HTTP_ERROR_FLOOR:
        return
    if status == _HTTP_UNAUTHORIZED:
        raise ValueError(
            "Lifebot rejected the broker's agent key (401) — operator: check LIFEBOT_AGENT_KEY"
        )
    if status == _HTTP_UNAVAILABLE:
        raise ValueError("Lifebot's agent key is not configured on the Lifebot side (503)")
    if status == _HTTP_RATE_LIMITED:
        raise ValueError("Lifebot API rate limited (429) — retry later")
    if status == _HTTP_BAD_REQUEST:
        raise ValueError(f"Lifebot rejected a parameter (400): {_bad_request_detail(response)}")
    raise ValueError(f"Lifebot API error {status}")


async def _lifebot_get(path: str, params: dict[str, str] | None = None) -> Any:
    key = _agent_key()
    base = _api_base()
    try:
        async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
            response = await client.get(
                f"{base}{path}",
                headers={_AGENT_HEADER: key},
                params=params or {},
            )
    except httpx.HTTPError:
        # Transport errors can embed the request URL — report none of it.
        raise ValueError("Lifebot API is unreachable (network error or timeout)") from None
    _raise_for_status(response)
    try:
        return response.json()
    except ValueError:
        raise ValueError("Lifebot API returned an invalid response") from None


def _mcp_text_content(payload: Any) -> list[dict[str, Any]]:
    return [{"type": "text", "text": json.dumps(payload, default=str)}]


# === TOOL METADATA ===

_PRIVACY_NOTE = (
    "This is the owner's personal health data. Dates are the owner's local calendar "
    "dates (YYYY-MM-DD). A missing field means there is no data for it, not zero."
)

_DAYS_SCHEMA = {
    "type": "integer",
    "minimum": _DAYS_MIN,
    "maximum": _DAYS_MAX,
}

_GET_BRIEF_META = NativeToolMeta(
    name="get_brief",
    description=(
        "Get a compact daily health summary for the owner: data freshness per source "
        "(so you can tell when data is stale), core metrics (steps, active energy, "
        "resting heart rate, HRV, VO2 max, body weight/composition, calories, protein) "
        "with 7-day and 30-day averages and progress toward goals, last night's sleep, "
        "a training rollup (sessions, days since last session, weekly volume), recent "
        "personal records and notable changes. This is the right first call for a daily "
        "briefing; use get_metrics, get_workouts or get_goals only to drill down. " + _PRIVACY_NOTE
    ),
    input_schema={
        "type": "object",
        "properties": {
            "date": {
                "type": "string",
                "description": (
                    "Brief date, YYYY-MM-DD, a real date not in the future "
                    "(default: today, owner's local date)."
                ),
            },
        },
    },
)

_GET_METRICS_META = NativeToolMeta(
    name="get_metrics",
    description=(
        "Get daily time series for specific health metrics, oldest first, ending today "
        "(today's in-progress value is included as stored). Each metric has a unit and "
        "a list of dated points. Names that returned no data are listed under "
        "'unknown' (misspelt or simply no data). Metric names are the stored names, "
        "e.g. step_count, resting_heart_rate, body_mass; get_brief shows which exist. "
        + _PRIVACY_NOTE
    ),
    input_schema={
        "type": "object",
        "properties": {
            "names": {
                "anyOf": [
                    {
                        "type": "array",
                        "items": {"type": "string"},
                        "minItems": 1,
                        "maxItems": _NAMES_MAX,
                    },
                    {"type": "string"},
                ],
                "description": (
                    f"1-{_NAMES_MAX} metric names (lowercase letters, digits, underscore; "
                    "max 64 chars each), as a list or a comma-separated string."
                ),
            },
            "days": {
                **_DAYS_SCHEMA,
                "description": (
                    f"Window length in days, {_DAYS_MIN}-{_DAYS_MAX}, ending today (default 30)."
                ),
            },
        },
        "required": ["names"],
    },
)

_GET_WORKOUTS_META = NativeToolMeta(
    name="get_workouts",
    description=(
        "List the owner's recent workouts, newest first: date, source (hevy or "
        "health), title, duration, and for strength sessions exercise count, set "
        "count and total volume in kg. With detail=true, strength sessions also list "
        "each exercise's sets (weight, reps, set type); large responses drop older "
        "sessions' detail and set detail_truncated. " + _PRIVACY_NOTE
    ),
    input_schema={
        "type": "object",
        "properties": {
            "days": {
                **_DAYS_SCHEMA,
                "description": (
                    f"Window length in days, {_DAYS_MIN}-{_DAYS_MAX}, ending today (default 14)."
                ),
            },
            "detail": {
                "type": "boolean",
                "description": "Include per-exercise set detail (default false).",
            },
        },
    },
)

_GET_GOALS_META = NativeToolMeta(
    name="get_goals",
    description=(
        "List the owner's stored health goals, sorted by metric: metric name, target "
        "value, type ('daily' = reach it every day, 'target' = reach it by a date) and "
        "optional target date. " + _PRIVACY_NOTE
    ),
    input_schema={"type": "object", "properties": {}},
)


# === CONNECTOR ===


class LifebotConnector(NativeConnector):
    """Lifebot native connector — read-only summaries of the owner's health data."""

    meta = ConnectorMeta(
        name="lifebot",
        display_name="Lifebot (health)",
        auth_mode="none",  # credential is a static agent key sourced from the broker env
    )

    @native_tool(_GET_BRIEF_META)
    async def get_brief(
        self, *, access_token: str = "", date: str | None = None
    ) -> list[dict[str, Any]]:
        params = {} if date is None else {"date": _validate_date(date)}
        return _mcp_text_content(await _lifebot_get("/agent/brief", params))

    @native_tool(_GET_METRICS_META)
    async def get_metrics(
        self, *, access_token: str = "", names: list[str] | str, days: int | None = None
    ) -> list[dict[str, Any]]:
        params = {"names": _validate_names(names)}
        if days is not None:
            params["days"] = str(_validate_days(days))
        return _mcp_text_content(await _lifebot_get("/agent/metrics", params))

    @native_tool(_GET_WORKOUTS_META)
    async def get_workouts(
        self, *, access_token: str = "", days: int | None = None, detail: bool | None = None
    ) -> list[dict[str, Any]]:
        params: dict[str, str] = {}
        if days is not None:
            params["days"] = str(_validate_days(days))
        if detail is not None:
            params["detail"] = "true" if _validate_detail(detail) else "false"
        return _mcp_text_content(await _lifebot_get("/agent/workouts", params))

    @native_tool(_GET_GOALS_META)
    async def get_goals(self, *, access_token: str = "") -> list[dict[str, Any]]:
        return _mcp_text_content(await _lifebot_get("/agent/goals"))
