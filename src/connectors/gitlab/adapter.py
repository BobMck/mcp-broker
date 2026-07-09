"""
GitLab MCP Connector

Native connector wrapping the GitLab v4 REST API. auth_mode="none": the
credential is a Personal Access Token (scope `read_api`) supplied through the
broker environment (`GITLAB_TOKEN`); it is sent as-is on each request — no token
exchange, no long-lived key stored by the connector itself.

Least-privilege by design (mirrors the gcp/tailscale connectors, spec:
gcp-mcp-standalone docs/superpowers/specs/2026-07-06-gcp-tailscale-connectors-design.md):
- read only: list projects, get a project, list a project's issues / merge
  requests / CI pipelines.
- No create/update/delete tools — writes are a deliberate later decision, not
  default-on. A `read_api`-scoped token cannot mutate anything even if asked.
- Instance pinned from env (`GITLAB_URL`, default https://gitlab.com).
"""

from __future__ import annotations

import json
import os
import re
from typing import Any
from urllib.parse import quote

import httpx

from broker.connectors.native import NativeConnector, NativeToolMeta, native_tool
from broker.models.connector_config import ConnectorMeta

# === CONSTANTS ===

_DEFAULT_URL = "https://gitlab.com"

_HTTP_TIMEOUT = 30.0
_HTTP_ERROR_FLOOR = 400

_PER_PAGE_DEFAULT = 20
_PER_PAGE_MAX = 100

# A project ref is either a numeric id or a namespace path ("group/sub/project").
# The pattern also blocks path/query injection into the request URL before we
# URL-encode it.
_PROJECT_REF_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]*$")

# GitLab enum params we accept, validated against a fixed allow-list.
_ISSUE_STATES = frozenset({"opened", "closed", "all"})
_MR_STATES = frozenset({"opened", "closed", "merged", "locked", "all"})


# === ENV (read at call time so tests can monkeypatch) ===


def _gitlab_token() -> str:
    token = os.environ.get("GITLAB_TOKEN", "")
    if not token:
        raise ValueError("GitLab token is not configured on the broker")
    return token


def _api_base() -> str:
    return f"{os.environ.get('GITLAB_URL', _DEFAULT_URL).rstrip('/')}/api/v4"


def _encode_project(project: str) -> str:
    """Validate then URL-encode a project id/path for use in a request path."""
    if not isinstance(project, str) or not _PROJECT_REF_RE.match(project):
        raise ValueError(f"Invalid project id or path: {project!r}")
    return quote(project, safe="")


def _clamp_per_page(per_page: int) -> int:
    try:
        value = int(per_page)
    except (TypeError, ValueError):
        raise ValueError(f"Invalid per_page: {per_page!r}") from None
    return max(1, min(value, _PER_PAGE_MAX))


def _validate_state(state: str, allowed: frozenset[str], label: str) -> str:
    if state not in allowed:
        raise ValueError(
            f"Invalid {label} state: {state!r} (allowed: {', '.join(sorted(allowed))})"
        )
    return state


# === HTTP HELPERS ===


def _error_message(response: Any) -> str:
    """GitLab's human-readable error only — no URLs, bodies, or the token."""
    try:
        body = response.json()
    except Exception:  # noqa: BLE001 -- any parse failure means no detail available
        return "no detail"
    # GitLab uses "message" for most errors and "error" for OAuth/scope failures.
    return str(body.get("message") or body.get("error") or "no detail")


async def _gl_request(path: str, params: dict[str, Any] | None = None) -> Any:
    token = _gitlab_token()
    async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
        response = await client.get(
            f"{_api_base()}{path}",
            headers={"PRIVATE-TOKEN": token},
            params=params or {},
        )
    if response.status_code >= _HTTP_ERROR_FLOOR:
        raise ValueError(f"GitLab API error {response.status_code}: {_error_message(response)}")
    if not response.content:
        return []
    return response.json()


# === SERIALIZATION ===


def _summarize_project(p: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": p.get("id"),
        "path": p.get("path_with_namespace"),
        "name": p.get("name"),
        "visibility": p.get("visibility"),
        "default_branch": p.get("default_branch"),
        "web_url": p.get("web_url"),
        "last_activity_at": p.get("last_activity_at"),
        "archived": p.get("archived"),
    }


def _summarize_issue(i: dict[str, Any]) -> dict[str, Any]:
    return {
        "iid": i.get("iid"),
        "title": i.get("title"),
        "state": i.get("state"),
        "author": (i.get("author") or {}).get("username"),
        "labels": i.get("labels"),
        "created_at": i.get("created_at"),
        "updated_at": i.get("updated_at"),
        "web_url": i.get("web_url"),
    }


def _summarize_mr(m: dict[str, Any]) -> dict[str, Any]:
    return {
        "iid": m.get("iid"),
        "title": m.get("title"),
        "state": m.get("state"),
        "author": (m.get("author") or {}).get("username"),
        "source_branch": m.get("source_branch"),
        "target_branch": m.get("target_branch"),
        "draft": m.get("draft"),
        "created_at": m.get("created_at"),
        "web_url": m.get("web_url"),
    }


def _summarize_pipeline(p: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": p.get("id"),
        "status": p.get("status"),
        "ref": p.get("ref"),
        "sha": p.get("sha"),
        "source": p.get("source"),
        "created_at": p.get("created_at"),
        "updated_at": p.get("updated_at"),
        "web_url": p.get("web_url"),
    }


def _mcp_text_content(payload: Any) -> list[dict[str, Any]]:
    return [{"type": "text", "text": json.dumps(payload, default=str)}]


# === TOOL METADATA ===

_PER_PAGE_SCHEMA = {
    "type": "integer",
    "description": f"Max results (1-{_PER_PAGE_MAX}, default {_PER_PAGE_DEFAULT}).",
}
_PROJECT_SCHEMA = {
    "type": "string",
    "description": "Project numeric id or full path, e.g. 12345 or group/subgroup/project.",
}

_LIST_PROJECTS_META = NativeToolMeta(
    name="list_projects",
    description=(
        "List GitLab projects the token can access (projects you are a member of), "
        "most recently active first: id, full path, visibility, default branch."
    ),
    input_schema={
        "type": "object",
        "properties": {
            "search": {"type": "string", "description": "Optional name/path substring filter."},
            "per_page": _PER_PAGE_SCHEMA,
        },
    },
)

_GET_PROJECT_META = NativeToolMeta(
    name="get_project",
    description="Get one project's summary by numeric id or full path.",
    input_schema={
        "type": "object",
        "properties": {"project": _PROJECT_SCHEMA},
        "required": ["project"],
    },
)

_LIST_ISSUES_META = NativeToolMeta(
    name="list_issues",
    description="List a project's issues (most recently updated first).",
    input_schema={
        "type": "object",
        "properties": {
            "project": _PROJECT_SCHEMA,
            "state": {
                "type": "string",
                "description": "opened | closed | all (default opened).",
            },
            "per_page": _PER_PAGE_SCHEMA,
        },
        "required": ["project"],
    },
)

_LIST_MRS_META = NativeToolMeta(
    name="list_merge_requests",
    description="List a project's merge requests (most recently updated first).",
    input_schema={
        "type": "object",
        "properties": {
            "project": _PROJECT_SCHEMA,
            "state": {
                "type": "string",
                "description": "opened | closed | merged | locked | all (default opened).",
            },
            "per_page": _PER_PAGE_SCHEMA,
        },
        "required": ["project"],
    },
)

_LIST_PIPELINES_META = NativeToolMeta(
    name="list_pipelines",
    description=(
        "List a project's recent CI/CD pipelines (newest first): status, ref, sha, "
        "source — the read path for checking whether CI is green."
    ),
    input_schema={
        "type": "object",
        "properties": {
            "project": _PROJECT_SCHEMA,
            "per_page": _PER_PAGE_SCHEMA,
        },
        "required": ["project"],
    },
)


# === CONNECTOR ===


class GitlabConnector(NativeConnector):
    """GitLab native connector — read-only project / issue / MR / pipeline visibility."""

    meta = ConnectorMeta(
        name="gitlab",
        display_name="GitLab",
        auth_mode="none",  # credential is a read_api PAT sourced from the broker env
    )

    @native_tool(_LIST_PROJECTS_META)
    async def list_projects(
        self, *, access_token: str = "", search: str = "", per_page: int = _PER_PAGE_DEFAULT
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "membership": "true",
            "order_by": "last_activity_at",
            "per_page": _clamp_per_page(per_page),
        }
        if search:
            params["search"] = search
        data = await _gl_request("/projects", params)
        return _mcp_text_content([_summarize_project(p) for p in data])

    @native_tool(_GET_PROJECT_META)
    async def get_project(self, *, access_token: str = "", project: str) -> list[dict[str, Any]]:
        data = await _gl_request(f"/projects/{_encode_project(project)}")
        return _mcp_text_content(_summarize_project(data))

    @native_tool(_LIST_ISSUES_META)
    async def list_issues(
        self,
        *,
        access_token: str = "",
        project: str,
        state: str = "opened",
        per_page: int = _PER_PAGE_DEFAULT,
    ) -> list[dict[str, Any]]:
        pid = _encode_project(project)
        params = {
            "state": _validate_state(state, _ISSUE_STATES, "issue"),
            "order_by": "updated_at",
            "per_page": _clamp_per_page(per_page),
        }
        data = await _gl_request(f"/projects/{pid}/issues", params)
        return _mcp_text_content([_summarize_issue(i) for i in data])

    @native_tool(_LIST_MRS_META)
    async def list_merge_requests(
        self,
        *,
        access_token: str = "",
        project: str,
        state: str = "opened",
        per_page: int = _PER_PAGE_DEFAULT,
    ) -> list[dict[str, Any]]:
        pid = _encode_project(project)
        params = {
            "state": _validate_state(state, _MR_STATES, "merge request"),
            "order_by": "updated_at",
            "per_page": _clamp_per_page(per_page),
        }
        data = await _gl_request(f"/projects/{pid}/merge_requests", params)
        return _mcp_text_content([_summarize_mr(m) for m in data])

    @native_tool(_LIST_PIPELINES_META)
    async def list_pipelines(
        self, *, access_token: str = "", project: str, per_page: int = _PER_PAGE_DEFAULT
    ) -> list[dict[str, Any]]:
        pid = _encode_project(project)
        params = {"order_by": "id", "sort": "desc", "per_page": _clamp_per_page(per_page)}
        data = await _gl_request(f"/projects/{pid}/pipelines", params)
        return _mcp_text_content([_summarize_pipeline(p) for p in data])
