"""
GCP MCP Connector

Native connector wrapping the Compute Engine + Cloud Logging REST APIs for the
broker's own project. auth_mode="none": the credential is a short-lived access
token from the GCE metadata server (the VM's attached service account) — no key
file exists, and the broker token store is not involved.

Least-privilege by design (spec: gcp-mcp-standalone
docs/superpowers/specs/2026-07-06-gcp-tailscale-connectors-design.md):
- read: list/get instances, serial output, log entries
- write: stop/start/reset instance ONLY
- project + zone pinned from env (GCP_PROJECT / GCP_ZONE), never tool params
"""

from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from broker.connectors.native import NativeConnector, NativeToolMeta, native_tool
from broker.models.connector_config import ConnectorMeta

# === CONSTANTS ===

# Link-local metadata address, not metadata.google.internal: the broker runs in
# a docker bridge network where the hostname may not resolve; the IP always routes.
_METADATA_TOKEN_URL = (
    "http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token"
)
_COMPUTE = "https://compute.googleapis.com/compute/v1"
_LOGGING_URL = "https://logging.googleapis.com/v2/entries:list"

_HTTP_TIMEOUT = 30.0
_TOKEN_SAFETY_WINDOW = 60  # seconds of remaining life below which we refresh
_MAX_LOG_ENTRIES = 50
_MAX_LOG_HOURS = 168  # 7 days
_SERIAL_TAIL_CHARS = 20_000
_PAYLOAD_SNIPPET_CHARS = 500
_HTTP_OK = 200
_HTTP_ERROR_FLOOR = 400

# GCE instance-name charset — also prevents path injection into the request URL.
_INSTANCE_NAME_RE = re.compile(r"^[a-z]([-a-z0-9]{0,61}[a-z0-9])?$")


# === ENV (read at call time so tests can monkeypatch) ===


def _project() -> str:
    project = os.environ.get("GCP_PROJECT", "")
    if not project:
        raise ValueError("GCP_PROJECT is not configured on the broker")
    return project


def _zone() -> str:
    return os.environ.get("GCP_ZONE", "europe-west2-a")


def _validate_instance(name: str) -> str:
    if not isinstance(name, str) or not _INSTANCE_NAME_RE.match(name):
        raise ValueError(f"Invalid instance name: {name!r}")
    return name


# === TOKEN (metadata server, cached in-process) ===

_token_cache: dict[str, Any] = {"token": "", "expires_at": 0.0}


async def _metadata_token() -> str:
    """Short-lived access token for the VM's service account."""
    if _token_cache["token"] and time.time() < _token_cache["expires_at"] - _TOKEN_SAFETY_WINDOW:
        return _token_cache["token"]
    async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
        response = await client.get(_METADATA_TOKEN_URL, headers={"Metadata-Flavor": "Google"})
    if response.status_code != _HTTP_OK:
        raise ValueError(f"Metadata server refused a token (HTTP {response.status_code})")
    payload = response.json()
    _token_cache["token"] = payload["access_token"]
    _token_cache["expires_at"] = time.time() + float(payload.get("expires_in", 0))
    return _token_cache["token"]


# === HTTP HELPERS ===


def _error_message(response: Any) -> str:
    """GCP's human-readable error message only — no URLs, bodies, or tokens."""
    try:
        return response.json().get("error", {}).get("message", "no detail")
    except Exception:  # noqa: BLE001 -- any parse failure means no detail available
        return "no detail"


async def _gcp_request(
    method: str,
    url: str,
    json_body: dict | None = None,
    params: dict | None = None,
) -> dict[str, Any]:
    token = await _metadata_token()
    async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
        response = await client.request(
            method,
            url,
            headers={"Authorization": f"Bearer {token}"},
            json=json_body,
            params=params,
        )
    if response.status_code >= _HTTP_ERROR_FLOOR:
        raise ValueError(f"GCP API error {response.status_code}: {_error_message(response)}")
    return response.json()


# === SERIALIZATION ===


def _summarize_instance(instance: dict[str, Any]) -> dict[str, Any]:
    network = (instance.get("networkInterfaces") or [{}])[0]
    access = (network.get("accessConfigs") or [{}])[0]
    return {
        "name": instance.get("name"),
        "status": instance.get("status"),
        "machine_type": (instance.get("machineType") or "").rsplit("/", 1)[-1],
        "zone": (instance.get("zone") or "").rsplit("/", 1)[-1],
        "internal_ip": network.get("networkIP"),
        "external_ip": access.get("natIP"),
        "last_start": instance.get("lastStartTimestamp"),
    }


def _summarize_log_entry(entry: dict[str, Any]) -> dict[str, Any]:
    payload = entry.get("textPayload")
    if payload is None:
        payload = json.dumps(
            entry.get("jsonPayload") or entry.get("protoPayload") or {}, default=str
        )
    return {
        "timestamp": entry.get("timestamp"),
        "severity": entry.get("severity"),
        "log": (entry.get("logName") or "").rsplit("/", 1)[-1],
        "payload": payload[:_PAYLOAD_SNIPPET_CHARS],
    }


def _mcp_text_content(payload: Any) -> list[dict[str, Any]]:
    return [{"type": "text", "text": json.dumps(payload, default=str)}]


# === TOOL METADATA ===

_LIST_INSTANCES_META = NativeToolMeta(
    name="list_instances",
    description=(
        "List Compute Engine instances in the broker's own GCP project/zone: name, "
        "status (RUNNING/TERMINATED/...), machine type, internal and external IPs."
    ),
    input_schema={"type": "object", "properties": {}},
)

_GET_INSTANCE_META = NativeToolMeta(
    name="get_instance",
    description="Get one Compute Engine instance's summary (status, IPs, last start time) by name.",
    input_schema={
        "type": "object",
        "properties": {
            "instance": {"type": "string", "description": "Instance name, e.g. gcp-mcp-broker"},
        },
        "required": ["instance"],
    },
)

_GET_SERIAL_OUTPUT_META = NativeToolMeta(
    name="get_serial_output",
    description=(
        "Read an instance's serial console output (last ~20k chars) — the no-SSH "
        "diagnostic path when a VM is unreachable or misbehaving at boot."
    ),
    input_schema={
        "type": "object",
        "properties": {
            "instance": {"type": "string", "description": "Instance name"},
        },
        "required": ["instance"],
    },
)

_LIST_LOG_ENTRIES_META = NativeToolMeta(
    name="list_log_entries",
    description=(
        "Query recent Cloud Logging entries for the broker project, newest first. "
        "Optionally pass a Cloud Logging filter expression such as 'severity>=ERROR' "
        'or \'resource.type="gce_instance"\'.'
    ),
    input_schema={
        "type": "object",
        "properties": {
            "log_filter": {
                "type": "string",
                "description": "Optional Cloud Logging filter expression",
            },
            "hours": {
                "type": "integer",
                "description": "Look-back window in hours (1-168, default 1)",
                "default": 1,
            },
            "max_entries": {
                "type": "integer",
                "description": "Maximum entries to return (1-50, default 20)",
                "default": 20,
            },
        },
    },
)

_STOP_INSTANCE_META = NativeToolMeta(
    name="stop_instance",
    description=(
        "Stop (power off) a Compute Engine instance. The instance keeps its disk and "
        "can be started again with start_instance. Stopping the broker VM itself will "
        "drop this very connection until the VM is started by other means."
    ),
    input_schema={
        "type": "object",
        "properties": {
            "instance": {"type": "string", "description": "Instance name"},
        },
        "required": ["instance"],
    },
)

_START_INSTANCE_META = NativeToolMeta(
    name="start_instance",
    description="Start a stopped Compute Engine instance.",
    input_schema={
        "type": "object",
        "properties": {
            "instance": {"type": "string", "description": "Instance name"},
        },
        "required": ["instance"],
    },
)

_RESET_INSTANCE_META = NativeToolMeta(
    name="reset_instance",
    description=(
        "Hard-reset (power-cycle) a Compute Engine instance — like pulling the power "
        "cord. Use when the instance is hung; prefer stop/start otherwise."
    ),
    input_schema={
        "type": "object",
        "properties": {
            "instance": {"type": "string", "description": "Instance name"},
        },
        "required": ["instance"],
    },
)


# === POWER ACTION (shared by stop/start/reset) ===


async def _instance_power_action(action: str, instance: str) -> list[dict[str, Any]]:
    _validate_instance(instance)
    url = f"{_COMPUTE}/projects/{_project()}/zones/{_zone()}/instances/{instance}/{action}"
    operation = await _gcp_request("POST", url)
    return _mcp_text_content(
        {
            "instance": instance,
            "action": action,
            "operation_status": operation.get("status"),
            "operation_id": operation.get("name"),
        }
    )


# === CONNECTOR ===


class GcpConnector(NativeConnector):
    """GCP native connector — least-privilege ops on the broker's own project."""

    meta = ConnectorMeta(
        name="gcp",
        display_name="Google Cloud (broker project)",
        auth_mode="none",  # credential self-sourced from the GCE metadata server
    )

    @native_tool(_LIST_INSTANCES_META)
    async def list_instances(self, *, access_token: str = "") -> list[dict[str, Any]]:
        url = f"{_COMPUTE}/projects/{_project()}/zones/{_zone()}/instances"
        data = await _gcp_request("GET", url)
        return _mcp_text_content([_summarize_instance(item) for item in data.get("items", [])])

    @native_tool(_GET_INSTANCE_META)
    async def get_instance(self, *, access_token: str = "", instance: str) -> list[dict[str, Any]]:
        _validate_instance(instance)
        url = f"{_COMPUTE}/projects/{_project()}/zones/{_zone()}/instances/{instance}"
        data = await _gcp_request("GET", url)
        return _mcp_text_content(_summarize_instance(data))

    @native_tool(_GET_SERIAL_OUTPUT_META)
    async def get_serial_output(
        self, *, access_token: str = "", instance: str
    ) -> list[dict[str, Any]]:
        _validate_instance(instance)
        url = f"{_COMPUTE}/projects/{_project()}/zones/{_zone()}/instances/{instance}/serialPort"
        data = await _gcp_request("GET", url, params={"port": 1})
        contents = data.get("contents", "")
        return _mcp_text_content(
            {"instance": instance, "serial_output_tail": contents[-_SERIAL_TAIL_CHARS:]}
        )

    @native_tool(_LIST_LOG_ENTRIES_META)
    async def list_log_entries(
        self,
        *,
        access_token: str = "",
        log_filter: str = "",
        hours: int = 1,
        max_entries: int = 20,
    ) -> list[dict[str, Any]]:
        bounded_hours = max(1, min(int(hours), _MAX_LOG_HOURS))
        page_size = max(1, min(int(max_entries), _MAX_LOG_ENTRIES))
        cutoff = datetime.now(timezone.utc) - timedelta(hours=bounded_hours)
        time_filter = f'timestamp >= "{cutoff.isoformat()}"'
        full_filter = f"{time_filter} AND ({log_filter})" if log_filter else time_filter
        body = {
            "resourceNames": [f"projects/{_project()}"],
            "filter": full_filter,
            "orderBy": "timestamp desc",
            "pageSize": page_size,
        }
        data = await _gcp_request("POST", _LOGGING_URL, json_body=body)
        return _mcp_text_content([_summarize_log_entry(e) for e in data.get("entries", [])])

    @native_tool(_STOP_INSTANCE_META)
    async def stop_instance(self, *, access_token: str = "", instance: str) -> list[dict[str, Any]]:
        return await _instance_power_action("stop", instance)

    @native_tool(_START_INSTANCE_META)
    async def start_instance(
        self, *, access_token: str = "", instance: str
    ) -> list[dict[str, Any]]:
        return await _instance_power_action("start", instance)

    @native_tool(_RESET_INSTANCE_META)
    async def reset_instance(
        self, *, access_token: str = "", instance: str
    ) -> list[dict[str, Any]]:
        return await _instance_power_action("reset", instance)
