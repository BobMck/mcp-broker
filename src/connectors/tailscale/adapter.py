"""
Tailscale MCP Connector

Native connector wrapping the Tailscale v2 REST API. auth_mode="none": the
credential is a scoped OAuth client (client-credentials grant) whose id/secret
come from the broker environment; short-lived access tokens are fetched on
demand — no long-lived Tailscale API key exists anywhere.

Least-privilege by design (spec: gcp-mcp-standalone
docs/superpowers/specs/2026-07-06-gcp-tailscale-connectors-design.md):
- read: list devices, get device
- write: expire a device's node key ONLY (force re-authentication)
- tailnet pinned from env (TAILSCALE_TAILNET, default "-" = the client's own)
"""

from __future__ import annotations

import json
import os
import re
import time
from typing import Any

import httpx

from broker.connectors.native import NativeConnector, NativeToolMeta, native_tool
from broker.models.connector_config import ConnectorMeta

# === CONSTANTS ===

_API = "https://api.tailscale.com/api/v2"
_TOKEN_URL = f"{_API}/oauth/token"  # noqa: S105 -- endpoint URL, not a password

_HTTP_TIMEOUT = 30.0
_TOKEN_SAFETY_WINDOW = 60  # seconds of remaining life below which we refresh
_HTTP_OK = 200
_HTTP_ERROR_FLOOR = 400

# Tailscale node ids are alphanumeric ("nABC..."); numeric legacy ids also occur.
# Also prevents path injection into the request URL.
_DEVICE_ID_RE = re.compile(r"^[A-Za-z0-9]+$")


# === ENV (read at call time so tests can monkeypatch) ===


def _client_credentials() -> tuple[str, str]:
    client_id = os.environ.get("TAILSCALE_OAUTH_CLIENT_ID", "")
    client_secret = os.environ.get("TAILSCALE_OAUTH_CLIENT_SECRET", "")
    if not client_id or not client_secret:
        raise ValueError("Tailscale OAuth client credentials are not configured on the broker")
    return client_id, client_secret


def _tailnet() -> str:
    # "-" is the Tailscale API alias for the tailnet the token belongs to.
    return os.environ.get("TAILSCALE_TAILNET", "-")


def _validate_device_id(device_id: str) -> str:
    if not isinstance(device_id, str) or not _DEVICE_ID_RE.match(device_id):
        raise ValueError(f"Invalid device id: {device_id!r}")
    return device_id


# === TOKEN (client-credentials grant, cached in-process) ===

_token_cache: dict[str, Any] = {"token": "", "expires_at": 0.0}


async def _tailscale_token() -> str:
    if _token_cache["token"] and time.time() < _token_cache["expires_at"] - _TOKEN_SAFETY_WINDOW:
        return _token_cache["token"]
    client_id, client_secret = _client_credentials()
    async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
        response = await client.post(
            _TOKEN_URL, data={"client_id": client_id, "client_secret": client_secret}
        )
    if response.status_code != _HTTP_OK:
        raise ValueError(f"Tailscale token request failed (HTTP {response.status_code})")
    payload = response.json()
    _token_cache["token"] = payload["access_token"]
    _token_cache["expires_at"] = time.time() + float(payload.get("expires_in", 0))
    return _token_cache["token"]


# === HTTP HELPERS ===


def _error_message(response: Any) -> str:
    """Tailscale's human-readable message only — no URLs, bodies, or tokens."""
    try:
        return response.json().get("message", "no detail")
    except Exception:  # noqa: BLE001 -- any parse failure means no detail available
        return "no detail"


async def _ts_request(method: str, path: str) -> Any:
    token = await _tailscale_token()
    async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
        response = await client.request(
            method, f"{_API}{path}", headers={"Authorization": f"Bearer {token}"}
        )
    if response.status_code >= _HTTP_ERROR_FLOOR:
        raise ValueError(f"Tailscale API error {response.status_code}: {_error_message(response)}")
    if not response.content:  # e.g. POST /device/{id}/expire returns an empty 200
        return {}
    return response.json()


# === SERIALIZATION ===


def _summarize_device(device: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": device.get("nodeId") or device.get("id"),
        "hostname": device.get("hostname"),
        "name": device.get("name"),
        "os": device.get("os"),
        "addresses": device.get("addresses"),
        "last_seen": device.get("lastSeen"),
        "key_expires": device.get("expires"),
        "key_expiry_disabled": device.get("keyExpiryDisabled"),
        "update_available": device.get("updateAvailable"),
    }


def _mcp_text_content(payload: Any) -> list[dict[str, Any]]:
    return [{"type": "text", "text": json.dumps(payload, default=str)}]


# === TOOL METADATA ===

_LIST_DEVICES_META = NativeToolMeta(
    name="list_devices",
    description=(
        "List all devices on the tailnet: hostname, OS, Tailscale IPs, last-seen "
        "time, and node-key expiry. A recent last_seen means the device is online."
    ),
    input_schema={"type": "object", "properties": {}},
)

_GET_DEVICE_META = NativeToolMeta(
    name="get_device",
    description="Get one tailnet device's summary by its device id (from list_devices).",
    input_schema={
        "type": "object",
        "properties": {
            "device_id": {"type": "string", "description": "Device id, e.g. nABC123"},
        },
        "required": ["device_id"],
    },
)

_EXPIRE_DEVICE_KEY_META = NativeToolMeta(
    name="expire_device_key",
    description=(
        "Expire a device's node key, forcing it to re-authenticate to rejoin the "
        "tailnet — the kill switch for a device that looks lost or compromised. "
        "The device loses tailnet connectivity until someone logs it in again."
    ),
    input_schema={
        "type": "object",
        "properties": {
            "device_id": {"type": "string", "description": "Device id, e.g. nABC123"},
        },
        "required": ["device_id"],
    },
)


# === CONNECTOR ===


class TailscaleConnector(NativeConnector):
    """Tailscale native connector — device visibility + key-expiry kill switch."""

    meta = ConnectorMeta(
        name="tailscale",
        display_name="Tailscale",
        auth_mode="none",  # credential self-sourced via OAuth client-credentials
    )

    @native_tool(_LIST_DEVICES_META)
    async def list_devices(self, *, access_token: str = "") -> list[dict[str, Any]]:
        data = await _ts_request("GET", f"/tailnet/{_tailnet()}/devices")
        return _mcp_text_content([_summarize_device(d) for d in data.get("devices", [])])

    @native_tool(_GET_DEVICE_META)
    async def get_device(self, *, access_token: str = "", device_id: str) -> list[dict[str, Any]]:
        _validate_device_id(device_id)
        data = await _ts_request("GET", f"/device/{device_id}")
        return _mcp_text_content(_summarize_device(data))

    @native_tool(_EXPIRE_DEVICE_KEY_META)
    async def expire_device_key(
        self, *, access_token: str = "", device_id: str
    ) -> list[dict[str, Any]]:
        _validate_device_id(device_id)
        await _ts_request("POST", f"/device/{device_id}/expire")
        return _mcp_text_content({"device_id": device_id, "key_expired": True})
