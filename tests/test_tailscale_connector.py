"""Tailscale native connector — OAuth client-credentials, device tools, error mapping."""

from __future__ import annotations

import json
from typing import Any

import pytest

import connectors.tailscale.adapter as ts_adapter
from broker.connectors.registry import ConnectorRegistry

# === FAKE HTTPX (same shape as tests/test_gcp_connector.py) ===


class FakeResponse:
    def __init__(self, status_code: int = 200, payload: Any = None, empty: bool = False):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.content = b"" if empty else json.dumps(self._payload).encode()

    def json(self) -> Any:
        return self._payload


class FakeAsyncClient:
    calls: list[tuple[str, str, dict]] = []
    responses: list[FakeResponse] = []

    def __init__(self, **_: Any) -> None: ...

    async def __aenter__(self) -> "FakeAsyncClient":
        return self

    async def __aexit__(self, *_: Any) -> bool:
        return False

    async def request(self, method: str, url: str, **kwargs: Any) -> FakeResponse:
        FakeAsyncClient.calls.append((method, url, kwargs))
        return FakeAsyncClient.responses.pop(0)

    async def get(self, url: str, **kwargs: Any) -> FakeResponse:
        return await self.request("GET", url, **kwargs)

    async def post(self, url: str, **kwargs: Any) -> FakeResponse:
        return await self.request("POST", url, **kwargs)


@pytest.fixture(autouse=True)
def _reset(monkeypatch: pytest.MonkeyPatch):
    FakeAsyncClient.calls = []
    FakeAsyncClient.responses = []
    ts_adapter._token_cache.update({"token": "", "expires_at": 0.0})
    monkeypatch.setattr(ts_adapter.httpx, "AsyncClient", FakeAsyncClient)
    monkeypatch.setenv("TAILSCALE_OAUTH_CLIENT_ID", "cid")
    monkeypatch.setenv("TAILSCALE_OAUTH_CLIENT_SECRET", "csecret")


def _token_response() -> FakeResponse:
    return FakeResponse(200, {"access_token": "ts-token", "expires_in": 3600})


@pytest.fixture
def connector():
    instance = ConnectorRegistry.get("tailscale")
    if instance is None:  # another test module cleared the registry
        ConnectorRegistry.auto_register(ts_adapter.TailscaleConnector)
        instance = ConnectorRegistry.get("tailscale")
    assert instance is not None
    return instance


# === TESTS ===


async def test_exactly_the_specified_tools_are_registered(connector) -> None:
    result = await connector.handle_mcp_request(
        method="tools/list", params={}, request_id=1, access_token=""
    )
    names = sorted(t["name"] for t in result["result"]["tools"])
    assert names == ["expire_device_key", "get_device", "list_devices"]


async def test_token_fetch_uses_client_credentials_and_caches(connector) -> None:
    FakeAsyncClient.responses = [
        _token_response(),
        FakeResponse(200, {"devices": []}),
        FakeResponse(200, {"devices": []}),
    ]
    await connector.list_devices(access_token="")
    await connector.list_devices(access_token="")
    token_calls = [c for c in FakeAsyncClient.calls if c[1].endswith("/oauth/token")]
    assert len(token_calls) == 1
    assert token_calls[0][2]["data"] == {"client_id": "cid", "client_secret": "csecret"}


async def test_list_devices_summarizes_and_defaults_tailnet(connector) -> None:
    FakeAsyncClient.responses = [
        _token_response(),
        FakeResponse(
            200,
            {
                "devices": [
                    {
                        "nodeId": "nABC123",
                        "hostname": "bobs-macbook-pro",
                        "name": "bobs-macbook-pro.tail1234.ts.net",
                        "os": "macOS",
                        "addresses": ["100.64.0.1"],
                        "lastSeen": "2026-07-06T18:00:00Z",
                        "expires": "2026-12-01T00:00:00Z",
                        "keyExpiryDisabled": False,
                        "updateAvailable": True,
                    }
                ]
            },
        ),
    ]
    content = await connector.list_devices(access_token="")
    _, url, _ = FakeAsyncClient.calls[-1]
    assert url.endswith("/tailnet/-/devices")  # "-" = the OAuth client's own tailnet
    device = json.loads(content[0]["text"])[0]
    assert device == {
        "id": "nABC123",
        "hostname": "bobs-macbook-pro",
        "name": "bobs-macbook-pro.tail1234.ts.net",
        "os": "macOS",
        "addresses": ["100.64.0.1"],
        "last_seen": "2026-07-06T18:00:00Z",
        "key_expires": "2026-12-01T00:00:00Z",
        "key_expiry_disabled": False,
        "update_available": True,
    }


async def test_expire_device_key_posts_and_handles_empty_body(connector) -> None:
    FakeAsyncClient.responses = [
        _token_response(),
        FakeResponse(200, empty=True),  # expire returns an empty 200
    ]
    content = await connector.expire_device_key(access_token="", device_id="nABC123")
    method, url, _ = FakeAsyncClient.calls[-1]
    assert method == "POST"
    assert url.endswith("/device/nABC123/expire")
    assert json.loads(content[0]["text"]) == {"device_id": "nABC123", "key_expired": True}


async def test_invalid_device_id_rejected_before_any_http(connector) -> None:
    with pytest.raises(ValueError, match="Invalid device id"):
        await connector.get_device(access_token="", device_id="../evil")
    assert FakeAsyncClient.calls == []


async def test_missing_credentials_is_a_readable_error(connector, monkeypatch) -> None:
    monkeypatch.delenv("TAILSCALE_OAUTH_CLIENT_SECRET")
    with pytest.raises(ValueError, match="Tailscale OAuth client credentials are not configured"):
        await connector.list_devices(access_token="")
    assert FakeAsyncClient.calls == []


async def test_upstream_error_maps_to_sanitized_valueerror(connector) -> None:
    FakeAsyncClient.responses = [
        _token_response(),
        FakeResponse(404, {"message": "device not found"}),
    ]
    with pytest.raises(ValueError, match="Tailscale API error 404: device not found"):
        await connector.get_device(access_token="", device_id="nMISSING")
