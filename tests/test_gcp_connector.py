"""GCP native connector — token caching, tool registry, dispatch, error mapping."""

from __future__ import annotations

import json
from typing import Any

import pytest

import connectors.gcp.adapter as gcp_adapter
from broker.connectors.registry import ConnectorRegistry

# === FAKE HTTPX ===


class FakeResponse:
    def __init__(self, status_code: int = 200, payload: Any = None):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.content = json.dumps(self._payload).encode()

    def json(self) -> Any:
        return self._payload


class FakeAsyncClient:
    """Stub for httpx.AsyncClient: records calls, replays canned responses."""

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
    gcp_adapter._token_cache.update({"token": "", "expires_at": 0.0})
    monkeypatch.setattr(gcp_adapter.httpx, "AsyncClient", FakeAsyncClient)
    monkeypatch.setenv("GCP_PROJECT", "test-project")
    monkeypatch.setenv("GCP_ZONE", "test-zone-a")


def _token_response() -> FakeResponse:
    return FakeResponse(200, {"access_token": "meta-token", "expires_in": 3600})


@pytest.fixture
def connector():
    instance = ConnectorRegistry.get("gcp")
    if instance is None:  # another test module cleared the registry
        ConnectorRegistry.auto_register(gcp_adapter.GcpConnector)
        instance = ConnectorRegistry.get("gcp")
    assert instance is not None
    return instance


# === TESTS ===


async def test_exactly_the_specified_tools_are_registered(connector) -> None:
    result = await connector.handle_mcp_request(
        method="tools/list", params={}, request_id=1, access_token=""
    )
    names = sorted(t["name"] for t in result["result"]["tools"])
    assert names == sorted(
        [
            "list_instances",
            "get_instance",
            "get_serial_output",
            "list_log_entries",
            "stop_instance",
            "start_instance",
            "reset_instance",
        ]
    )


async def test_metadata_token_is_cached(connector) -> None:
    FakeAsyncClient.responses = [
        _token_response(),
        FakeResponse(200, {"items": []}),
        FakeResponse(200, {"items": []}),  # second call: NO new token response needed
    ]
    await connector.list_instances(access_token="")
    await connector.list_instances(access_token="")
    token_calls = [c for c in FakeAsyncClient.calls if "169.254.169.254" in c[1]]
    assert len(token_calls) == 1
    assert token_calls[0][2]["headers"] == {"Metadata-Flavor": "Google"}


async def test_list_instances_summarizes(connector) -> None:
    FakeAsyncClient.responses = [
        _token_response(),
        FakeResponse(
            200,
            {
                "items": [
                    {
                        "name": "gcp-mcp-broker",
                        "status": "RUNNING",
                        "machineType": "zones/z/machineTypes/e2-small",
                        "zone": "zones/test-zone-a",
                        "networkInterfaces": [
                            {
                                "networkIP": "10.0.0.2",
                                "accessConfigs": [{"natIP": "34.1.2.3"}],
                            }
                        ],
                    }
                ]
            },
        ),
    ]
    content = await connector.list_instances(access_token="")
    payload = json.loads(content[0]["text"])
    assert payload == [
        {
            "name": "gcp-mcp-broker",
            "status": "RUNNING",
            "machine_type": "e2-small",
            "zone": "test-zone-a",
            "internal_ip": "10.0.0.2",
            "external_ip": "34.1.2.3",
            "last_start": None,
        }
    ]


async def test_stop_instance_posts_to_stop_url(connector) -> None:
    FakeAsyncClient.responses = [
        _token_response(),
        FakeResponse(200, {"status": "PENDING", "name": "operation-123"}),
    ]
    content = await connector.stop_instance(access_token="", instance="gcp-mcp-broker")
    method, url, kwargs = FakeAsyncClient.calls[-1]
    assert method == "POST"
    assert url.endswith("/projects/test-project/zones/test-zone-a/instances/gcp-mcp-broker/stop")
    assert kwargs["headers"]["Authorization"] == "Bearer meta-token"
    payload = json.loads(content[0]["text"])
    assert payload["action"] == "stop"
    assert payload["operation_status"] == "PENDING"


async def test_invalid_instance_name_rejected_before_any_http(connector) -> None:
    with pytest.raises(ValueError, match="Invalid instance name"):
        await connector.reset_instance(access_token="", instance="../evil")
    assert FakeAsyncClient.calls == []


async def test_upstream_error_maps_to_sanitized_valueerror(connector) -> None:
    FakeAsyncClient.responses = [
        _token_response(),
        FakeResponse(403, {"error": {"message": "Required permission missing"}}),
    ]
    with pytest.raises(ValueError, match="GCP API error 403: Required permission missing"):
        await connector.get_instance(access_token="", instance="gcp-mcp-broker")


async def test_missing_project_env_is_a_readable_error(connector, monkeypatch) -> None:
    monkeypatch.delenv("GCP_PROJECT")
    with pytest.raises(ValueError, match="GCP_PROJECT is not configured"):
        await connector.list_instances(access_token="")
    assert FakeAsyncClient.calls == []


async def test_list_log_entries_builds_bounded_query(connector) -> None:
    FakeAsyncClient.responses = [
        _token_response(),
        FakeResponse(
            200,
            {
                "entries": [
                    {
                        "timestamp": "2026-07-06T10:00:00Z",
                        "severity": "ERROR",
                        "logName": "projects/test-project/logs/syslog",
                        "textPayload": "oom-killer invoked",
                    }
                ]
            },
        ),
    ]
    content = await connector.list_log_entries(
        access_token="", log_filter="severity>=ERROR", hours=2, max_entries=999
    )
    method, url, kwargs = FakeAsyncClient.calls[-1]
    body = kwargs["json"]
    assert method == "POST"
    assert body["resourceNames"] == ["projects/test-project"]
    assert body["pageSize"] == 50  # clamped from 999
    assert "severity>=ERROR" in body["filter"]
    assert 'timestamp >= "' in body["filter"]
    entry = json.loads(content[0]["text"])[0]
    assert entry == {
        "timestamp": "2026-07-06T10:00:00Z",
        "severity": "ERROR",
        "log": "syslog",
        "payload": "oom-killer invoked",
    }
