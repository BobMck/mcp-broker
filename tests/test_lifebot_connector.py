"""Lifebot native connector — static agent key, read-only tools, validation, error mapping."""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

import connectors.lifebot.adapter as lb_adapter
from broker.connectors.registry import ConnectorRegistry

# === FAKE HTTPX (same shape as tests/test_tailscale_connector.py) ===

KEY = "test-agent-key-sentinel"
BASE = "https://lifebot.example.com"


class FakeResponse:
    def __init__(self, status_code: int = 200, payload: Any = None, text: str | None = None):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.content = (text if text is not None else json.dumps(self._payload)).encode()
        self._text = text

    def json(self) -> Any:
        if self._text is not None:
            return json.loads(self._text)
        return self._payload


class FakeAsyncClient:
    calls: list[tuple[str, str, dict]] = []
    responses: list[FakeResponse | Exception] = []

    def __init__(self, **_: Any) -> None: ...

    async def __aenter__(self) -> FakeAsyncClient:
        return self

    async def __aexit__(self, *_: Any) -> bool:
        return False

    async def request(self, method: str, url: str, **kwargs: Any) -> FakeResponse:
        FakeAsyncClient.calls.append((method, url, kwargs))
        response = FakeAsyncClient.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    async def get(self, url: str, **kwargs: Any) -> FakeResponse:
        return await self.request("GET", url, **kwargs)


@pytest.fixture(autouse=True)
def _reset(monkeypatch: pytest.MonkeyPatch):
    FakeAsyncClient.calls = []
    FakeAsyncClient.responses = []
    monkeypatch.setattr(lb_adapter.httpx, "AsyncClient", FakeAsyncClient)
    monkeypatch.setenv("LIFEBOT_API_URL", BASE)
    monkeypatch.setenv("LIFEBOT_AGENT_KEY", KEY)


@pytest.fixture
def connector():
    instance = ConnectorRegistry.get("lifebot")
    if instance is None:  # another test module cleared the registry
        ConnectorRegistry.auto_register(lb_adapter.LifebotConnector)
        instance = ConnectorRegistry.get("lifebot")
    assert instance is not None
    return instance


def _last_call() -> tuple[str, str, dict]:
    return FakeAsyncClient.calls[-1]


# === REGISTRATION / META ===


async def test_exactly_the_specified_tools_are_registered(connector) -> None:
    result = await connector.handle_mcp_request(
        method="tools/list", params={}, request_id=1, access_token=""
    )
    tools = result["result"]["tools"]
    assert sorted(t["name"] for t in tools) == [
        "get_brief",
        "get_goals",
        "get_metrics",
        "get_workouts",
    ]
    assert all(t["inputSchema"]["type"] == "object" for t in tools)
    metrics = next(t for t in tools if t["name"] == "get_metrics")
    assert metrics["inputSchema"]["required"] == ["names"]


def test_meta_is_a_static_credential_native_connector(connector) -> None:
    assert connector.meta.name == "lifebot"
    assert connector.meta.display_name == "Lifebot (health)"
    assert connector.meta.auth_mode == "none"
    assert connector.meta.is_native


# === HAPPY PATHS ===


async def test_get_brief_defaults_to_no_date_and_sends_key_header(connector) -> None:
    payload = {"date": "2026-01-15", "freshness": [], "metrics": []}
    FakeAsyncClient.responses = [FakeResponse(200, payload)]
    content = await connector.get_brief(access_token="")
    method, url, kwargs = _last_call()
    assert method == "GET"
    assert url == f"{BASE}/agent/brief"
    assert kwargs["params"] == {}
    assert kwargs["headers"] == {"x-lifebot-agent-key": KEY}
    assert content[0]["type"] == "text"
    assert json.loads(content[0]["text"]) == payload


async def test_get_brief_passes_date_as_param(connector) -> None:
    FakeAsyncClient.responses = [FakeResponse(200, {"date": "2026-02-28"})]
    await connector.get_brief(access_token="", date="2026-02-28")
    _, url, kwargs = _last_call()
    assert url == f"{BASE}/agent/brief"
    assert kwargs["params"] == {"date": "2026-02-28"}


async def test_get_metrics_with_list_and_days(connector) -> None:
    payload = {
        "metrics": [{"name": "step_count", "unit": "count", "points": []}],
        "unknown": ["nope"],
    }
    FakeAsyncClient.responses = [FakeResponse(200, payload)]
    content = await connector.get_metrics(
        access_token="", names=["step_count", "resting_heart_rate"], days=7
    )
    _, url, kwargs = _last_call()
    assert url == f"{BASE}/agent/metrics"
    assert kwargs["params"] == {"names": "step_count,resting_heart_rate", "days": "7"}
    assert kwargs["headers"] == {"x-lifebot-agent-key": KEY}
    assert json.loads(content[0]["text"]) == payload


async def test_get_metrics_accepts_comma_separated_string_and_dedupes(connector) -> None:
    FakeAsyncClient.responses = [FakeResponse(200, {"metrics": [], "unknown": []})]
    await connector.get_metrics(access_token="", names="step_count, body_mass,step_count")
    _, _, kwargs = _last_call()
    assert kwargs["params"] == {"names": "step_count,body_mass"}  # no days -> server default


async def test_get_metrics_ten_names_after_dedupe_is_allowed(connector) -> None:
    names = [f"m{i}" for i in range(10)] + ["m0"]  # 11 entries, 10 distinct
    FakeAsyncClient.responses = [FakeResponse(200, {"metrics": [], "unknown": []})]
    await connector.get_metrics(access_token="", names=names)
    _, _, kwargs = _last_call()
    assert kwargs["params"]["names"] == ",".join(f"m{i}" for i in range(10))


async def test_get_workouts_defaults_send_no_params(connector) -> None:
    payload = {"from": "2026-01-01", "to": "2026-01-14", "workouts": []}
    FakeAsyncClient.responses = [FakeResponse(200, payload)]
    content = await connector.get_workouts(access_token="")
    _, url, kwargs = _last_call()
    assert url == f"{BASE}/agent/workouts"
    assert kwargs["params"] == {}
    assert kwargs["headers"] == {"x-lifebot-agent-key": KEY}
    assert json.loads(content[0]["text"]) == payload


@pytest.mark.parametrize(("detail", "expected"), [(True, "true"), (False, "false")])
async def test_get_workouts_days_and_detail_params(connector, detail, expected) -> None:
    FakeAsyncClient.responses = [FakeResponse(200, {"workouts": []})]
    await connector.get_workouts(access_token="", days=30, detail=detail)
    _, _, kwargs = _last_call()
    assert kwargs["params"] == {"days": "30", "detail": expected}


async def test_get_goals(connector) -> None:
    payload = {"goals": [{"metric": "step_count", "target": 8000.0, "type": "daily"}]}
    FakeAsyncClient.responses = [FakeResponse(200, payload)]
    content = await connector.get_goals(access_token="")
    _, url, kwargs = _last_call()
    assert url == f"{BASE}/agent/goals"
    assert kwargs["params"] == {}
    assert kwargs["headers"] == {"x-lifebot-agent-key": KEY}
    assert json.loads(content[0]["text"]) == payload


async def test_trailing_slash_on_base_url_is_stripped(connector, monkeypatch) -> None:
    monkeypatch.setenv("LIFEBOT_API_URL", f"{BASE}/")
    FakeAsyncClient.responses = [FakeResponse(200, {"goals": []})]
    await connector.get_goals(access_token="")
    assert _last_call()[1] == f"{BASE}/agent/goals"


async def test_base_url_may_carry_a_path_prefix(connector, monkeypatch) -> None:
    monkeypatch.setenv("LIFEBOT_API_URL", f"{BASE}/prod/")
    FakeAsyncClient.responses = [FakeResponse(200, {"goals": []})]
    await connector.get_goals(access_token="")
    assert _last_call()[1] == f"{BASE}/prod/agent/goals"


# === VALIDATION (rejected before any HTTP) ===


@pytest.mark.parametrize(
    "date",
    [
        "2026-02-30",  # impossible
        "2026-13-01",  # impossible month
        "2026-1-5",  # wrong shape
        "2026-01-15\n",  # trailing newline
        " 2026-01-15",
        "20260115",
        "2026-01-15&names=x",  # query injection
        "٢٠٢٦-٠١-١٥",  # non-ASCII digits
        "",
        20260115,
    ],
)
async def test_get_brief_rejects_bad_dates(connector, date) -> None:
    with pytest.raises(ValueError, match="Invalid date"):
        await connector.get_brief(access_token="", date=date)
    assert FakeAsyncClient.calls == []


async def test_date_with_trailing_newline_fails_the_format_check(connector) -> None:
    with pytest.raises(ValueError, match="expected YYYY-MM-DD"):
        await connector.get_brief(access_token="", date="2026-01-15\n")
    assert FakeAsyncClient.calls == []


@pytest.mark.parametrize(
    "names",
    [
        ["step/count"],  # slash
        ["step.count"],  # dot
        ["../goals"],
        ["step_count\n"],  # trailing newline
        "step_count\n",  # trailing newline, string form
        ["Step_Count"],  # uppercase
        ["a" * 65],  # too long
        ["step_count&days=1"],
        [""],
        "a,,b",  # empty entry
        [1],
        [["step_count"]],
        [None],
    ],
)
async def test_get_metrics_rejects_bad_names(connector, names) -> None:
    with pytest.raises(ValueError, match="Invalid metric name"):
        await connector.get_metrics(access_token="", names=names)
    assert FakeAsyncClient.calls == []


@pytest.mark.parametrize("names", [[], "", 5, None, {"a": 1}])
async def test_get_metrics_rejects_empty_or_wrong_type_names(connector, names) -> None:
    with pytest.raises(ValueError, match="Invalid"):
        await connector.get_metrics(access_token="", names=names)
    assert FakeAsyncClient.calls == []


async def test_get_metrics_rejects_eleven_distinct_names(connector) -> None:
    with pytest.raises(ValueError, match="1-10 distinct metric names"):
        await connector.get_metrics(access_token="", names=[f"m{i}" for i in range(11)])
    assert FakeAsyncClient.calls == []


async def test_get_metrics_accepts_64_char_name(connector) -> None:
    FakeAsyncClient.responses = [FakeResponse(200, {"metrics": [], "unknown": []})]
    await connector.get_metrics(access_token="", names=["a" * 64])
    assert _last_call()[2]["params"]["names"] == "a" * 64


@pytest.mark.parametrize("days", [0, 366, -1, True, False, 7.0, "7", [7]])
async def test_bad_days_rejected_for_metrics_and_workouts(connector, days) -> None:
    with pytest.raises(ValueError, match="Invalid days"):
        await connector.get_metrics(access_token="", names=["step_count"], days=days)
    with pytest.raises(ValueError, match="Invalid days"):
        await connector.get_workouts(access_token="", days=days)
    assert FakeAsyncClient.calls == []


@pytest.mark.parametrize("days", [1, 365])
async def test_days_bounds_are_inclusive(connector, days) -> None:
    FakeAsyncClient.responses = [FakeResponse(200, {"workouts": []})]
    await connector.get_workouts(access_token="", days=days)
    assert _last_call()[2]["params"] == {"days": str(days)}


@pytest.mark.parametrize("detail", ["true", "false", 1, 0, "yes", [True]])
async def test_non_boolean_detail_rejected(connector, detail) -> None:
    with pytest.raises(ValueError, match="Invalid detail"):
        await connector.get_workouts(access_token="", detail=detail)
    assert FakeAsyncClient.calls == []


# === CONFIGURATION ===


async def test_missing_api_url_is_a_readable_error(connector, monkeypatch) -> None:
    monkeypatch.delenv("LIFEBOT_API_URL")
    with pytest.raises(ValueError, match="Lifebot API URL is not configured on the broker"):
        await connector.get_goals(access_token="")
    assert FakeAsyncClient.calls == []


async def test_missing_agent_key_is_a_readable_error(connector, monkeypatch) -> None:
    monkeypatch.delenv("LIFEBOT_AGENT_KEY")
    with pytest.raises(ValueError, match="Lifebot agent key is not configured on the broker"):
        await connector.get_goals(access_token="")
    assert FakeAsyncClient.calls == []


async def test_empty_agent_key_counts_as_not_configured(connector, monkeypatch) -> None:
    monkeypatch.setenv("LIFEBOT_AGENT_KEY", "")
    with pytest.raises(ValueError, match="agent key is not configured"):
        await connector.get_goals(access_token="")
    assert FakeAsyncClient.calls == []


@pytest.mark.parametrize(
    "url",
    [
        "http://lifebot.example.com",
        "HTTP://lifebot.example.com",
        "ftp://lifebot.example.com",
        "lifebot.example.com",
        "https://",
        "https:///agent",
        "https://lifebot.example.com?x=1",
        "https://lifebot.example.com#frag",
        "https://lifebot.example.com/a b",
    ],
)
async def test_non_https_or_malformed_base_url_rejected(connector, monkeypatch, url) -> None:
    monkeypatch.setenv("LIFEBOT_API_URL", url)
    with pytest.raises(ValueError, match="must be an https:// URL"):
        await connector.get_goals(access_token="")
    assert FakeAsyncClient.calls == []


# === UPSTREAM ERROR MAPPING ===


async def test_401_tells_operator_to_check_the_key(connector) -> None:
    FakeAsyncClient.responses = [FakeResponse(401, {"error": "unauthorized"})]
    with pytest.raises(ValueError, match=r"rejected the broker's agent key.*LIFEBOT_AGENT_KEY"):
        await connector.get_goals(access_token="")


async def test_503_means_key_not_configured_upstream(connector) -> None:
    FakeAsyncClient.responses = [FakeResponse(503, {"error": "agent key not configured"})]
    with pytest.raises(ValueError, match="not configured on the Lifebot side"):
        await connector.get_goals(access_token="")


async def test_429_means_rate_limited(connector) -> None:
    FakeAsyncClient.responses = [FakeResponse(429, {"message": "Too Many Requests"})]
    with pytest.raises(ValueError, match="rate limited.*retry later"):
        await connector.get_goals(access_token="")


async def test_400_includes_the_contract_detail(connector) -> None:
    FakeAsyncClient.responses = [
        FakeResponse(400, {"error": "invalid parameter", "detail": "date is in the future"})
    ]
    with pytest.raises(ValueError, match="date is in the future"):
        await connector.get_brief(access_token="", date="2999-01-01")


async def test_400_detail_is_truncated(connector) -> None:
    FakeAsyncClient.responses = [FakeResponse(400, {"detail": "x" * 5000})]
    with pytest.raises(ValueError) as excinfo:
        await connector.get_brief(access_token="")
    assert len(str(excinfo.value)) < 400


@pytest.mark.parametrize(
    "response",
    [
        FakeResponse(400, text="not json"),
        FakeResponse(400, {"error": "invalid parameter"}),
        FakeResponse(400, {"detail": 5}),
    ],
)
async def test_400_without_usable_detail_still_maps(connector, response) -> None:
    FakeAsyncClient.responses = [response]
    with pytest.raises(ValueError, match=r"\(400\): no detail"):
        await connector.get_goals(access_token="")


@pytest.mark.parametrize("status", [404, 500, 502])
async def test_other_statuses_map_to_generic_error_without_body(connector, status) -> None:
    FakeAsyncClient.responses = [
        FakeResponse(status, {"error": "internal error", "trace": "secret-internals"})
    ]
    with pytest.raises(ValueError) as excinfo:
        await connector.get_goals(access_token="")
    assert str(excinfo.value) == f"Lifebot API error {status}"


async def test_transport_error_does_not_leak_url(connector) -> None:
    FakeAsyncClient.responses = [httpx.ConnectError(f"connection failed to {BASE}/agent/goals")]
    with pytest.raises(ValueError) as excinfo:
        await connector.get_goals(access_token="")
    assert "unreachable" in str(excinfo.value)
    assert BASE not in str(excinfo.value)


async def test_non_json_success_body_is_a_readable_error(connector) -> None:
    FakeAsyncClient.responses = [FakeResponse(200, text="<html>oops</html>")]
    with pytest.raises(ValueError, match="invalid response"):
        await connector.get_goals(access_token="")


# === THE KEY NEVER LEAKS ===


async def test_key_never_appears_in_errors_or_results(connector, monkeypatch) -> None:
    # Upstream bodies that echo the key back must not reach the caller either.
    echo = {"error": f"bad key {KEY}", "detail": "ok", "message": KEY}
    scenarios: list[Any] = [
        FakeResponse(200, {"goals": []}),
        FakeResponse(400, echo),
        FakeResponse(401, echo),
        FakeResponse(429, echo),
        FakeResponse(500, echo),
        FakeResponse(503, echo),
        httpx.ConnectError(f"failed with {KEY}"),
    ]
    for scenario in scenarios:
        FakeAsyncClient.responses = [scenario]
        try:
            content = await connector.get_goals(access_token="")
        except ValueError as exc:
            assert KEY not in str(exc)
        else:
            assert KEY not in json.dumps(content)


async def test_key_never_in_validation_errors_or_logs(connector, caplog) -> None:
    with caplog.at_level("DEBUG"):
        with pytest.raises(ValueError) as excinfo:
            await connector.get_metrics(access_token="", names=["bad/name"])
        FakeAsyncClient.responses = [FakeResponse(401, {})]
        with pytest.raises(ValueError):
            await connector.get_goals(access_token="")
    assert KEY not in str(excinfo.value)
    assert KEY not in caplog.text


async def test_key_not_in_url_or_params(connector) -> None:
    FakeAsyncClient.responses = [FakeResponse(200, {"workouts": []})]
    await connector.get_workouts(access_token="", days=3, detail=True)
    _, url, kwargs = _last_call()
    assert KEY not in url
    assert KEY not in json.dumps(kwargs["params"])


async def test_jsonrpc_error_for_rejected_argument_does_not_leak_key(connector) -> None:
    result = await connector.handle_mcp_request(
        method="tools/call",
        params={"name": "get_metrics", "arguments": {"names": ["a.b"]}},
        request_id=7,
        access_token="",
    )
    assert KEY not in json.dumps(result)
    assert FakeAsyncClient.calls == []
