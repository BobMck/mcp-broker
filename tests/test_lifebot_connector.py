"""Lifebot native connector — static read/write keys, tools, validation, error mapping."""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

import connectors.lifebot.adapter as lb_adapter
from broker.connectors.registry import ConnectorRegistry

# === FAKE HTTPX (same shape as tests/test_tailscale_connector.py) ===

KEY = "test-agent-key-sentinel"
WKEY = "test-write-key-sentinel"
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
    monkeypatch.setenv("LIFEBOT_AGENT_WRITE_KEY", WKEY)


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
        "get_course_curriculum",
        "get_goals",
        "get_messages",
        "get_metrics",
        "get_study",
        "get_study_plan",
        "get_study_tasks",
        "get_workouts",
        "post_reply",
        "save_study_plan",
        "save_study_tasks",
    ]
    assert all(t["inputSchema"]["type"] == "object" for t in tools)
    metrics = next(t for t in tools if t["name"] == "get_metrics")
    assert metrics["inputSchema"]["required"] == ["names"]
    save = next(t for t in tools if t["name"] == "save_study_plan")
    assert save["inputSchema"]["required"] == ["plan"]
    assert save["inputSchema"]["properties"]["plan"]["required"] == ["goal", "focus"]
    reply = next(t for t in tools if t["name"] == "post_reply")
    assert reply["inputSchema"]["required"] == ["text"]
    get_tasks = next(t for t in tools if t["name"] == "get_study_tasks")
    assert sorted(get_tasks["inputSchema"]["properties"]) == ["from", "to"]
    assert "required" not in get_tasks["inputSchema"]
    save_tasks = next(t for t in tools if t["name"] == "save_study_tasks")
    assert save_tasks["inputSchema"]["required"] == ["from", "to", "tasks"]
    task = save_tasks["inputSchema"]["properties"]["tasks"]["items"]
    assert task["required"] == ["date", "title", "minutes"]
    assert task["additionalProperties"] is False
    assert sorted(task["properties"]) == [
        "course_id",
        "date",
        "kind",
        "lecture_ids",
        "link_label",
        "minutes",
        "note",
        "title",
        "url",
    ]
    for server_assigned in ("id", "order", "done", "done_by", "auto_tick"):
        assert server_assigned not in task["properties"]
    assert task["properties"]["kind"]["enum"] == [
        "video",
        "reading",
        "lab",
        "practice",
        "review",
        "exam",
        "other",
    ]
    assert save_tasks["inputSchema"]["properties"]["tasks"]["maxItems"] == 60
    assert "REPLACES" in save_tasks["description"]


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


# === STUDY / MESSAGES: SHARED HELPERS ===

READ_HEADER = "x-lifebot-agent-key"
WRITE_HEADER = "x-lifebot-agent-write-key"
PLAN = {
    "goal": "Finish the networking course",
    "focus": [{"course_id": "course-a", "order": 1}],
}
TASK = {"date": "2026-11-23", "title": "Watch lecture 4", "minutes": 45}
TASKS_CALL: dict[str, Any] = {"from": "2026-11-23", "to": "2026-11-29", "tasks": [TASK]}


def _assert_only_key(kwargs: dict, header: str, key: str) -> None:
    """Exactly one credential header was sent, and the other key value is nowhere in it."""
    assert kwargs["headers"] == {header: key}
    other = WKEY if key == KEY else KEY
    assert other not in json.dumps(kwargs, default=str)


def _plan_of_size(size: int) -> dict[str, Any]:
    plan: dict[str, Any] = {**PLAN, "notes": ""}
    base = len(json.dumps(plan, separators=(",", ":")).encode())
    plan["notes"] = "x" * (size - base)
    assert len(json.dumps(plan, separators=(",", ":")).encode()) == size
    return plan


# === STUDY / MESSAGES: READ HAPPY PATHS ===


async def test_get_study_defaults_send_no_params_and_the_read_key(connector) -> None:
    payload = {"courses": [], "summary": {"count": 0}, "notes": ["No snapshot history yet"]}
    FakeAsyncClient.responses = [FakeResponse(200, payload)]
    content = await connector.get_study(access_token="")
    method, url, kwargs = _last_call()
    assert (method, url) == ("GET", f"{BASE}/agent/study")
    assert kwargs["params"] == {}
    _assert_only_key(kwargs, READ_HEADER, KEY)
    assert json.loads(content[0]["text"]) == payload


async def test_get_study_passes_days(connector) -> None:
    FakeAsyncClient.responses = [FakeResponse(200, {"courses": []})]
    await connector.get_study(access_token="", days=90)
    assert _last_call()[2]["params"] == {"days": "90"}


async def test_get_study_plan_sends_the_read_key(connector) -> None:
    payload = {"proposed": {"version": "STUDY#x", "status": "proposed", "plan": PLAN}}
    FakeAsyncClient.responses = [FakeResponse(200, payload)]
    content = await connector.get_study_plan(access_token="")
    method, url, kwargs = _last_call()
    assert (method, url) == ("GET", f"{BASE}/agent/study-plan")
    assert kwargs["params"] == {}
    _assert_only_key(kwargs, READ_HEADER, KEY)
    assert json.loads(content[0]["text"]) == payload


async def test_get_messages_defaults_send_no_params(connector) -> None:
    payload = {"conversation": "main", "messages": []}
    FakeAsyncClient.responses = [FakeResponse(200, payload)]
    content = await connector.get_messages(access_token="")
    method, url, kwargs = _last_call()
    assert (method, url) == ("GET", f"{BASE}/agent/messages")
    assert kwargs["params"] == {}
    _assert_only_key(kwargs, READ_HEADER, KEY)
    assert json.loads(content[0]["text"]) == payload


async def test_get_messages_passes_conversation_and_limit(connector) -> None:
    FakeAsyncClient.responses = [FakeResponse(200, {"messages": []})]
    await connector.get_messages(access_token="", conversation="web_page-2", limit=5)
    assert _last_call()[2]["params"] == {"conversation": "web_page-2", "limit": "5"}


@pytest.mark.parametrize("conversation", ["a", "a" * 32, "a-b_c9"])
async def test_conversation_bounds_are_inclusive(connector, conversation) -> None:
    FakeAsyncClient.responses = [FakeResponse(200, {"messages": []})]
    await connector.get_messages(access_token="", conversation=conversation)
    assert _last_call()[2]["params"] == {"conversation": conversation}


@pytest.mark.parametrize("limit", [1, 50])
async def test_limit_bounds_are_inclusive(connector, limit) -> None:
    FakeAsyncClient.responses = [FakeResponse(200, {"messages": []})]
    await connector.get_messages(access_token="", limit=limit)
    assert _last_call()[2]["params"] == {"limit": str(limit)}


# === STUDY / MESSAGES: WRITE HAPPY PATHS ===


async def test_save_study_plan_puts_the_plan_with_the_write_key_only(connector) -> None:
    payload = {
        "saved": True,
        "version": "STUDY#2026-01-01T00:00:00.000Z",
        "status": "proposed",
        "unknown_course_ids": ["course-a"],
    }
    FakeAsyncClient.responses = [FakeResponse(201, payload)]
    content = await connector.save_study_plan(access_token="", plan=PLAN)
    method, url, kwargs = _last_call()
    assert (method, url) == ("PUT", f"{BASE}/agent/study-plan")
    assert kwargs["json"] == PLAN
    assert "params" not in kwargs
    _assert_only_key(kwargs, WRITE_HEADER, WKEY)
    assert json.loads(content[0]["text"]) == payload


async def test_save_study_plan_passes_a_full_plan_through_unchanged(connector) -> None:
    plan = {
        "goal": "Pass the exam",
        "why": "Career",
        "deadline": "2026-12-31",
        "weekly_hours": 6.5,
        "focus": [
            {"course_id": "course-a", "order": 1, "title": "A", "target_completion": 80},
            {"course_id": "course-b", "order": 2, "target_date": "2026-11-30"},
        ],
        "milestones": [{"date": "2026-12-01", "text": "Mock exam"}],
        "notes": "café ☕",
    }
    FakeAsyncClient.responses = [FakeResponse(201, {"saved": True})]
    await connector.save_study_plan(access_token="", plan=plan)
    assert _last_call()[2]["json"] == plan


async def test_plan_of_exactly_20480_bytes_is_accepted(connector) -> None:
    FakeAsyncClient.responses = [FakeResponse(201, {"saved": True})]
    await connector.save_study_plan(access_token="", plan=_plan_of_size(20480))
    assert len(FakeAsyncClient.calls) == 1


async def test_post_reply_posts_text_with_the_write_key_only(connector) -> None:
    FakeAsyncClient.responses = [FakeResponse(201, {"saved": True, "id": "m-1"})]
    content = await connector.post_reply(access_token="", text="Hello there")
    method, url, kwargs = _last_call()
    assert (method, url) == ("POST", f"{BASE}/agent/messages/reply")
    assert kwargs["json"] == {"text": "Hello there"}  # conversation omitted -> server default
    assert "params" not in kwargs
    _assert_only_key(kwargs, WRITE_HEADER, WKEY)
    assert json.loads(content[0]["text"]) == {"saved": True, "id": "m-1"}


async def test_post_reply_passes_conversation(connector) -> None:
    FakeAsyncClient.responses = [FakeResponse(201, {"saved": True, "id": "m-2"})]
    await connector.post_reply(access_token="", text="Hi", conversation="side_chat")
    assert _last_call()[2]["json"] == {"text": "Hi", "conversation": "side_chat"}


@pytest.mark.parametrize("length", [1, 8000])
async def test_reply_text_bounds_are_inclusive(connector, length) -> None:
    FakeAsyncClient.responses = [FakeResponse(201, {"saved": True, "id": "m"})]
    await connector.post_reply(access_token="", text="x" * length)
    assert _last_call()[2]["json"] == {"text": "x" * length}


# === STUDY / MESSAGES: VALIDATION (rejected before any HTTP) ===


@pytest.mark.parametrize(
    "conversation",
    [
        "main\n",  # trailing newline
        "\nmain",
        "Main",  # uppercase
        "a" * 33,  # too long
        "",
        "a b",
        "a/b",
        "a.b",
        "main&limit=1",  # query injection
        "ｍain",  # non-ASCII look-alike
        5,
        ["main"],
    ],
)
async def test_bad_conversation_rejected_for_read_and_write(connector, conversation) -> None:
    with pytest.raises(ValueError, match="Invalid conversation"):
        await connector.get_messages(access_token="", conversation=conversation)
    with pytest.raises(ValueError, match="Invalid conversation"):
        await connector.post_reply(access_token="", text="hi", conversation=conversation)
    assert FakeAsyncClient.calls == []


@pytest.mark.parametrize("limit", [0, 51, -1, True, False, 5.0, "5", [5]])
async def test_bad_limit_rejected(connector, limit) -> None:
    with pytest.raises(ValueError, match="Invalid limit"):
        await connector.get_messages(access_token="", limit=limit)
    assert FakeAsyncClient.calls == []


@pytest.mark.parametrize("days", [0, 366, -1, True, False, 7.0, "7", [7]])
async def test_bad_days_rejected_for_study(connector, days) -> None:
    with pytest.raises(ValueError, match="Invalid days"):
        await connector.get_study(access_token="", days=days)
    assert FakeAsyncClient.calls == []


@pytest.mark.parametrize("days", [1, 365])
async def test_study_days_bounds_are_inclusive(connector, days) -> None:
    FakeAsyncClient.responses = [FakeResponse(200, {"courses": []})]
    await connector.get_study(access_token="", days=days)
    assert _last_call()[2]["params"] == {"days": str(days)}


@pytest.mark.parametrize("plan", [[PLAN], "a plan", 5, None, True, (PLAN,)])
async def test_plan_must_be_an_object(connector, plan) -> None:
    with pytest.raises(ValueError, match="expected a JSON object"):
        await connector.save_study_plan(access_token="", plan=plan)
    assert FakeAsyncClient.calls == []


async def test_plan_over_20480_bytes_rejected(connector) -> None:
    with pytest.raises(ValueError, match="too large"):
        await connector.save_study_plan(access_token="", plan=_plan_of_size(20481))
    assert FakeAsyncClient.calls == []


async def test_plan_size_counts_utf8_bytes_not_characters(connector) -> None:
    # 10,500 two-byte characters: well under 20480 characters, over 20480 bytes.
    plan = {**PLAN, "notes": "é" * 10500}
    with pytest.raises(ValueError, match="too large"):
        await connector.save_study_plan(access_token="", plan=plan)
    assert FakeAsyncClient.calls == []


@pytest.mark.parametrize("plan", [{"goal": float("nan"), "focus": [1]}, {**PLAN, "x": {1, 2}}])
async def test_plan_must_be_plain_json_data(connector, plan) -> None:
    with pytest.raises(ValueError, match="plain JSON"):
        await connector.save_study_plan(access_token="", plan=plan)
    assert FakeAsyncClient.calls == []


@pytest.mark.parametrize("goal", [None, "", 5, ["x"], True])
async def test_plan_needs_a_non_empty_string_goal(connector, goal) -> None:
    with pytest.raises(ValueError, match="goal must be a non-empty string"):
        await connector.save_study_plan(access_token="", plan={**PLAN, "goal": goal})
    with pytest.raises(ValueError, match="goal must be a non-empty string"):
        await connector.save_study_plan(access_token="", plan={"focus": PLAN["focus"]})
    assert FakeAsyncClient.calls == []


@pytest.mark.parametrize("focus", [None, [], "course-a", {"course_id": "a"}, 1])
async def test_plan_needs_a_non_empty_focus_list(connector, focus) -> None:
    with pytest.raises(ValueError, match="focus must be a non-empty list"):
        await connector.save_study_plan(access_token="", plan={**PLAN, "focus": focus})
    with pytest.raises(ValueError, match="focus must be a non-empty list"):
        await connector.save_study_plan(access_token="", plan={"goal": "g"})
    assert FakeAsyncClient.calls == []


@pytest.mark.parametrize("text", ["", "x" * 8001, None, 5, ["hi"]])
async def test_bad_reply_text_rejected(connector, text) -> None:
    with pytest.raises(ValueError, match="Invalid text"):
        await connector.post_reply(access_token="", text=text)
    assert FakeAsyncClient.calls == []


async def test_validation_errors_do_not_echo_plan_or_text(connector) -> None:
    secret_plan = {"goal": "", "focus": [{"course_id": "private-course-id", "order": 1}]}
    with pytest.raises(ValueError) as plan_error:
        await connector.save_study_plan(access_token="", plan=secret_plan)
    with pytest.raises(ValueError) as text_error:
        await connector.post_reply(access_token="", text="private message " * 600)
    assert "private" not in str(plan_error.value)
    assert "private" not in str(text_error.value)


# === STUDY / MESSAGES: CONFIGURATION (two keys) ===


async def test_missing_write_key_blocks_both_write_tools_before_any_http(
    connector, monkeypatch
) -> None:
    monkeypatch.delenv("LIFEBOT_AGENT_WRITE_KEY")
    with pytest.raises(ValueError, match="Lifebot write key is not configured on the broker"):
        await connector.save_study_plan(access_token="", plan=PLAN)
    with pytest.raises(ValueError, match="Lifebot write key is not configured on the broker"):
        await connector.post_reply(access_token="", text="hi")
    assert FakeAsyncClient.calls == []


async def test_empty_write_key_counts_as_not_configured(connector, monkeypatch) -> None:
    monkeypatch.setenv("LIFEBOT_AGENT_WRITE_KEY", "")
    with pytest.raises(ValueError, match="write key is not configured"):
        await connector.post_reply(access_token="", text="hi")
    assert FakeAsyncClient.calls == []


async def test_read_tools_keep_working_without_the_write_key(connector, monkeypatch) -> None:
    monkeypatch.delenv("LIFEBOT_AGENT_WRITE_KEY")
    FakeAsyncClient.responses = [FakeResponse(200, {}) for _ in range(5)]
    await connector.get_brief(access_token="")
    await connector.get_study(access_token="")
    await connector.get_study_plan(access_token="")
    await connector.get_messages(access_token="")
    await connector.get_goals(access_token="")
    assert len(FakeAsyncClient.calls) == 5
    for _, _, kwargs in FakeAsyncClient.calls:
        _assert_only_key(kwargs, READ_HEADER, KEY)


async def test_write_tools_work_without_the_read_key_and_never_send_it(
    connector, monkeypatch
) -> None:
    monkeypatch.delenv("LIFEBOT_AGENT_KEY")
    FakeAsyncClient.responses = [FakeResponse(201, {"saved": True}) for _ in range(2)]
    await connector.save_study_plan(access_token="", plan=PLAN)
    await connector.post_reply(access_token="", text="hi")
    for _, _, kwargs in FakeAsyncClient.calls:
        assert kwargs["headers"] == {WRITE_HEADER: WKEY}


async def test_write_tools_need_the_api_url(connector, monkeypatch) -> None:
    monkeypatch.delenv("LIFEBOT_API_URL")
    with pytest.raises(ValueError, match="Lifebot API URL is not configured on the broker"):
        await connector.post_reply(access_token="", text="hi")
    assert FakeAsyncClient.calls == []


# === STUDY / MESSAGES: UPSTREAM ERROR MAPPING ===

READ_CALLS = [
    lambda c: c.get_study(access_token=""),
    lambda c: c.get_study_plan(access_token=""),
    lambda c: c.get_messages(access_token=""),
    lambda c: c.get_study_tasks(access_token=""),
    lambda c: c.get_course_curriculum(access_token="", course_id="5072786"),
]
WRITE_CALLS = [
    lambda c: c.save_study_plan(access_token="", plan=PLAN),
    lambda c: c.post_reply(access_token="", text="hi"),
    lambda c: c.save_study_tasks(access_token="", **TASKS_CALL),
]


@pytest.mark.parametrize("call", READ_CALLS)
async def test_read_route_401_and_503_name_the_read_key(connector, call) -> None:
    FakeAsyncClient.responses = [FakeResponse(401, {"error": "unauthorized"})]
    with pytest.raises(ValueError, match=r"agent key \(401\).*LIFEBOT_AGENT_KEY"):
        await call(connector)
    FakeAsyncClient.responses = [FakeResponse(503, {"error": "agent key not configured"})]
    with pytest.raises(ValueError, match=r"Lifebot's agent key is not configured on the Lifebot"):
        await call(connector)


@pytest.mark.parametrize("call", WRITE_CALLS)
async def test_write_route_401_names_the_write_key(connector, call) -> None:
    FakeAsyncClient.responses = [FakeResponse(401, {"error": "unauthorized"})]
    with pytest.raises(ValueError, match=r"WRITE key \(401\).*LIFEBOT_AGENT_WRITE_KEY"):
        await call(connector)


@pytest.mark.parametrize("call", WRITE_CALLS)
async def test_write_route_503_means_write_key_not_configured_upstream(connector, call) -> None:
    FakeAsyncClient.responses = [FakeResponse(503, {"error": "agent write key not configured"})]
    with pytest.raises(ValueError, match=r"write key is not configured on the Lifebot side"):
        await call(connector)


@pytest.mark.parametrize("call", WRITE_CALLS)
async def test_write_route_409_means_try_again(connector, call) -> None:
    FakeAsyncClient.responses = [FakeResponse(409, {"error": "conflict", "detail": "retry"})]
    with pytest.raises(ValueError) as excinfo:
        await call(connector)
    assert str(excinfo.value) == "Lifebot was updated at the same time; try again"


async def test_write_route_400_surfaces_the_api_detail(connector) -> None:
    detail = "focus[1].course_id duplicates an earlier focus item"
    FakeAsyncClient.responses = [FakeResponse(400, {"error": "invalid plan", "detail": detail})]
    with pytest.raises(ValueError, match=r"\(400\): focus\[1\]\.course_id duplicates"):
        await connector.save_study_plan(access_token="", plan=PLAN)
    FakeAsyncClient.responses = [
        FakeResponse(400, {"error": "invalid message", "detail": "text must be 1-8000 characters"})
    ]
    with pytest.raises(ValueError, match="text must be 1-8000 characters"):
        await connector.post_reply(access_token="", text="hi")


@pytest.mark.parametrize("call", READ_CALLS)
async def test_read_route_400_surfaces_the_api_detail(connector, call) -> None:
    FakeAsyncClient.responses = [
        FakeResponse(400, {"error": "invalid parameter", "detail": "limit must be 1-50"})
    ]
    with pytest.raises(ValueError, match=r"\(400\): limit must be 1-50"):
        await call(connector)


@pytest.mark.parametrize("call", [*READ_CALLS, *WRITE_CALLS])
async def test_429_and_other_statuses_map_on_new_routes(connector, call) -> None:
    FakeAsyncClient.responses = [FakeResponse(429, {"message": "Too Many Requests"})]
    with pytest.raises(ValueError, match="rate limited.*retry later"):
        await call(connector)
    FakeAsyncClient.responses = [FakeResponse(500, {"error": "internal error", "trace": "x"})]
    with pytest.raises(ValueError) as excinfo:
        await call(connector)
    assert str(excinfo.value) == "Lifebot API error 500"


@pytest.mark.parametrize("call", WRITE_CALLS)
async def test_write_transport_error_and_bad_body_are_readable(connector, call) -> None:
    FakeAsyncClient.responses = [httpx.ConnectError(f"connection failed to {BASE}/agent/x")]
    with pytest.raises(ValueError) as excinfo:
        await call(connector)
    assert "unreachable" in str(excinfo.value)
    assert BASE not in str(excinfo.value)
    FakeAsyncClient.responses = [FakeResponse(201, text="<html>oops</html>")]
    with pytest.raises(ValueError, match="invalid response"):
        await call(connector)


# === STUDY / MESSAGES: NEITHER KEY EVER LEAKS ===


async def test_neither_key_appears_in_errors_or_results(connector) -> None:
    echo = {"error": f"bad {KEY} {WKEY}", "detail": "ok", "message": f"{KEY}{WKEY}"}
    statuses = [400, 401, 409, 429, 500, 503]
    for call in [*READ_CALLS, *WRITE_CALLS]:
        scenarios: list[Any] = [FakeResponse(s, echo) for s in statuses]
        scenarios.append(httpx.ConnectError(f"failed with {KEY} and {WKEY}"))
        for scenario in scenarios:
            FakeAsyncClient.responses = [scenario]
            try:
                content = await call(connector)
            except ValueError as exc:
                assert KEY not in str(exc)
                assert WKEY not in str(exc)
            else:
                assert KEY not in json.dumps(content)
                assert WKEY not in json.dumps(content)


async def test_each_key_goes_only_to_its_own_routes(connector) -> None:
    FakeAsyncClient.responses = [FakeResponse(200, {}) for _ in READ_CALLS]
    FakeAsyncClient.responses += [FakeResponse(201, {}) for _ in WRITE_CALLS]
    for call in [*READ_CALLS, *WRITE_CALLS]:
        await call(connector)
    reads = FakeAsyncClient.calls[: len(READ_CALLS)]
    writes = FakeAsyncClient.calls[len(READ_CALLS) :]
    for method, _, kwargs in reads:
        assert method == "GET"
        _assert_only_key(kwargs, READ_HEADER, KEY)
    for method, _, kwargs in writes:
        assert method in {"PUT", "POST"}
        _assert_only_key(kwargs, WRITE_HEADER, WKEY)


async def test_keys_and_content_never_in_logs_via_the_mcp_dispatcher(connector, caplog) -> None:
    # Goes through handle_mcp_request, which is where tool failures are logged.
    private_text = "private-message-text-sentinel"
    private_goal = "private-goal-sentinel"
    calls: list[tuple[str, dict[str, Any], Any]] = [
        ("post_reply", {"text": private_text}, FakeResponse(401, {})),
        ("post_reply", {"text": private_text}, FakeResponse(409, {})),
        ("post_reply", {"text": ""}, FakeResponse(201, {})),  # validation failure
        ("save_study_plan", {"plan": {**PLAN, "goal": private_goal}}, FakeResponse(503, {})),
        ("save_study_plan", {"plan": {"goal": private_goal}}, FakeResponse(201, {})),
        ("save_study_plan", {"plan": PLAN}, FakeResponse(201, {"saved": True})),
        ("get_messages", {"conversation": "main"}, FakeResponse(401, {})),
        ("get_study", {"days": 30}, httpx.ConnectError(f"failed with {KEY}")),
    ]
    results = []
    with caplog.at_level("DEBUG"):
        for name, arguments, response in calls:
            FakeAsyncClient.responses = [response]
            results.append(
                await connector.handle_mcp_request(
                    method="tools/call",
                    params={"name": name, "arguments": arguments},
                    request_id=1,
                    access_token="",
                )
            )
    assert "Tool post_reply failed" in caplog.text  # failures are logged, so the checks below bite
    for leak in (KEY, WKEY, private_text, private_goal):
        assert leak not in caplog.text
        assert leak not in json.dumps(results)


async def test_jsonrpc_dispatch_reaches_the_write_tools(connector) -> None:
    FakeAsyncClient.responses = [FakeResponse(201, {"saved": True, "id": "m-9"})]
    result = await connector.handle_mcp_request(
        method="tools/call",
        params={"name": "post_reply", "arguments": {"text": "hi", "conversation": "main"}},
        request_id=3,
        access_token="",
    )
    assert "error" not in result
    method, url, kwargs = _last_call()
    assert (method, url) == ("POST", f"{BASE}/agent/messages/reply")
    assert kwargs["json"] == {"text": "hi", "conversation": "main"}


# === STUDY TASKS: HAPPY PATHS ===


async def test_get_study_tasks_defaults_send_no_params_and_the_read_key(connector) -> None:
    payload = {"from": "2026-11-22", "to": "2026-11-28", "days": [], "totals": {"tasks": 0}}
    FakeAsyncClient.responses = [FakeResponse(200, payload)]
    content = await connector.get_study_tasks(access_token="")
    method, url, kwargs = _last_call()
    assert (method, url) == ("GET", f"{BASE}/agent/study-tasks")
    assert kwargs["params"] == {}
    assert "json" not in kwargs
    _assert_only_key(kwargs, READ_HEADER, KEY)
    assert json.loads(content[0]["text"]) == payload


async def test_get_study_tasks_passes_from_and_to(connector) -> None:
    FakeAsyncClient.responses = [FakeResponse(200, {}), FakeResponse(200, {})]
    await connector.get_study_tasks(access_token="", **{"from": "2026-11-01"})
    assert _last_call()[2]["params"] == {"from": "2026-11-01"}
    await connector.get_study_tasks(access_token="", **{"from": "2026-11-01", "to": "2026-12-05"})
    assert _last_call()[2]["params"] == {"from": "2026-11-01", "to": "2026-12-05"}


async def test_save_study_tasks_puts_the_range_with_the_write_key_only(connector) -> None:
    payload = {
        "from": "2026-11-23",
        "to": "2026-11-29",
        "saved": 1,
        "kept_done": 0,
        "removed": 2,
        "unknown_course_ids": [],
    }
    FakeAsyncClient.responses = [FakeResponse(200, payload)]
    content = await connector.save_study_tasks(access_token="", **TASKS_CALL)
    method, url, kwargs = _last_call()
    assert (method, url) == ("PUT", f"{BASE}/agent/study-tasks")
    assert kwargs["json"] == TASKS_CALL
    assert "params" not in kwargs
    _assert_only_key(kwargs, WRITE_HEADER, WKEY)
    assert json.loads(content[0]["text"]) == payload


async def test_save_study_tasks_passes_full_tasks_through_unchanged(connector) -> None:
    tasks = [
        {
            "date": "2026-11-23",
            "title": "Lab: subnetting ☕",
            "minutes": 60,
            "kind": "lab",
            "url": "https://example.com/lab",
            "link_label": "Lab",
            "course_id": "course-a",
            "note": "Do the second exercise first",
        },
        {"date": "2026-11-24", "title": "Review", "minutes": 5, "kind": "review"},
    ]
    FakeAsyncClient.responses = [FakeResponse(200, {"saved": 2})]
    await connector.save_study_tasks(access_token="", **{**TASKS_CALL, "tasks": tasks})
    assert _last_call()[2]["json"]["tasks"] == tasks


async def test_study_task_tools_describe_lecture_ids_and_automatic_ticking(connector) -> None:
    result = await connector.handle_mcp_request(
        method="tools/list", params={}, request_id=1, access_token=""
    )
    tools = {t["name"]: t for t in result["result"]["tools"]}
    save = tools["save_study_tasks"]
    lecture_ids = save["inputSchema"]["properties"]["tasks"]["items"]["properties"]["lecture_ids"]
    assert lecture_ids["minItems"] == 1
    assert lecture_ids["maxItems"] == 20
    assert lecture_ids["uniqueItems"] is True
    assert lecture_ids["items"]["pattern"] == "^[A-Za-z0-9_-]{1,64}$"
    assert "course_id" in lecture_ids["description"]
    assert "get_course_curriculum" in lecture_ids["description"]
    assert "ticked automatically" in lecture_ids["description"]
    assert "ticked automatically" in save["description"]
    assert "auto_tick" in tools["get_study_tasks"]["description"]
    assert 'done_by: "udemy"' in tools["get_study_tasks"]["description"]


async def test_save_study_tasks_passes_lecture_ids_through_unchanged(connector) -> None:
    tasks = [
        {
            "date": "2026-11-23",
            "title": "Lectures 3-4",
            "minutes": 30,
            "course_id": "course-a",
            "lecture_ids": ["lec_1", "lec-2"],
        }
    ]
    FakeAsyncClient.responses = [FakeResponse(200, {"saved": 1})]
    await connector.save_study_tasks(access_token="", **{**TASKS_CALL, "tasks": tasks})
    assert _last_call()[2]["json"]["tasks"] == tasks


async def test_save_study_tasks_accepts_an_empty_list_and_sixty_tasks(connector) -> None:
    FakeAsyncClient.responses = [FakeResponse(200, {"saved": 0}), FakeResponse(200, {"saved": 60})]
    await connector.save_study_tasks(access_token="", **{**TASKS_CALL, "tasks": []})
    assert _last_call()[2]["json"]["tasks"] == []
    sixty = [TASK] * 60
    await connector.save_study_tasks(access_token="", **{**TASKS_CALL, "tasks": sixty})
    assert _last_call()[2]["json"]["tasks"] == sixty


# === STUDY TASKS: VALIDATION (rejected before any HTTP) ===


@pytest.mark.parametrize(
    "bad",
    [
        "2026-13-01",  # not a real date
        "2026-02-30",
        "2026-11-2",
        "2026-11-22\n",
        "٢٠٢٦-١١-٢٢",  # non-ASCII digits
        "tomorrow",
        "",
        20261122,
        None,
        ["2026-11-22"],
    ],
)
async def test_study_tasks_reject_bad_dates(connector, bad) -> None:
    with pytest.raises(ValueError, match="Invalid date"):
        await connector.get_study_tasks(access_token="", **{"from": bad})
    with pytest.raises(ValueError, match="Invalid date"):
        await connector.get_study_tasks(access_token="", to=bad)
    with pytest.raises(ValueError, match="Invalid date"):
        await connector.save_study_tasks(access_token="", **{**TASKS_CALL, "from": bad})
    with pytest.raises(ValueError, match="Invalid date"):
        await connector.save_study_tasks(access_token="", **{**TASKS_CALL, "to": bad})
    assert FakeAsyncClient.calls == []


@pytest.mark.parametrize(
    "tasks",
    [
        None,
        "tasks",
        {"date": "2026-11-23"},
        [TASK, "x"],
        [TASK, None],
        [[TASK]],
        [{"n": float("nan")}],
    ],
)
async def test_save_study_tasks_rejects_malformed_tasks(connector, tasks) -> None:
    with pytest.raises(ValueError, match="Invalid tasks"):
        await connector.save_study_tasks(access_token="", **{**TASKS_CALL, "tasks": tasks})
    assert FakeAsyncClient.calls == []


async def test_save_study_tasks_rejects_sixty_one_tasks(connector) -> None:
    with pytest.raises(ValueError, match="at most 60"):
        await connector.save_study_tasks(access_token="", **{**TASKS_CALL, "tasks": [TASK] * 61})
    assert FakeAsyncClient.calls == []


async def test_save_study_tasks_rejects_oversized_payload(connector) -> None:
    big = [{**TASK, "note": "x" * 2000} for _ in range(60)]
    with pytest.raises(ValueError, match="too large"):
        await connector.save_study_tasks(access_token="", **{**TASKS_CALL, "tasks": big})
    assert FakeAsyncClient.calls == []


@pytest.mark.parametrize("missing", ["from", "to", "tasks"])
async def test_save_study_tasks_requires_all_arguments(connector, missing) -> None:
    arguments = {key: value for key, value in TASKS_CALL.items() if key != missing}
    with pytest.raises(ValueError, match=f"Missing argument.*{missing}"):
        await connector.save_study_tasks(access_token="", **arguments)
    assert FakeAsyncClient.calls == []


async def test_unknown_arguments_are_rejected_before_any_http(connector) -> None:
    with pytest.raises(ValueError, match="Unknown argument.*days"):
        await connector.get_study_tasks(access_token="", days=7)
    with pytest.raises(ValueError, match="Unknown argument.*conversation"):
        await connector.save_study_tasks(access_token="", conversation="main", **TASKS_CALL)
    assert FakeAsyncClient.calls == []


async def test_study_tasks_validation_errors_do_not_echo_task_content(connector) -> None:
    secret = "private-task-title-sentinel"
    with pytest.raises(ValueError) as excinfo:
        await connector.save_study_tasks(
            access_token="", **{**TASKS_CALL, "tasks": [{"title": secret}, secret]}
        )
    assert secret not in str(excinfo.value)


# === STUDY TASKS: CONFIGURATION AND UPSTREAM ERRORS ===


async def test_save_study_tasks_needs_the_write_key_before_any_http(connector, monkeypatch) -> None:
    monkeypatch.delenv("LIFEBOT_AGENT_WRITE_KEY")
    with pytest.raises(ValueError, match="Lifebot write key is not configured on the broker"):
        await connector.save_study_tasks(access_token="", **TASKS_CALL)
    assert FakeAsyncClient.calls == []


async def test_study_tasks_tools_use_only_their_own_key(connector, monkeypatch) -> None:
    monkeypatch.delenv("LIFEBOT_AGENT_KEY")
    FakeAsyncClient.responses = [FakeResponse(200, {})]
    await connector.save_study_tasks(access_token="", **TASKS_CALL)
    assert _last_call()[2]["headers"] == {WRITE_HEADER: WKEY}
    monkeypatch.setenv("LIFEBOT_AGENT_KEY", KEY)
    monkeypatch.delenv("LIFEBOT_AGENT_WRITE_KEY")
    FakeAsyncClient.responses = [FakeResponse(200, {})]
    await connector.get_study_tasks(access_token="")
    assert _last_call()[2]["headers"] == {READ_HEADER: KEY}


async def test_save_study_tasks_400_surfaces_the_api_detail(connector) -> None:
    detail = "tasks[3].minutes must be an integer from 5 to 240"
    FakeAsyncClient.responses = [FakeResponse(400, {"error": "invalid tasks", "detail": detail})]
    with pytest.raises(ValueError) as excinfo:
        await connector.save_study_tasks(access_token="", **TASKS_CALL)
    assert str(excinfo.value) == f"Lifebot rejected a parameter (400): {detail}"


async def test_get_study_tasks_400_surfaces_the_api_detail(connector) -> None:
    detail = "range is longer than 35 days"
    FakeAsyncClient.responses = [FakeResponse(400, {"error": "invalid range", "detail": detail})]
    with pytest.raises(ValueError, match=r"\(400\): range is longer than 35 days"):
        await connector.get_study_tasks(access_token="")


async def test_jsonrpc_dispatch_reaches_the_study_tasks_tools(connector) -> None:
    FakeAsyncClient.responses = [FakeResponse(200, {"saved": 1}), FakeResponse(200, {"days": []})]
    saved = await connector.handle_mcp_request(
        method="tools/call",
        params={"name": "save_study_tasks", "arguments": TASKS_CALL},
        request_id=4,
        access_token="",
    )
    assert "error" not in saved
    method, url, kwargs = _last_call()
    assert (method, url) == ("PUT", f"{BASE}/agent/study-tasks")
    assert kwargs["json"] == TASKS_CALL
    got = await connector.handle_mcp_request(
        method="tools/call",
        params={"name": "get_study_tasks", "arguments": {"from": "2026-11-23"}},
        request_id=5,
        access_token="",
    )
    assert "error" not in got
    assert _last_call()[2]["params"] == {"from": "2026-11-23"}
    bad = await connector.handle_mcp_request(
        method="tools/call",
        params={"name": "save_study_tasks", "arguments": {"from": "2026-11-23"}},
        request_id=6,
        access_token="",
    )
    assert bad["result"]["isError"] is True
    assert "Missing argument" in bad["result"]["content"][0]["text"]


# === COURSE CURRICULUM ===


async def test_get_course_curriculum_schema(connector) -> None:
    result = await connector.handle_mcp_request(
        method="tools/list", params={}, request_id=1, access_token=""
    )
    tool = next(t for t in result["result"]["tools"] if t["name"] == "get_course_curriculum")
    assert tool["inputSchema"]["required"] == ["course_id"]
    assert sorted(tool["inputSchema"]["properties"]) == ["brief", "course_id"]
    assert tool["inputSchema"]["properties"]["brief"]["type"] == "boolean"
    assert "404" in tool["description"]


async def test_get_course_curriculum_sends_course_id_and_the_read_key(connector) -> None:
    payload = {
        "course_id": "5072786",
        "totals": {"items": 2, "done": 1},
        "next": [],
        "sections": [],
    }
    FakeAsyncClient.responses = [FakeResponse(200, payload)]
    content = await connector.get_course_curriculum(access_token="", course_id="5072786")
    method, url, kwargs = _last_call()
    assert (method, url) == ("GET", f"{BASE}/agent/study-curriculum")
    assert kwargs["params"] == {"course_id": "5072786"}
    assert "json" not in kwargs
    _assert_only_key(kwargs, READ_HEADER, KEY)
    assert json.loads(content[0]["text"]) == payload


async def test_get_course_curriculum_sends_brief_only_when_true(connector) -> None:
    FakeAsyncClient.responses = [FakeResponse(200, {}), FakeResponse(200, {})]
    await connector.get_course_curriculum(access_token="", course_id="c-1_a", brief=True)
    assert _last_call()[2]["params"] == {"course_id": "c-1_a", "brief": "true"}
    await connector.get_course_curriculum(access_token="", course_id="c-1_a", brief=False)
    assert _last_call()[2]["params"] == {"course_id": "c-1_a"}


@pytest.mark.parametrize("course_id", ["a", "A-z_9", "a" * 64])
async def test_course_id_bounds_are_inclusive(connector, course_id) -> None:
    FakeAsyncClient.responses = [FakeResponse(200, {})]
    await connector.get_course_curriculum(access_token="", course_id=course_id)
    assert _last_call()[2]["params"] == {"course_id": course_id}


@pytest.mark.parametrize(
    "course_id",
    [
        "",
        "a" * 65,
        "5072786\n",
        " 5072786",
        "a/b",
        "a.b",
        "a b",
        "5072786&brief=true",  # query injection
        "５０７２７８６",  # non-ASCII digits
        5072786,
        None,
        ["a"],
    ],
)
async def test_bad_course_id_rejected_without_echo(connector, course_id) -> None:
    with pytest.raises(ValueError, match="Invalid course_id") as excinfo:
        await connector.get_course_curriculum(access_token="", course_id=course_id)
    assert "5072786" not in str(excinfo.value)
    assert FakeAsyncClient.calls == []


@pytest.mark.parametrize("brief", ["true", 1, 0, "yes", [True]])
async def test_non_boolean_brief_rejected(connector, brief) -> None:
    with pytest.raises(ValueError, match="Invalid brief"):
        await connector.get_course_curriculum(access_token="", course_id="c1", brief=brief)
    assert FakeAsyncClient.calls == []


async def test_course_curriculum_404_says_no_lecture_list_synced(connector) -> None:
    FakeAsyncClient.responses = [
        FakeResponse(404, {"error": "no curriculum", "detail": "secret-internals"})
    ]
    with pytest.raises(ValueError, match="No lecture list has been synced") as excinfo:
        await connector.get_course_curriculum(access_token="", course_id="c1")
    assert "secret-internals" not in str(excinfo.value)


async def test_course_curriculum_other_errors_use_the_shared_mapping(connector) -> None:
    FakeAsyncClient.responses = [FakeResponse(400, {"detail": "course_id is malformed"})]
    with pytest.raises(ValueError, match=r"\(400\): course_id is malformed"):
        await connector.get_course_curriculum(access_token="", course_id="c1")
    FakeAsyncClient.responses = [FakeResponse(401, {})]
    with pytest.raises(ValueError, match=r"agent key \(401\).*LIFEBOT_AGENT_KEY"):
        await connector.get_course_curriculum(access_token="", course_id="c1")
    FakeAsyncClient.responses = [FakeResponse(503, {})]
    with pytest.raises(ValueError, match="not configured on the Lifebot side"):
        await connector.get_course_curriculum(access_token="", course_id="c1")


async def test_jsonrpc_dispatch_reaches_get_course_curriculum(connector) -> None:
    FakeAsyncClient.responses = [FakeResponse(200, {"course_id": "c1"})]
    result = await connector.handle_mcp_request(
        method="tools/call",
        params={"name": "get_course_curriculum", "arguments": {"course_id": "c1", "brief": True}},
        request_id=8,
        access_token="",
    )
    assert "error" not in result
    assert _last_call()[2]["params"] == {"course_id": "c1", "brief": "true"}
    FakeAsyncClient.responses = [FakeResponse(404, {})]
    missing = await connector.handle_mcp_request(
        method="tools/call",
        params={"name": "get_course_curriculum", "arguments": {"course_id": "c2"}},
        request_id=9,
        access_token="",
    )
    assert missing["result"]["isError"] is True
    assert "No lecture list has been synced" in missing["result"]["content"][0]["text"]
