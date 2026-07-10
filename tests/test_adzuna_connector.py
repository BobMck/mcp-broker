"""Adzuna native connector — app_id/app_key auth, read-only tools, validation, error mapping."""

from __future__ import annotations

import json
from typing import Any

import pytest

import connectors.adzuna.adapter as az_adapter
from broker.connectors.registry import ConnectorRegistry

# === FAKE HTTPX (same shape as tests/test_gitlab_connector.py) ===


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

    async def __aenter__(self) -> FakeAsyncClient:
        return self

    async def __aexit__(self, *_: Any) -> bool:
        return False

    async def request(self, method: str, url: str, **kwargs: Any) -> FakeResponse:
        FakeAsyncClient.calls.append((method, url, kwargs))
        return FakeAsyncClient.responses.pop(0)

    async def get(self, url: str, **kwargs: Any) -> FakeResponse:
        return await self.request("GET", url, **kwargs)


@pytest.fixture(autouse=True)
def _reset(monkeypatch: pytest.MonkeyPatch):
    FakeAsyncClient.calls = []
    FakeAsyncClient.responses = []
    monkeypatch.setattr(az_adapter.httpx, "AsyncClient", FakeAsyncClient)
    monkeypatch.setenv("ADZUNA_APP_ID", "aid")
    monkeypatch.setenv("ADZUNA_APP_KEY", "akey-secret")
    monkeypatch.delenv("ADZUNA_COUNTRY", raising=False)


@pytest.fixture
def connector():
    instance = ConnectorRegistry.get("adzuna")
    if instance is None:  # another test module cleared the registry
        ConnectorRegistry.auto_register(az_adapter.AdzunaConnector)
        instance = ConnectorRegistry.get("adzuna")
    assert instance is not None
    return instance


# === TESTS ===


async def test_exactly_the_specified_tools_are_registered(connector) -> None:
    result = await connector.handle_mcp_request(
        method="tools/list", params={}, request_id=1, access_token=""
    )
    names = sorted(t["name"] for t in result["result"]["tools"])
    assert names == [
        "list_categories",
        "salary_histogram",
        "salary_history",
        "search_jobs",
        "top_companies",
    ]


async def test_search_sends_credentials_default_country_and_summarizes(connector) -> None:
    FakeAsyncClient.responses = [
        FakeResponse(
            200,
            {
                "count": 1,
                "results": [
                    {
                        "id": "42",
                        "title": "Python Engineer",
                        "company": {"display_name": "Acme"},
                        "location": {"display_name": "London"},
                        "category": {"label": "IT Jobs"},
                        "salary_min": 60000,
                        "salary_max": 80000,
                        "contract_type": "permanent",
                        "created": "2026-07-09T00:00:00Z",
                        "redirect_url": "https://adzuna/job/42",
                    }
                ],
            },
        )
    ]
    content = await connector.search_jobs(access_token="", what="python", category="it-jobs")
    _, url, kwargs = FakeAsyncClient.calls[-1]
    assert url == "https://api.adzuna.com/v1/api/jobs/gb/search/1"  # default country gb
    assert kwargs["params"]["app_id"] == "aid"
    assert kwargs["params"]["app_key"] == "akey-secret"
    assert kwargs["params"]["what"] == "python"
    payload = json.loads(content[0]["text"])
    assert payload["count"] == 1
    assert payload["results"][0] == {
        "id": "42",
        "title": "Python Engineer",
        "company": "Acme",
        "location": "London",
        "category": "IT Jobs",
        "salary_min": 60000,
        "salary_max": 80000,
        "contract_type": "permanent",
        "created": "2026-07-09T00:00:00Z",
        "url": "https://adzuna/job/42",
    }


async def test_country_param_overrides_env_default(connector, monkeypatch) -> None:
    monkeypatch.setenv("ADZUNA_COUNTRY", "us")
    FakeAsyncClient.responses = [FakeResponse(200, {"results": []})]
    await connector.search_jobs(access_token="", country="fr")
    _, url, _ = FakeAsyncClient.calls[-1]
    assert url == "https://api.adzuna.com/v1/api/jobs/fr/search/1"  # explicit param wins


async def test_env_default_country_used_when_no_param(connector, monkeypatch) -> None:
    monkeypatch.setenv("ADZUNA_COUNTRY", "us")
    FakeAsyncClient.responses = [FakeResponse(200, {"results": []})]
    await connector.search_jobs(access_token="")
    _, url, _ = FakeAsyncClient.calls[-1]
    assert url == "https://api.adzuna.com/v1/api/jobs/us/search/1"


async def test_invalid_country_rejected_before_any_http(connector) -> None:
    with pytest.raises(ValueError, match="Unsupported country"):
        await connector.search_jobs(access_token="", country="xx")
    assert FakeAsyncClient.calls == []


async def test_results_per_page_clamped(connector) -> None:
    FakeAsyncClient.responses = [FakeResponse(200, {"results": []})]
    await connector.search_jobs(access_token="", results_per_page=999)
    _, _, kwargs = FakeAsyncClient.calls[-1]
    assert kwargs["params"]["results_per_page"] == 50


async def test_missing_credentials_is_a_readable_error(connector, monkeypatch) -> None:
    monkeypatch.delenv("ADZUNA_APP_KEY")
    with pytest.raises(ValueError, match="Adzuna app_id/app_key are not configured"):
        await connector.search_jobs(access_token="")
    assert FakeAsyncClient.calls == []


async def test_upstream_error_sanitized_and_no_key_leak(connector) -> None:
    FakeAsyncClient.responses = [
        FakeResponse(400, {"display": "bad request", "exception": "AUTH_FAIL"})
    ]
    with pytest.raises(ValueError) as ei:
        await connector.search_jobs(access_token="", what="x")
    msg = str(ei.value)
    assert "Adzuna API error 400: bad request" in msg
    assert "akey-secret" not in msg  # the app_key must never appear in an error


async def test_list_categories(connector) -> None:
    FakeAsyncClient.responses = [
        FakeResponse(200, {"results": [{"tag": "it-jobs", "label": "IT Jobs"}]})
    ]
    content = await connector.list_categories(access_token="")
    _, url, _ = FakeAsyncClient.calls[-1]
    assert url == "https://api.adzuna.com/v1/api/jobs/gb/categories"
    assert json.loads(content[0]["text"]) == [{"tag": "it-jobs", "label": "IT Jobs"}]


async def test_salary_history_clamps_months(connector) -> None:
    FakeAsyncClient.responses = [FakeResponse(200, {"month": {"2026-06": 65000}})]
    content = await connector.salary_history(access_token="", what="python", months=999)
    _, url, kwargs = FakeAsyncClient.calls[-1]
    assert url == "https://api.adzuna.com/v1/api/jobs/gb/history"
    assert kwargs["params"]["months"] == 24
    assert json.loads(content[0]["text"]) == {"2026-06": 65000}


async def test_empty_optional_params_are_dropped(connector) -> None:
    FakeAsyncClient.responses = [FakeResponse(200, {"results": []})]
    await connector.search_jobs(access_token="", what="python")  # where/category empty
    _, _, kwargs = FakeAsyncClient.calls[-1]
    assert "where" not in kwargs["params"] and "category" not in kwargs["params"]
    assert kwargs["params"]["what"] == "python"
