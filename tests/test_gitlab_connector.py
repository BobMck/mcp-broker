"""GitLab native connector — PAT auth, read-only tools, validation, error mapping."""

from __future__ import annotations

import json
from typing import Any

import pytest

import connectors.gitlab.adapter as gl_adapter
from broker.connectors.registry import ConnectorRegistry

# === FAKE HTTPX (same shape as tests/test_tailscale_connector.py) ===


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

    async def post(self, url: str, **kwargs: Any) -> FakeResponse:
        return await self.request("POST", url, **kwargs)


@pytest.fixture(autouse=True)
def _reset(monkeypatch: pytest.MonkeyPatch):
    FakeAsyncClient.calls = []
    FakeAsyncClient.responses = []
    monkeypatch.setattr(gl_adapter.httpx, "AsyncClient", FakeAsyncClient)
    monkeypatch.setenv("GITLAB_TOKEN", "glpat-xxx")
    monkeypatch.delenv("GITLAB_URL", raising=False)


@pytest.fixture
def connector():
    instance = ConnectorRegistry.get("gitlab")
    if instance is None:  # another test module cleared the registry
        ConnectorRegistry.auto_register(gl_adapter.GitlabConnector)
        instance = ConnectorRegistry.get("gitlab")
    assert instance is not None
    return instance


# === TESTS ===


async def test_exactly_the_specified_tools_are_registered(connector) -> None:
    result = await connector.handle_mcp_request(
        method="tools/list", params={}, request_id=1, access_token=""
    )
    names = sorted(t["name"] for t in result["result"]["tools"])
    assert names == [
        "get_project",
        "list_issues",
        "list_merge_requests",
        "list_pipelines",
        "list_projects",
    ]


async def test_list_projects_sends_token_and_membership(connector) -> None:
    FakeAsyncClient.responses = [
        FakeResponse(
            200,
            [
                {
                    "id": 42,
                    "path_with_namespace": "bob/infra",
                    "name": "infra",
                    "visibility": "private",
                    "default_branch": "main",
                    "web_url": "https://gitlab.com/bob/infra",
                    "last_activity_at": "2026-07-09T12:00:00Z",
                    "archived": False,
                }
            ],
        )
    ]
    content = await connector.list_projects(access_token="")
    _, url, kwargs = FakeAsyncClient.calls[-1]
    assert url == "https://gitlab.com/api/v4/projects"
    assert kwargs["headers"] == {"PRIVATE-TOKEN": "glpat-xxx"}
    assert kwargs["params"]["membership"] == "true"
    project = json.loads(content[0]["text"])[0]
    assert project == {
        "id": 42,
        "path": "bob/infra",
        "name": "infra",
        "visibility": "private",
        "default_branch": "main",
        "web_url": "https://gitlab.com/bob/infra",
        "last_activity_at": "2026-07-09T12:00:00Z",
        "archived": False,
    }


async def test_get_project_url_encodes_path(connector) -> None:
    FakeAsyncClient.responses = [FakeResponse(200, {"id": 7, "path_with_namespace": "g/s/p"})]
    await connector.get_project(access_token="", project="g/s/p")
    _, url, _ = FakeAsyncClient.calls[-1]
    assert url == "https://gitlab.com/api/v4/projects/g%2Fs%2Fp"


async def test_per_page_is_clamped(connector) -> None:
    FakeAsyncClient.responses = [FakeResponse(200, [])]
    await connector.list_projects(access_token="", per_page=9999)
    _, _, kwargs = FakeAsyncClient.calls[-1]
    assert kwargs["params"]["per_page"] == 100


async def test_self_hosted_url_is_honored(connector, monkeypatch) -> None:
    monkeypatch.setenv("GITLAB_URL", "https://gitlab.example.com/")
    FakeAsyncClient.responses = [FakeResponse(200, [])]
    await connector.list_projects(access_token="")
    _, url, _ = FakeAsyncClient.calls[-1]
    assert url == "https://gitlab.example.com/api/v4/projects"


async def test_invalid_project_ref_rejected_before_any_http(connector) -> None:
    with pytest.raises(ValueError, match="Invalid project id or path"):
        await connector.get_project(access_token="", project="../evil?x=1")
    assert FakeAsyncClient.calls == []


async def test_invalid_issue_state_rejected_before_any_http(connector) -> None:
    with pytest.raises(ValueError, match="Invalid issue state"):
        await connector.list_issues(access_token="", project="bob/infra", state="bogus")
    assert FakeAsyncClient.calls == []


async def test_missing_token_is_a_readable_error(connector, monkeypatch) -> None:
    monkeypatch.delenv("GITLAB_TOKEN")
    with pytest.raises(ValueError, match="GitLab token is not configured"):
        await connector.list_projects(access_token="")
    assert FakeAsyncClient.calls == []


async def test_upstream_error_maps_to_sanitized_valueerror(connector) -> None:
    FakeAsyncClient.responses = [FakeResponse(404, {"message": "404 Project Not Found"})]
    with pytest.raises(ValueError, match="GitLab API error 404: 404 Project Not Found"):
        await connector.get_project(access_token="", project="bob/missing")


async def test_list_pipelines_orders_desc(connector) -> None:
    FakeAsyncClient.responses = [
        FakeResponse(200, [{"id": 99, "status": "success", "ref": "main", "sha": "abc"}])
    ]
    content = await connector.list_pipelines(access_token="", project="bob/infra")
    _, url, kwargs = FakeAsyncClient.calls[-1]
    assert url == "https://gitlab.com/api/v4/projects/bob%2Finfra/pipelines"
    assert kwargs["params"]["sort"] == "desc"
    pipeline = json.loads(content[0]["text"])[0]
    assert pipeline["id"] == 99
    assert pipeline["status"] == "success"
