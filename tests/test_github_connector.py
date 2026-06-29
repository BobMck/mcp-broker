"""GitHub connector unit tests — registration and Static-flavour metadata."""

from __future__ import annotations

import pytest

from broker.connectors.registry import ConnectorRegistry


@pytest.fixture(autouse=True)
def clear_registry():
    ConnectorRegistry.clear()
    yield
    ConnectorRegistry.clear()


@pytest.fixture
def github_connector():
    from connectors.github.adapter import GitHubConnector

    connector = ConnectorRegistry.get("github")
    if connector is None:
        ConnectorRegistry.auto_register(GitHubConnector)
        connector = ConnectorRegistry.get("github")
    assert connector is not None
    return connector


class TestRegistration:
    def test_auto_registers_with_name_github(self, github_connector):
        assert github_connector.meta.name == "github"

    def test_display_name(self, github_connector):
        assert github_connector.meta.display_name == "GitHub"

    def test_is_static_flavour(self, github_connector):
        # No mcp_oauth_url => Static (manual OAuth app required, no dynamic
        # registration). ConnectorMeta exposes no `requires_oauth`; Static is
        # the absence of discovery plus statically declared OAuth endpoints.
        assert github_connector.meta.mcp_oauth_url is None
        assert github_connector.meta.uses_discovery is False

    def test_static_oauth_config(self, github_connector):
        meta = github_connector.meta
        assert meta.oauth_authorize_url == "https://github.com/login/oauth/authorize"
        assert meta.oauth_token_url == "https://github.com/login/oauth/access_token"
        assert set(meta.scopes) == {"repo", "read:org"}
