"""Cloudflare connector unit tests — registration and Discovery-flavour metadata."""

from __future__ import annotations

import pytest

from broker.connectors.registry import ConnectorRegistry


@pytest.fixture(autouse=True)
def clear_registry():
    ConnectorRegistry.clear()
    yield
    ConnectorRegistry.clear()


@pytest.fixture
def cloudflare_connector():
    from connectors.cloudflare.adapter import CloudflareConnector

    connector = ConnectorRegistry.get("cloudflare")
    if connector is None:
        ConnectorRegistry.auto_register(CloudflareConnector)
        connector = ConnectorRegistry.get("cloudflare")
    assert connector is not None
    return connector


class TestRegistration:
    def test_auto_registers_with_name_cloudflare(self, cloudflare_connector):
        assert cloudflare_connector.meta.name == "cloudflare"

    def test_display_name(self, cloudflare_connector):
        assert cloudflare_connector.meta.display_name == "Cloudflare"

    def test_is_discovery_flavour(self, cloudflare_connector):
        assert cloudflare_connector.meta.uses_discovery is True
        assert cloudflare_connector.meta.mcp_oauth_url == "https://mcp.cloudflare.com"
