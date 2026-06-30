"""
Cloudflare MCP Connector

Cloudflare's official remote MCP server (Discovery flavour, RFC 8414 + 7591).
Auto-registers on import via BaseConnector.__init_subclass__.

Probe evidence (2026-06-29):
  curl https://mcp.cloudflare.com/.well-known/oauth-authorization-server
  → 200 JSON with registration_endpoint → Discovery selected.
  resource_name: "Cloudflare API MCP Server"
"""

from __future__ import annotations

from broker.connectors.base import BaseConnector
from broker.models.connector_config import ConnectorMeta

_CF_BASE = "https://mcp.cloudflare.com"


class CloudflareConnector(BaseConnector):
    """Cloudflare remote MCP connector (Discovery flavour).

    Uses MCP OAuth discovery (RFC 8414 + RFC 7591):
    - Endpoints discovered via .well-known at mcp.cloudflare.com
    - Client registered dynamically via /register
    - No manual Cloudflare app registration required

    Covers: DNS records, zones, firewall rules, Workers, Tunnels, and the
    full Cloudflare API exposed by the hosted MCP server.
    """

    meta = ConnectorMeta(
        name="cloudflare",
        display_name="Cloudflare",
        # Base URL only — the proxy appends the route path from the request,
        # so an MCP client posting to `/proxy/cloudflare/mcp` ends up at
        # `https://mcp.cloudflare.com/mcp`. Including `/mcp` here would produce
        # the double-segment `/mcp/mcp` and 404 from Cloudflare.
        mcp_url=_CF_BASE,
        mcp_transport="streamable_http",
        # Static URLs are ignored at runtime for endpoint routing (the discovered
        # endpoints are used) but are still REQUIRED by the ConnectorMeta validator
        # for auth_mode="broker" — removing them raises ValueError at startup.
        oauth_authorize_url=f"{_CF_BASE}/authorize",
        oauth_token_url=f"{_CF_BASE}/token",  # noqa: S106 — endpoint URL, not a password
        scopes=(),
        # Discovery — triggers MCP OAuth flow (RFC 8414 + RFC 7591).
        mcp_oauth_url=_CF_BASE,
    )

    # Standard Discovery flow — no hook overrides needed.
    # Cloudflare supports both client_secret_basic and client_secret_post
    # (token_endpoint_auth_methods_supported in the AS metadata), so the
    # broker's default client_secret_post works without override.
