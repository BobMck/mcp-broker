"""
GitHub MCP Connector

GitHub's official remote MCP server. Static flavour: the GitHub remote MCP
server does not expose RFC 8414 / RFC 7591 discovery endpoints (probed
2026-06-29: both /.well-known/oauth-authorization-server and
/.well-known/oauth-protected-resource return 404), so the broker uses a
manually registered GitHub OAuth app — see SETUP.md.
Auto-registers on import via BaseConnector.__init_subclass__.
"""

from __future__ import annotations

from broker.connectors.base import BaseConnector
from broker.models.connector_config import ConnectorMeta


class GitHubConnector(BaseConnector):
    """GitHub remote MCP connector (Static flavour)."""

    meta = ConnectorMeta(
        name="github",
        display_name="GitHub",
        # Base URL only — the proxy appends the request's route path. Mirrors the
        # Notion adapter's note: putting the full /mcp path here can double the
        # segment. If the server 404s on tools/list during Task 4's live deploy,
        # switch to the full path form (https://api.githubcopilot.com/mcp).
        mcp_url="https://api.githubcopilot.com",
        mcp_transport="streamable_http",
        oauth_authorize_url="https://github.com/login/oauth/authorize",
        oauth_token_url="https://github.com/login/oauth/access_token",  # noqa: S106 — endpoint URL, not a password
        scopes=["repo", "read:org"],
    )
