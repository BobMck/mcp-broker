# Cloudflare — OAuth Setup (Discovery)

Cloudflare's hosted MCP server at `https://mcp.cloudflare.com` supports RFC 8414
endpoint discovery and RFC 7591 dynamic client registration. No OAuth app
registration is required up-front — the broker registers itself per-app on first
connect.

## Chosen Server

**Base URL:** `https://mcp.cloudflare.com`  
**MCP endpoint:** `https://mcp.cloudflare.com/mcp`  
**Resource name:** Cloudflare API MCP Server  
**Transport:** `streamable_http`  
**Flavour:** Discovery (RFC 8414 + RFC 7591)

### Tools exposed

The Cloudflare API MCP server provides full Cloudflare API coverage, including:

- DNS records (list, create, update, delete)
- Zones (list, create, settings)
- Firewall rules and WAF configuration
- Workers (deploy, list, delete)
- Cloudflare Tunnels (create, route, list)
- R2 buckets, KV namespaces, D1 databases
- Accounts and users

## Probe Evidence (verified 2026-06-29)

```bash
curl -fsSL https://mcp.cloudflare.com/.well-known/oauth-authorization-server
# → 200 JSON:
# {
#   "issuer": "https://mcp.cloudflare.com",
#   "authorization_endpoint": "https://mcp.cloudflare.com/authorize",
#   "token_endpoint": "https://mcp.cloudflare.com/token",
#   "registration_endpoint": "https://mcp.cloudflare.com/register",   ← Discovery
#   "response_types_supported": ["code"],
#   "grant_types_supported": ["authorization_code", "refresh_token"],
#   "token_endpoint_auth_methods_supported": ["client_secret_basic", "client_secret_post", "none"],
#   "code_challenge_methods_supported": ["plain", "S256"]
# }

curl -fsSL https://mcp.cloudflare.com/.well-known/oauth-protected-resource
# → 200 JSON:
# {
#   "resource": "https://mcp.cloudflare.com",
#   "authorization_servers": ["https://mcp.cloudflare.com"],
#   "bearer_methods_supported": ["header"],
#   "resource_name": "Cloudflare API MCP Server"
# }
```

`registration_endpoint` present → **Discovery** flavour selected.

## Broker Configuration

1. Add to `settings.yaml`:
   ```yaml
   broker:
     connectors: [..., cloudflare]
   ```

   No `apps` entry needed — credentials are minted dynamically via RFC 7591.

2. Connect: `./start connect` (interactive — select `cloudflare` from the connector menu).  
   For a non-default app: `./start connect --app {client_id:app_id}`.

## Required Scopes

Cloudflare's MCP server determines scopes during the authorization flow based on
the Cloudflare account permissions. No scopes need to be declared at registration
time — the `scopes` field is empty and the AS returns the granted scopes in the
token response.

Users will be prompted in the Cloudflare authorization UI to grant access to their
account resources.

## Provider Quirks

- Cloudflare supports both `client_secret_basic` and `client_secret_post` for
  token exchange — the broker's default (`client_secret_post`) works without an
  override.
- PKCE S256 is supported (listed in `code_challenge_methods_supported`).
- `client_id_metadata_document_supported: false` — the broker does not need to
  serve a client metadata document.
- Revocation endpoint reuses the token endpoint (`https://mcp.cloudflare.com/token`).

## Known Limitations

- Dynamic client secrets may expire — if `client_secret_expires_at` is non-zero in
  the registration response, re-registration will be needed after expiry.
- Full tool coverage depends on the Cloudflare account plan (some enterprise features
  may not be accessible via the MCP server).
