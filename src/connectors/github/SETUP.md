# GitHub MCP Connector — OAuth Setup (Static)

**Connector name:** `github`
**Flavour:** Static (manually registered GitHub OAuth app).

GitHub's remote MCP server (`https://api.githubcopilot.com`) does **not** expose
RFC 8414 / RFC 7591 discovery endpoints. Probed 2026-06-29:

```bash
curl -fsSL https://api.githubcopilot.com/.well-known/oauth-authorization-server  # 404
curl -fsSL https://api.githubcopilot.com/.well-known/oauth-protected-resource    # 404
```

Because discovery is unavailable, the broker uses a GitHub OAuth app whose
`client_id` / `client_secret` you register up front (below).

## OAuth App Registration

1. Sign in to GitHub and go to **Settings → Developer settings → OAuth Apps → New OAuth App**.
2. Create a new OAuth application.
3. Set the **Authorization callback URL** to `https://bobsmcp.uk/oauth/github/callback`.
4. Record the **Client ID** and generate a **Client Secret**.

## Required Scopes

| Scope | Justification |
|-------|---------------|
| `repo` | Read/write access to repositories the GitHub MCP tools operate on (issues, PRs, file contents). The MCP server requires repo access for its core tooling. |
| `read:org` | Read-only access to organization membership/teams so org-scoped repositories and resources resolve correctly. |

No privileged scopes beyond these are requested.

## Broker Configuration

1. Add credentials to `.env` (or, in a templated deployment, to `.env.j2`
   / sourced from a secrets manager — do not commit real values):
   ```
   GITHUB_CLIENT_ID=...
   GITHUB_CLIENT_SECRET=...
   ```

2. Add to `settings.yaml`:
   ```yaml
   broker:
     connectors: [..., github]
   apps:
     your_app_id:
       connectors:
         github:
           client_id: ${GITHUB_CLIENT_ID}
           client_secret: ${GITHUB_CLIENT_SECRET}
   ```

3. Connect: `./start connect` (interactive — select `github` from the connector
   menu, which opens the browser for OAuth). For a non-default app, add
   `--app {client_id:app_id}`.

## Provider Quirks

Standard OAuth2 with `client_secret_post` — no adapter hook overrides needed.

## Known Limitations

- `mcp_url` is the base origin (`https://api.githubcopilot.com`); the proxy
  appends the request's route path. If `tools/list` 404s after a live deploy,
  switch to the full path form (`https://api.githubcopilot.com/mcp`). See the
  comment in `adapter.py`.
