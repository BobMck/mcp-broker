# Tailscale Connector Setup

Native connector (`auth_mode="none"`), credential = a scoped OAuth client.

## Create the OAuth client (admin console — manual)

1. https://login.tailscale.com/admin/settings/oauth → **Generate OAuth client**.
2. Scope: **devices:core** (read + write — write is needed only for key expiry).
   Do NOT grant any other scope.
3. Copy the client id and secret. The secret is shown once.

## Environment

| Var | Required | Meaning |
|---|---|---|
| `TAILSCALE_OAUTH_CLIENT_ID` | yes | OAuth client id |
| `TAILSCALE_OAUTH_CLIENT_SECRET` | yes | OAuth client secret |
| `TAILSCALE_TAILNET` | no (default `-`) | Tailnet name; `-` = the client's own tailnet |

## Enable

Add `tailscale` to `broker.connectors` and to the app's `allowed_connectors` in settings.
