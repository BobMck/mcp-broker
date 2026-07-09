# GitLab Connector Setup

Native connector (`auth_mode="none"`), credential = a Personal Access Token
scoped to `read_api`. Read-only: projects, issues, merge requests, pipelines.

## Create the token (GitLab — manual)

1. https://gitlab.com/-/user_settings/personal_access_tokens → **Add new token**
   (self-hosted: `<your-instance>/-/user_settings/personal_access_tokens`).
2. Scope: **`read_api`** only. Do NOT grant `api`, `write_repository`, etc. —
   the connector exposes no write tools, and a `read_api` token cannot mutate.
3. Set an expiry and copy the token (shown once).

## Environment

| Var | Required | Meaning |
|---|---|---|
| `GITLAB_TOKEN` | yes | Personal Access Token (`read_api` scope) |
| `GITLAB_URL` | no (default `https://gitlab.com`) | Base URL; set for self-hosted GitLab |

## Enable

Add `gitlab` to `broker.connectors` and to the app's `allowed_connectors` in settings.
