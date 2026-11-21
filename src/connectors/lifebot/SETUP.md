# Lifebot Connector Setup

Native connector (`auth_mode="none"`), credential = a static agent key for the
Lifebot read-only agent API. It lets an AI assistant read compact, pre-computed
summaries of the owner's personal health data: a daily brief, metric time series,
workouts and goals. Read-only: the upstream API is GET-only and there are no write
tools. No raw health rows are exposed.

## Get the key (Lifebot side — manual)

1. Deploy the Lifebot agent API and note its base URL (for example
   `https://lifebot.example.com`).
2. Generate an agent key on the Lifebot side (a long random string).
3. Store the key in your secret manager and load it into the broker's environment.
   Do not commit it.

To rotate: generate a new key on the Lifebot side, update the secret in your secret
manager and the broker's environment, then restart or redeploy the broker. The old
key stops working once the Lifebot side picks up the new one (allow a few minutes).
A `401` from the connector means the broker's key is wrong or stale.

## Environment

| Var | Required | Meaning |
|---|---|---|
| `LIFEBOT_API_URL` | yes | Base URL of the Lifebot agent API, e.g. `https://lifebot.example.com`. Must be `https://`; a trailing slash is stripped |
| `LIFEBOT_AGENT_KEY` | yes | Agent key (secret). Sent only in the `x-lifebot-agent-key` header |

## Tools

| Tool | Arguments | Returns |
|---|---|---|
| `get_brief` | `date` (optional, `YYYY-MM-DD`) | Daily summary: data freshness, core metrics with 7/30-day averages and goal progress, last night's sleep, training rollup, recent personal records, notable changes |
| `get_metrics` | `names` (1-10, list or comma-separated), `days` (optional, 1-365) | Daily time series per metric, plus names with no data |
| `get_workouts` | `days` (optional, 1-365), `detail` (optional, boolean) | Recent workouts, optionally with per-exercise sets |
| `get_goals` | none | Stored goals |

Dates are the owner's local calendar dates. A missing field means no data, not zero.

## Enable

Add `lifebot` to `broker.connectors` and to the app's `allowed_connectors` in settings.
