# Lifebot Connector Setup

Native connector (`auth_mode="none"`), credentials = two static keys for the
Lifebot agent API. It lets an AI assistant read compact, pre-computed summaries of
the owner's personal health data (a daily brief, metric time series, workouts and
goals), the owner's study progress, study plan and dated daily study tasks, and the
web page conversation. No raw health rows are exposed. The three write tools only
ever create study plan *proposals*, replace the not-done daily study tasks in a date
range, and post chat *replies*; they cannot activate a plan or change any health
data, goals or courses.

## Get the key (Lifebot side — manual)

1. Deploy the Lifebot agent API and note its base URL (for example
   `https://lifebot.example.com`).
2. Generate an agent key on the Lifebot side (a long random string).
3. Store the key in your secret manager and load it into the broker's environment.
   Do not commit it.
4. For the write tools, also generate the separate Lifebot write key and load it as
   `LIFEBOT_AGENT_WRITE_KEY`. The read key never works on the write routes, and the
   write key never works on the read routes. Without the write key the read tools
   keep working and the write tools report that the write key is not configured.

To rotate: generate a new key on the Lifebot side, update the secret in your secret
manager and the broker's environment, then restart or redeploy the broker. The old
key stops working once the Lifebot side picks up the new one (allow a few minutes).
A `401` from the connector means the broker's key is wrong or stale. The write key
rotates the same way; a `401` from a write tool names `LIFEBOT_AGENT_WRITE_KEY`.

## Environment

| Var | Required | Meaning |
|---|---|---|
| `LIFEBOT_API_URL` | yes | Base URL of the Lifebot agent API, e.g. `https://lifebot.example.com`. Must be `https://`; a trailing slash is stripped |
| `LIFEBOT_AGENT_KEY` | yes | Agent key (secret). Sent only in the `x-lifebot-agent-key` header, on read routes |
| `LIFEBOT_AGENT_WRITE_KEY` | for the write tools | Write key (secret), a different key from the above. Sent only in the `x-lifebot-agent-write-key` header, on the write routes |

## Tools

| Tool | Arguments | Returns |
|---|---|---|
| `get_brief` | `date` (optional, `YYYY-MM-DD`) | Daily summary: data freshness, core metrics with 7/30-day averages and goal progress, last night's sleep, training rollup, recent personal records, notable changes |
| `get_metrics` | `names` (1-10, list or comma-separated), `days` (optional, 1-365) | Daily time series per metric, plus names with no data |
| `get_workouts` | `days` (optional, 1-365), `detail` (optional, boolean) | Recent workouts, optionally with per-exercise sets |
| `get_goals` | none | Stored goals |
| `get_study` | `days` (optional, 1-365) | Online courses with completion, focus under the active plan, recent progress, pace, projected finish, stalled flag, and caveats in `notes` |
| `get_study_plan` | none | The active study plan and any proposed plan awaiting the owner's decision |
| `get_messages` | `conversation` (optional, `[a-z0-9_-]{1,32}`), `limit` (optional, 1-50) | Recent web page conversation messages, oldest first |
| `get_study_tasks` | `from`, `to` (both optional, `YYYY-MM-DD`; default today to today plus six days; at most 35 days) | The owner's dated daily study tasks per day, with what is done, planned and done minutes, and totals |
| `save_study_plan` | `plan` (object: `goal` and `focus` required; at most 20 KB) | Saves a plan as a **proposal**; the owner accepts or discards it on his page. Returns `version`, `status: "proposed"`, `unknown_course_ids`. Write key |
| `save_study_tasks` | `from`, `to` (`YYYY-MM-DD`, `from` today or later, at most 14 days), `tasks` (up to 60, at most 8 per date; each `date`, `title`, `minutes`, optional `kind`, `url` (https), `link_label`, `course_id`, `note`) | **Replaces** the plan for the whole range: every not-done task in it is removed, done tasks are kept, then the given tasks are added. Returns `saved`, `kept_done`, `removed`, `unknown_course_ids`. Write key |
| `post_reply` | `text` (1-8000 chars), `conversation` (optional) | Posts the assistant's reply to the owner's web page. Write key |

Dates are the owner's local calendar dates, except study course snapshots, which are
UTC dates. A missing field means no data, not zero. Message text and plan contents are
never logged by the connector.

## Enable

Add `lifebot` to `broker.connectors` and to the app's `allowed_connectors` in settings.
