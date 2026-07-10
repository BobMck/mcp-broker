# Adzuna Connector Setup

Native connector (`auth_mode="none"`), credential = an Adzuna **app_id + app_key**
(free tier). Read-only: job search, salary histogram, salary history (comp trends),
top employers, category list.

## Create the credentials (Adzuna — manual)

1. Register at https://developer.adzuna.com/ → create an app.
2. Copy the **Application ID** (`app_id`) and **Application Key** (`app_key`).
   The app_id is not sensitive; the app_key is a secret.

## Environment

| Var | Required | Meaning |
|---|---|---|
| `ADZUNA_APP_ID` | yes | Application ID |
| `ADZUNA_APP_KEY` | yes | Application Key (secret) |
| `ADZUNA_COUNTRY` | no (default `gb`) | Default country code; per-call `country` overrides it (validated allow-list) |

## Enable

Add `adzuna` to `broker.connectors` and to the app's `allowed_connectors` in settings.
