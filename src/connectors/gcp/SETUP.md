# GCP Connector Setup

Native connector (`auth_mode="none"`). No OAuth app, no key file.

## Requirements

1. The broker must run **on a GCE VM** — the credential is the VM's attached
   service account, fetched from the metadata server (169.254.169.254).
2. That service account needs (least privilege):
   - a custom role with exactly: `compute.instances.{list,get,start,stop,reset,getSerialPortOutput}`,
     `compute.zones.list`, `compute.zoneOperations.get`
   - `roles/logging.viewer`

## Environment

| Var | Required | Meaning |
|---|---|---|
| `GCP_PROJECT` | yes | Project the tools operate on (pinned; not a tool param) |
| `GCP_ZONE` | no (default `europe-west2-a`) | Zone for instance operations |

## Enable

Add `gcp` to `broker.connectors` and to the app's `allowed_connectors` in settings.
