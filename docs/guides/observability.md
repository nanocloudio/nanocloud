# Observability Guide

Nanocloud exposes structured logs and lightweight tracing metadata so operators
can correlate API calls, controller activity, and kubelet work without adding
external sidecars.

## Structured logging modes

`nanocloud server` defaults to the existing text/LTSV format to stay
backwards-compatible with existing tailing workflows. Set the format explicitly
with either the CLI flag or its environment override:

```sh
nanocloud server --log-format json
# or
NANOCLOUD_LOG_FORMAT=json nanocloud server
```

- **text** – human-friendly key/value pairs written to stdout/stderr. The
  baseline output now includes `trace_id` and `span_id` fields so you can grep
  for all logs within the same request or reconciliation.
- **json** – structured objects suitable for log pipelines. All baseline fields
  (`ts`, `level`, `service`, `component`, `msg`, `trace_id`, `span_id`, pid)
  are emitted consistently and caller-supplied metadata is appended as extra
  keys so downstream tools can index them individually.

Example JSON line:

```json
{
  "ts": "2024-01-05T18:04:59.014Z",
  "level": "INFO",
  "service": "nanocloud",
  "component": "bundle-controller",
  "msg": "Reconciling bundle",
  "trace_id": "e4dc62ba6c2e47d2ac1f7d88f0e8b0e1",
  "span_id": "f9308f27a4835ef0",
  "namespace": "default",
  "name": "postgres"
}
```

## Trace correlation

Every inbound API request, bundle reconciliation, VolumeSnapshot run, network
policy refresh, and kubelet replica plan now executes within a tracing span.
The active `trace_id`/`span_id` pair is propagated through asynchronous tasks
and automatically attached to every log line emitted via the Nanocloud logger,
regardless of the selected log format.

Use the IDs to follow a request end-to-end:

1. Capture the `trace_id` from an API request log (e.g. `GET /api/v1/pods`).
2. Search for the same `trace_id` to see controller and kubelet events that were
   triggered by that request.
3. Use the `span_id` to differentiate nested work (per-bundle or per-pod) when
   multiple operations share the same `trace_id`.

The spans are backed by the `tracing` crate, so future stories can export them
to OpenTelemetry without changing the correlation IDs documented here.

## Events CLI

Use `nanocloud events` to list or stream controller events without hand-crafting
field selectors:

```sh
# List the latest Warning events for a single bundle
nanocloud events --namespace prod --bundle payments --level warning --limit 20

# Follow new events, trimming the initial list to the last 15 minutes
nanocloud events --since 15m --follow --reason SecurityPolicyViolation
```

Key flags:

- `--namespace` / `--bundle` – scope by namespace and Bundle name.
- `--limit COUNT` – cap the initial page size (default unlimited).
- `--since <duration|RFC3339>` – discard events older than the supplied duration
  or timestamp before printing the initial list.
- `--level normal|warning` – filter by event type (Warnings typically indicate
  controller errors or policy blocks).
- `--reason REASON` (repeatable) – keep only events with matching reasons.
- `--follow` – keep the connection open and stream new events with automatic
  reconnection/backoff. The CLI tracks `resourceVersion` bookmarks so it resumes
  without gaps.

Streaming output mirrors the familiar `kubectl` columns (time, level, reason,
object, message) so existing watch workflows continue to work while benefiting
from the extra filters Nanocloud exposes.

## Metrics inventory

Nanocloud exports Prometheus-compatible counters/gauges via `/metrics`. The
following inventory defines the canonical names, label sets, and semantics:

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `nanocloud_controller_reconciles_total` | counter | `controller`, `result` (`success`, `error`) | Incremented once per reconciliation loop (bundles, snapshots, statefulsets, networkpolicies). |
| `nanocloud_binding_executions_total` | counter | `service`, `result` (`success`, `failed`, `timeout`) | Tracks binding envelope executions; increments before/after each run so failure modes are visible. |
| `nanocloud_image_pulls_total` | counter | `cache_hit` (`true`, `false`) | Records image fetch attempts, distinguishing between cached and remote pulls. |
| `nanocloud_restarts_total` | counter | `namespace`, `service`, `reason` | Counts kubelet restarts triggered by crashloop detection or admin actions. Keep `reason` to the documented vocabulary to avoid cardinality spikes. |
| `nanocloud_bundles_gauge` | gauge | `state` (`ready`, `degraded`) | Instantaneous bundle counts used for dashboards. |
| `nanocloud_pods_gauge` | gauge | `namespace` | Number of pods managed by Nanocloud per namespace. |

Guidelines:

- Labels are intentionally low-cardinality; avoid embedding UUIDs or resource
  versions when updating collectors.
- Update counters inline on the hot path (they are lock-free) and refresh gauges
  after each reconciliation cycle to keep samples consistent.
- Help text emitted alongside each metric must match the descriptions above so
  dashboards stay self-documenting.
