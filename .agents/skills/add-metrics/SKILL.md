---
name: add-metrics
description: "Add or change Prometheus metrics — cache WithLabelValues, hot path, cleanup, avoid high cardinality, naming, MustRegister, backward compatibility. From tikv/pd metrics PRs."
---

## Principles (checklist + detail)

Before adding or changing metrics, ensure:

1. **Cache WithLabelValues** — Call once at init, store result in package-level vars; never call on every request. `WithLabelValues` does lookup/creation; on hot paths it adds overhead and can worsen cardinality.

2. **Hot path** — Remind developers to judge whether the current path is a hot path; if it is, suggest moving metrics operations to a background (periodic) task so the request path stays fast.

3. **Related paths & cleanup** — Instrument add + delete (or create/teardown). When an entity (store, group, stream) is removed, call `vec.DeleteLabelValues(...)` so cardinality does not grow and stale series are removed. Same for create/delete, register/unregister, connect/disconnect.

4. **Avoid high-cardinality labels** — Prefer reducing or aggregating; avoid many unique label values on hot paths. High cardinality can overwhelm Prometheus (memory, scrape cost).

5. **Naming** — Align with existing metrics in the same module: **Namespace** and **Subsystem** (e.g. `pd_client`, `request`), name with `_seconds`/`_total`/`_count`, snake_case, label names (e.g. `type`, `host`, `stream`) for consistent dashboards and queries.

6. **Encapsulation** — If you wrap metrics logic in a func, include **`Metrics`** in the name (e.g. `recordRequestDurationMetrics`, `initMetrics`, `registerMetrics`) so it’s easy to find and grep.

7. **Backward compatibility** — Do not change the **type** of existing metrics (e.g. Counter → Gauge) or remove/rename existing **labels**; dashboards and alerts may break. Prefer adding **new** metrics or deprecating old ones in docs.

8. **Registration** — Use **`prometheus.MustRegister(...)`** (not `Register`); duplicate or invalid registration will panic at init and fail fast.

9. **Dashboard/panel alignment** — When adding or changing a metric, ensure Grafana panels and alerts use the same definition (e.g. store used vs user storage size).

10. **Don’t record when there’s nothing to record** — Skip updating metrics when the operation didn’t happen (e.g. no retry, no forward); avoid recording zeros or empty aggregates that add noise.

11. **Const labels for stable dimensions** — For dimensions fixed per process/component (e.g. resource group name in client), use const labels at init instead of dynamic label values on every call.

12. **Fix wrong metrics and document semantics** — If a metric measures the wrong thing (e.g. “processing time” including network), fix the measurement or add a new metric/version; do not silently change semantics.

13. **Instrument full lifecycle** — For gRPC streams or long-lived resources, add metrics for the full lifecycle and reflect cleanup/end so dashboards don’t show stuck or misleading series.
