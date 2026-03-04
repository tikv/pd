---
name: add-metrics
description: "Add or change Prometheus metrics — cache WithLabelValues, hot path, cleanup, avoid high cardinality, naming, MustRegister, backward compatibility. From tikv/pd metrics PRs."
---

# Adding Metrics

## PR summary (tikv/pd, latest ~100 metrics-related PRs)

From [GitHub search](https://github.com/tikv/pd/pulls?q=is%3Apr+metrics) (600+ PRs), recurring themes:

- **Cleanup on deletion** — Delete cluster/group metrics when store or resource is removed (e.g. [#9945](https://github.com/tikv/pd/pull/9945), [#9266](https://github.com/tikv/pd/pull/9266)).
- **Reduce cardinality** — Reduce balance/filter metrics or avoid many unique label values ([#9537](https://github.com/tikv/pd/pull/9537), [#9536](https://github.com/tikv/pd/pull/9536)).
- **Dashboard/panel alignment** — Align Grafana and metric semantics (e.g. store used vs user storage) ([#10277](https://github.com/tikv/pd/pull/10277), [#9891](https://github.com/tikv/pd/pull/9891)).
- **Labels for observability** — Add keyspace/store labels where useful ([#9778](https://github.com/tikv/pd/pull/9778), [#9898](https://github.com/tikv/pd/pull/9898)).
- **gRPC/stream metrics** — Per-service stream duration histograms, lifecycle instrumentation ([#10201](https://github.com/tikv/pd/pull/10201), [#9768](https://github.com/tikv/pd/pull/9768)).
- **Client** — Don’t record when no retry; use const labels for resource group ([#9464](https://github.com/tikv/pd/pull/9464), [#9383](https://github.com/tikv/pd/pull/9383)).
- **Fix wrong metrics** — Correct measurement or add new metric/version; don’t silently change semantics ([#9561](https://github.com/tikv/pd/pull/9561), [#3524](https://github.com/tikv/pd/pull/3524)).

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
