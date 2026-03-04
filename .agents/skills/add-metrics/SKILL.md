---
name: add-metrics
description: Add or change Prometheus metrics: cache WithLabelValues, consider background aggregation on hot paths, cover add/delete and clean labels, match module naming, use Metrics in helper func names, avoid changing existing metric types and labels for backward compatibility, register with prometheus.MustRegister.
---

# Adding Metrics

## Principles (checklist)

Before adding or changing metrics, ensure:

1. **Cache WithLabelValues** — Call once at init, store result in package-level vars; never call on every request.
2. **Hot path** — On high-frequency paths, consider moving collection to a background periodic task.
3. **Related paths** — Instrument all related operations (e.g. add + delete); clean up labels on teardown (`DeleteLabelValues`).
4. **Naming** — Align with existing metrics in the same module (Namespace, Subsystem, name/label style).
5. **Encapsulation** — If you wrap metrics logic in a func, include **`Metrics`** in the name (e.g. `recordRequestDurationMetrics`).
6. **Backward compatibility** — Avoid changing the **type** of existing metrics or existing **labels**; old metrics may already be in use (dashboards, alerts, scrapers).
7. **Registration** — Register metrics with **`prometheus.MustRegister(...)`** (not `Register`); duplicate or invalid registration will panic at init and fail fast.

---

## Principle details

### 1. Cache WithLabelValues

**Do not** call `WithLabelValues(...)` on every use. **Do** call it once at initialization, store the returned `prometheus.Observer` / `Counter` / `Gauge` in package-level variables, and use only those cached variables in business code.

`WithLabelValues` does lookup/creation; calling it on hot paths adds overhead and can worsen label cardinality.

### 2. Hot path: consider background aggregation

If the metric is on a **high-frequency request path**, consider moving stats collection to a **background periodic task** instead of updating on every request. In-memory aggregation with periodic flush (or timer-driven gauge updates) keeps the request path fast.

### 3. Cover all related code paths and label lifecycle

Check that **every related path** is instrumented. If you add metrics for one operation (e.g. add group), also handle the symmetric/teardown operation (e.g. delete group): remove or reset the metric for that label set so cardinality does not grow and stale series are not left behind. Same for create/delete, register/unregister, connect/disconnect. Use `vec.DeleteLabelValues(...)` in the delete/teardown path.

### 4. Naming: align with same module or directory

Follow the naming style of existing metrics in the same file or same `Namespace`/`Subsystem`:

- **Namespace** and **Subsystem** (e.g. `pd_client`, `cmd` / `request`)
- **Name**: suffix (`_seconds`, `_total`, `_count`), snake_case
- **Label names** (e.g. `type`, `host`, `stream`) for consistent dashboards and queries

### 5. Encapsulation: use `Metrics` in function names

When wrapping metrics logic in a helper, include the **`Metrics`** keyword in the function name (e.g. `recordRequestDurationMetrics`, `initMetrics`, `registerMetrics`) so metrics-related code is easy to find and grep.

### 6. Backward compatibility: avoid changing existing metrics type and labels

Do **not** change the **type** of an existing metric (e.g. Counter → Gauge, or Histogram → Summary) or remove/rename existing **labels**. Existing metrics are often consumed by dashboards, alerts, and scrapers; changing type or labels breaks queries and can cause silent misreporting. Prefer adding **new** metrics or new labels (with a new name) instead of modifying the old ones; deprecate old ones in docs if needed.

### 7. Registration: use prometheus.MustRegister

When registering metrics with the default registry (or a custom one), use **`prometheus.MustRegister(...)`** instead of `Register(...)`. `MustRegister` panics on duplicate or invalid registration, so problems surface at startup rather than at scrape time or in production. This keeps metric setup consistent and fail-fast.
