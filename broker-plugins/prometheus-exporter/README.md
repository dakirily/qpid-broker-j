# Prometheus Exporter Plugin (Apache Qpid Broker-J)

The `prometheus-exporter` plugin exposes Apache Qpid Broker-J statistics as **Prometheus metrics** using the Prometheus **text exposition format**.

It walks the Broker-J `ConfiguredObject` hierarchy, reads each object’s `getStatistics()` map, converts statistics into Prometheus **counters** and **gauges**, and renders them via the Prometheus Java client 1.x text writer.

---

## Features

- Exposes a `/metrics` HTTP endpoint (via the Broker-J HTTP Management layer).
- Emits Prometheus text format (version `0.0.4`).
- Converts Broker-J statistics into:
  - **Counter** metrics for `StatisticType.CUMULATIVE`
  - **Gauge** metrics for `StatisticType.POINT_IN_TIME`
- Supports filtering:
  - include/exclude disabled statistics
  - export only selected metric families

---

## Endpoints

### `GET /metrics`

Returns metrics for the full object tree starting at the broker root.

Example:

```bash
curl -u admin:admin http://localhost:8080/metrics
```

### `GET /metrics/<virtualHostNode>/<virtualHost>`

Returns metrics starting at the specified Virtual Host (useful for reducing output size).

Example:

```bash
curl -u admin:admin http://localhost:8080/metrics/myVhn/myVhost
```

> The exact routing depends on the HTTP management configuration, but `/metrics` and `/metrics/<vhn>/<vh>` are commonly used and covered by system tests.

---

## Authentication

`/metrics` is protected the same way as the HTTP Management API. Unauthenticated requests typically return `401 Unauthorized`.

---

## Metric naming

### Metric family name

Metric family names follow this pattern:

```
qpid_<category>_<metric>
```

Where:

- `<category>` is derived from the `ConfiguredObject` category (e.g. `broker`, `queue`, `exchange`, `virtual_host`, `port`, …)
- `<metric>` is taken from the model (`ConfiguredObjectStatistic.getMetricName()`), or generated from the statistic name when no explicit metric name is provided.

### Counters and `_total`

Counter metrics follow Prometheus/OpenMetrics conventions:

- the **metadata name** is the base name
- the **exposed name** includes the `_total` suffix

This may result in **breaking name changes** if you migrate from older exporters that encoded `_count`/`_total` differently.

---

## Labels

Labels distinguish samples belonging to different objects.

- For direct children, the plugin typically uses:
  - `name="<objectName>"`
- For deeper hierarchies, parent names are added using `<parent_type>_name` labels, ordered from closest parent to root.

Example (conceptual):

```
qpid_queue_depth_messages{ name="queueA", virtual_host_name="vh1" } 0
```

---

## Query parameters

### `includeDisabled`

Controls whether statistics that are disabled in the model should be exported.

- `includeDisabled=true` — export disabled statistics as well
- `includeDisabled=false` — export only enabled statistics
- if omitted — falls back to the broker context variable (see below), otherwise defaults to `false`

Example:

```bash
curl -u admin:admin "http://localhost:8080/metrics?includeDisabled=true"
```

### `name[]`

Exports only the specified **metric families**. May be provided multiple times.

- if omitted or empty — all metric families are exported

Example:

```bash
curl -u admin:admin \
  "http://localhost:8080/metrics?name[]=qpid_queue_depth_messages&name[]=qpid_queue_consumers"
```

> Note: depending on the implementation, counter filters may accept both the base name and the `_total`-suffixed name for convenience.
- Uses an **AUTO_EXTEND** naming strategy to avoid Prometheus label-set conflicts: if the same metric name would
  otherwise be emitted with different label sets under different parents, the exporter prefixes parent categories into the
  metric family name (e.g. `qpid_virtualhost_queue_*` vs `qpid_broker_queue_*`).

---

## Broker context variables

### `qpid.metrics.includeDisabled`

Default value for `includeDisabled` when the query parameter is not specified.

- `true` — export disabled statistics by default
- `false` — do not export disabled statistics by default

### `qpid.metrics.strictNaming` (optional, if enabled in your build)

Strict naming mode.

- `true` — fail the scrape if a metric name must be sanitized or if label sets become inconsistent
- `false` — log a warning and continue (default)

This is useful in CI/testing to prevent “silent renames” caused by name sanitization.

---

### `qpid.metrics.prometheus.preserveMetricNameSuffix` (optional)

Comma separated list of metric identifiers for which the exporter must **preserve** suffixes like `_count` and `_total`
(i.e. **do not strip** them).

Each entry may be either:
- a **raw model metricName** (as returned by `ConfiguredObjectStatistic.getMetricName()`), e.g. `inbound_bytes_total`
- a **fully qualified Prometheus family name**, e.g. `qpid_broker_inbound_bytes_total`

Matching is case-insensitive and treats any non-alphanumeric characters as separators (equivalent to `_`).

### `qpid.metrics.prometheus.stripMetricNameSuffix` (optional, default: `true`)

Global switch that controls whether the exporter strips suffixes like `_count` and `_total` from metric names.

- `true` (default) — apply suffix stripping (with optional per-metric overrides via `preserveMetricNameSuffix`).
- `false` — **legacy naming mode**: do not strip suffixes for any metric.

> Note: when `stripMetricNameSuffix=false`, `preserveMetricNameSuffix` has no practical effect because nothing is stripped.
## Performance notes

Metrics are collected on each scrape by traversing the object tree and reading statistics. On installations with many objects (queues/exchanges/etc.), the output and scrape time can become large.

Recommendations:

- Use `name[]` to export only required metric families
- Use per-virtual-host endpoints if available
- Scrape less frequently if needed

---

## Build & install (high level)

1. Build the module `broker-plugins/prometheus-exporter`.
2. Ensure HTTP management is enabled on the broker.
3. Deploy the plugin JAR so Broker-J can load it (deployment location depends on your distribution/build).
4. Restart the broker and verify:

```bash
curl -u admin:admin http://localhost:8080/metrics
```

---

## Migration / breaking changes

When migrating from older Prometheus Java clients (e.g., simpleclient 0.16), **metric names may change**, especially for:

- counters (alignment to `_total` naming)
- gauges that previously ended with `_total` in legacy implementations
- metric names that required sanitization to become Prometheus-valid

Best practice:

- capture a metrics dump before and after the upgrade
- build an `old_name -> new_name` mapping
- update dashboards, alerts, and recording rules accordingly
