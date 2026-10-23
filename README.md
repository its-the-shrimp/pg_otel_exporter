# pg_otel_exporter

A PostgreSQL extension (built with [pgrx]) that emits OpenTelemetry **logs, traces and
metrics**, configurable directly in `postgresql.conf`.

- **Traces** — every executed statement becomes a span (executor hooks +
  `ProcessUtility` hook). Statements run from inside other statements
  (functions, SPI, CTEs with subqueries) become child spans of the outer
  statement.
- **Trace-context propagation** — apps can attach a query to a distributed
  trace by embedding a W3C `traceparent` in a sqlcommenter-style SQL comment:

  ```sql
  SELECT * FROM users /* traceparent='00-<trace-id>-<parent-span-id>-01' */;
  ```

  The span for that query then shares the caller's trace id and parents to the
  caller's span id, so it shows up nested under the application's request in
  Jaeger, OpenObserve, etc.
- **Metrics** — a background worker samples `pg_stat_*` views on an interval
  and exports the metric set of otelcol-contrib's Postgres receiver
  (`postgresql.commits`, `postgresql.connections`, `postgresql.database_size`,
  bgwriter/checkpointer counters, replication lag, ...).
- **Logs** — `emit_log_hook` exports server log messages as OTel log records,
  correlated (trace id / span id) with the query that caused them.

[pgrx]: https://github.com/pgcentralfoundation/pgrx

## Requirements

- PostgreSQL 13–19
- `shared_preload_libraries = 'pg_otel_exporter'` — required for the metrics
  background worker; also enables log capture in auxiliary processes
- An OTLP/HTTP endpoint (e.g. an OpenTelemetry Collector on
  `http://localhost:4318`)

## Installation

### Pre-built

Each [release] ships a tarball per Postgres major version (14–19), OS
(Linux/MacOS) and architecture (amd64/arm64), containing the shared library
plus the `.control` and SQL files.
This installation requires `pg_config` binary for the target Postgres on your `PATH`

**Linux (amd64/arm64)**

```sh
curl -LO https://github.com/its-the-shrimp/pg_otel_exporter/releases/latest/download/pg_otel_exporter-<version>-pg<NN>-linux-<arch>.tar.gz
tar -xzf pg_otel_exporter-<version>-pg<NN>-linux-<arch>.tar.gz
sudo cp pg_otel_exporter.so "$(pg_config --pkglibdir)/"
sudo cp pg_otel_exporter.control pg_otel_exporter--<version>.sql "$(pg_config --sharedir)/extension/"
```

**macOS (arm64)**
```sh
curl -LO https://github.com/its-the-shrimp/pg_otel_exporter/releases/latest/download/pg_otel_exporter-<version>-pg<NN>-macos-arm64.tar.gz
tar -xzf pg_otel_exporter-<version>-pg<NN>-macos-arm64.tar.gz
cp pg_otel_exporter.dylib "$(pg_config --pkglibdir)/"
cp pg_otel_exporter.control pg_otel_exporter--<version>.sql "$(pg_config --sharedir)/extension/"
```

[release]: https://github.com/its-the-shrimp/pg_otel_exporter/releases

### From source

Build with [pgrx] and install into a pgrx-managed Postgres:

```sh
cargo pgrx init            # once, per Postgres version
cargo pgrx install --release
```

## Setup

In `postgresql.conf`:

```ini
shared_preload_libraries = 'pg_otel_exporter'
pg_otel_exporter.endpoint = 'http://localhost:4318'   # OTLP/HTTP base URL
```

Then in each database: `CREATE EXTENSION pg_otel_exporter;`

## Configuration

All configuration lives in `postgresql.conf` (GUCs are `USERSET`, so they can
also be changed per-database, per-role or per-session and reload with
`pg_reload_conf()`).

### Standard OpenTelemetry values

These are equivalent to the standard OpenTelemetry environment variables, minus the
`OTEL_` / `OTEL_EXPORTER_OTLP_` prefixes.

| GUC | Default | Meaning |
| --- | --- | --- |
| `pg_otel_exporter.sdk_disabled` | `false` | When true, no signals are exported. |
| `pg_otel_exporter.endpoint` | `http://127.0.0.1:4318` | OTLP/HTTP base URL; signals are POSTed to `{endpoint}/v1/traces\|metrics\|logs`. |
| `pg_otel_exporter.service_name` | `postgresql` | `service.name` resource attribute. |
| `pg_otel_exporter.traces_sampler_arg` | `1.0` | Sampling probability for root spans (queries without a propagated traceparent). |
| `pg_otel_exporter.bsp_schedule_delay` | `1000` | How often buffered telemetry is POSTed. |
| `pg_otel_exporter.bsp_max_export_batch_size` | `512` | Max events per export request. |
| `pg_otel_exporter.log_level` | `warning` | Minimum level to export; accepts Postgres levels: `debug5`…`debug1`, `info`, `notice`, `warning`, `error`, `log`, `fatal`, `panic`. |
| `pg_otel_exporter.metric_export_interval` | `60000` | How often the metrics background worker samples `pg_stat_*` views (1000–3600000). |

### pg_otel_exporter-specific values

| GUC | Default | Meaning |
| --- | --- | --- |
| `pg_otel_exporter.traces` | `on` | Report each executed statement as a span. |
| `pg_otel_exporter.metrics` | `on` | Enable the metrics background worker. |
| `pg_otel_exporter.logs` | `on` | Export server log messages. |

## Exported signals

### Traces

| Attribute | Value |
| --- | --- |
| span name | `query` (executor statements) or `utility` (`ProcessUtility` statements) |
| `db.system` | `postgresql` |
| `db.name` | database executing the statement |
| `db.user` | user executing the statement |
| `db.statement` | full statement text |
| `db.application_name` | `application_name` of the session |
| `db.rows_affected` | rows processed |

Utility statements (DDL, `COPY`, `DO`, ...) are captured via the
`ProcessUtility` hook. Errors mark the span's status.

### Metrics

From `pg_stat_database` (per database): `postgresql.backends`, `commits`,
`rollbacks`, `blocks_read`, `tup_returned/fetched/inserted/updated/deleted`,
`conflicts`, `temp_files`, `temp_bytes`, `deadlocks`, `database_size`;
from `pg_stat_activity`: `postgresql.connections` (by state);
from `pg_locks`: `postgresql.locks` (by locktype/mode);
from `pg_stat_checkpointer` / `pg_stat_bgwriter`: checkpoint and buffer
counters; from `pg_stat_replication`: `postgresql.replication_byte_lag`.

### Logs

Log records carry severity (mapped to OTel severity numbers), message,
`code.function`, `code.filepath`, and the trace/span ids of the query being
executed when the message was raised, plus `db.*` attributes.

## Development

```sh
cargo test                       # unit tests (traceparent parsing)
cargo pgrx test pg17             # integration tests against a managed PG
```

A stub OTLP receiver (prints whatever arrives on :4319) lives in `tools/otel_receiver.py`
