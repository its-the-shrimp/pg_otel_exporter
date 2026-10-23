//! Background worker sampling `pg_stat`_* views into `OTel` metrics,
//! mirroring otelcol-contrib's Postgres receiver metric set.
//!
//! Values are recorded through `opentelemetry` instruments (`f64_gauge` /
//! `f64_counter`) of a `SdkMeterProvider`; a `ManualReader` collects the
//! aggregated data, which is transformed to protobuf and queued for export.

use crate::config::METRIC_EXPORT_INTERVAL;

use {
    opentelemetry::{
        KeyValue,
        metrics::{Counter, Gauge, Meter, MeterProvider as _},
    },
    opentelemetry_sdk::{
        error::OTelSdkResult,
        metrics::{
            InstrumentKind, ManualReader, Pipeline, Temporality, data::ResourceMetrics,
            reader::MetricReader,
        },
    },
    pgrx::{
        bgworkers::{
            BackgroundWorker, BackgroundWorkerBuilder, BgWorkerStartTime, SignalWakeFlags,
        },
        prelude::*,
        spi::{Spi, SpiClient, SpiHeapTupleData},
    },
    std::{
        collections::HashMap,
        sync::{Arc, Weak},
        time::{Duration, SystemTime},
    },
};

use {
    crate::otel::exporter,
    opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest,
};

/// Upper bound on a single latch wait; keeps shutdown responsive.
const SLICE: Duration = Duration::from_secs(2);

// ---- reader wrapper -----------------------------------------------------------

/// `ManualReader` shared between us (for `collect`) and the meter provider
/// (which registers its pipelines with it).
#[derive(Clone)]
struct SharedManualReader(Arc<ManualReader>);

impl std::fmt::Debug for SharedManualReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SharedManualReader")
    }
}

impl MetricReader for SharedManualReader {
    fn register_pipeline(&self, pipeline: Weak<Pipeline>) {
        self.0.register_pipeline(pipeline);
    }
    fn collect(&self, rm: &mut ResourceMetrics) -> OTelSdkResult {
        self.0.collect(rm)
    }
    fn force_flush(&self) -> OTelSdkResult {
        self.0.force_flush()
    }
    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.0.shutdown_with_timeout(timeout)
    }
    fn temporality(&self, kind: InstrumentKind) -> Temporality {
        self.0.temporality(kind)
    }
}

// ---- instruments ----------------------------------------------------------------

/// Instruments, built lazily per metric name from the process' meter.
struct Instruments {
    meter: Meter,
    counters: HashMap<String, Counter<f64>>,
    gauges: HashMap<String, Gauge<f64>>,
}

impl Instruments {
    fn new(meter: Meter) -> Self {
        Self {
            meter,
            counters: HashMap::new(),
            gauges: HashMap::new(),
        }
    }

    fn count(&mut self, name: &str, unit: &str, value: f64, attrs: &[KeyValue]) {
        if let Some(counter) = self.counters.get(name) {
            counter.add(value, attrs);
            return;
        }
        let counter = self
            .meter
            .f64_counter(name.to_string())
            .with_unit(unit.to_string())
            .build();
        self.counters.insert(name.to_string(), counter);
        if let Some(counter) = self.counters.get(name) {
            counter.add(value, attrs);
        }
    }

    fn record(&mut self, name: &str, unit: &str, value: f64, attrs: &[KeyValue]) {
        if let Some(gauge) = self.gauges.get(name) {
            gauge.record(value, attrs);
            return;
        }
        let gauge = self
            .meter
            .f64_gauge(name.to_string())
            .with_unit(unit.to_string())
            .build();
        self.gauges.insert(name.to_string(), gauge);
        if let Some(gauge) = self.gauges.get(name) {
            gauge.record(value, attrs);
        }
    }
}

fn f(row: &SpiHeapTupleData<'_>, name: &str) -> f64 {
    row.get_by_name::<f64, _>(name)
        .ok()
        .flatten()
        .unwrap_or(0.0)
}

fn s(row: &SpiHeapTupleData<'_>, name: &str) -> String {
    row.get_by_name::<String, _>(name)
        .ok()
        .flatten()
        .unwrap_or_default()
}

fn db_attr(db: &str) -> KeyValue {
    KeyValue::new("database", db.to_string())
}

macro_rules! sample {
    ($client:expr, $query:expr, $row:ident => $body:block) => {
        match $client.select($query, None, &[]) {
            Ok(table) => {
                for $row in table {
                    $body
                }
            }
            Err(e) => eprintln!("pg_otel_exporter: metrics query failed: {e}"),
        }
    };
}

/// Sample all metric sources in one SPI transaction, recording values into
/// the SDK instruments (export happens via `ManualReader::collect`).
#[allow(clippy::too_many_lines)]
fn collect(client: &SpiClient<'_>, inst: &mut Instruments) {
    // ---- pg_stat_database ---------------------------------------------------
    sample!(
        client,
        "SELECT * FROM pg_stat_database WHERE datname IS NOT NULL",
        row => {
            let db = db_attr(&s(&row, "datname"));
            let attrs = vec![db.clone()];
            inst.record(
                "postgresql.backends",
                "{backends}",
                f(&row, "numbackends"),
                &attrs,
            );
            inst.count(
                "postgresql.commits",
                "{xact}",
                f(&row, "xact_commit"),
                &attrs,
            );
            inst.count(
                "postgresql.rollbacks",
                "{xact}",
                f(&row, "xact_rollback"),
                &attrs,
            );
            inst.count(
                "postgresql.blocks_read",
                "{blocks}",
                f(&row, "blks_read"),
                &attrs,
            );
            inst.count(
                "postgresql.tup_returned",
                "{rows}",
                f(&row, "tup_returned"),
                &attrs,
            );
            inst.count(
                "postgresql.tup_fetched",
                "{rows}",
                f(&row, "tup_fetched"),
                &attrs,
            );
            inst.count(
                "postgresql.tup_inserted",
                "{rows}",
                f(&row, "tup_inserted"),
                &attrs,
            );
            inst.count(
                "postgresql.tup_updated",
                "{rows}",
                f(&row, "tup_updated"),
                &attrs,
            );
            inst.count(
                "postgresql.tup_deleted",
                "{rows}",
                f(&row, "tup_deleted"),
                &attrs,
            );
            inst.count(
                "postgresql.conflicts",
                "{conflicts}",
                f(&row, "conflicts"),
                &attrs,
            );
            inst.count(
                "postgresql.temp_files",
                "{files}",
                f(&row, "temp_files"),
                &attrs,
            );
            inst.count("postgresql.temp_bytes", "By", f(&row, "temp_bytes"), &attrs);
            inst.count(
                "postgresql.deadlocks",
                "{deadlocks}",
                f(&row, "deadlocks"),
                &attrs,
            );
            #[expect(clippy::cast_precision_loss)]
            let size = row
                .get_by_name::<i64, _>("size")
                .ok()
                .flatten()
                .unwrap_or(0) as f64;
            inst.record("postgresql.database_size", "By", size, &[db]);
        }
    );

    // ---- pg_stat_activity: connections by state ------------------------------
    sample!(
        client,
        "SELECT d.datname, a.state, count(*)::float8 AS n \
         FROM pg_stat_activity a JOIN pg_database d ON d.oid = a.datid \
         GROUP BY d.datname, a.state",
        row => {
            inst.record(
                "postgresql.connections",
                "{connections}",
                f(&row, "n"),
                &[
                    db_attr(&s(&row, "datname")),
                    KeyValue::new("state", s(&row, "state")),
                ],
            );
        }
    );

    // ---- pg_locks ------------------------------------------------------------
    sample!(
        client,
        "SELECT COALESCE(d.datname,'') AS datname, l.locktype, l.mode, count(*)::float8 AS n \
         FROM pg_locks l LEFT JOIN pg_database d ON d.oid = l.database \
         GROUP BY COALESCE(d.datname,''), l.locktype, l.mode",
        row => {
            inst.record(
                "postgresql.locks",
                "{locks}",
                f(&row, "n"),
                &[
                    db_attr(&s(&row, "datname")),
                    KeyValue::new("locktype", s(&row, "locktype")),
                    KeyValue::new("mode", s(&row, "mode")),
                ],
            );
        }
    );

    // ---- pg_stat_checkpointer (PG17) ----------------------------------------
    sample!(
        client,
        "SELECT num_timed::float8, num_requested::float8, buffers_written::float8 FROM pg_stat_checkpointer",
        row => {
            let attrs: Vec<KeyValue> = Vec::new();
            inst.count(
                "postgresql.bgwriter_checkpoints_timed",
                "{checkpoints}",
                f(&row, "num_timed"),
                &attrs,
            );
            inst.count(
                "postgresql.bgwriter_checkpoints_requested",
                "{checkpoints}",
                f(&row, "num_requested"),
                &attrs,
            );
            inst.count(
                "postgresql.bgwriter_buffers_checkpoint",
                "{buffers}",
                f(&row, "buffers_written"),
                &attrs,
            );
        }
    );

    // ---- pg_stat_bgwriter -----------------------------------------------------
    sample!(
        client,
        "SELECT buffers_clean::float8, maxwritten_clean::float8 FROM pg_stat_bgwriter",
        row => {
            let attrs: Vec<KeyValue> = Vec::new();
            inst.count(
                "postgresql.bgwriter_buffers_clean",
                "{buffers}",
                f(&row, "buffers_clean"),
                &attrs,
            );
            inst.count(
                "postgresql.bgwriter_maxwritten_clean",
                "{buffers}",
                f(&row, "maxwritten_clean"),
                &attrs,
            );
        }
    );

    // ---- replication -----------------------------------------------------------
    sample!(
        client,
        "SELECT r.state, \
                pg_wal_lsn_diff(pg_current_wal_lsn(), r.replay_lsn)::float8 AS lag \
         FROM pg_stat_replication r",
        row => {
            inst.record(
                "postgresql.replication_byte_lag",
                "By",
                f(&row, "lag"),
                &[KeyValue::new("state", s(&row, "state"))],
            );
        }
    );
}

// ---- worker --------------------------------------------------------------------

#[pg_guard]
#[unsafe(no_mangle)]
#[allow(clippy::too_many_lines)]
pub extern "C-unwind" fn metrics_worker() {
    // NB: `ensure_init()` reads the database name via the syscache, which is
    // only available once the worker has connected to a database. It must
    // therefore run *after* `connect_worker_to_spi()`.
    let reader = Arc::new(
        ManualReader::builder()
            .with_temporality(Temporality::Cumulative)
            .build(),
    );

    {
        BackgroundWorker::attach_signal_handlers(
            SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM,
        );
        BackgroundWorker::connect_worker_to_spi(Some("postgres"), None);

        // `ensure_init()` resolves the database name via the syscache, which
        // requires both a database connection and an active transaction. It
        // must therefore run inside `BackgroundWorker::transaction`.
        BackgroundWorker::transaction(std::panic::AssertUnwindSafe(|| {
            crate::otel::ensure_init();
        }));

        // provider must be kept alive for the process lifetime of the worker
        let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
            .with_reader(SharedManualReader(reader.clone()))
            .with_resource(crate::otel::resource().clone())
            .build();
        let meter = provider.meter("pg_otel_exporter");
        let mut instruments = Instruments::new(meter);

        loop {
            // Wait for the sampling interval in short slices so a missed
            // latch wake cannot delay shutdown by more than a slice.
            // NB: `wait_latch` returns false when SIGTERM arrived (it
            // consumes the flag internally), true on timeout/other wakes.
            let interval_ms = METRIC_EXPORT_INTERVAL.get().clamp(1_000, 3_600_000);
            let deadline = SystemTime::now()
                + Duration::from_millis(u64::try_from(interval_ms).unwrap_or_default());
            loop {
                let remaining = deadline
                    .duration_since(SystemTime::now())
                    .unwrap_or(Duration::ZERO);
                if remaining.is_zero() {
                    break;
                }
                if !BackgroundWorker::wait_latch(Some(remaining.min(SLICE))) {
                    return;
                }
            }

            if !crate::otel::active() || !crate::config::METRICS.get() {
                continue;
            }
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                BackgroundWorker::transaction(std::panic::AssertUnwindSafe(|| {
                    Spi::connect(|client| collect(client, &mut instruments));
                }));
            }));
            if result.is_err() {
                eprintln!("pg_otel_exporter: metrics iteration failed; retrying");
                unsafe {
                    pg_sys::AbortCurrentTransaction();
                }
                std::thread::sleep(Duration::from_secs(1));
                continue;
            }

            // collect aggregated values from the SDK and queue for export
            let mut rm = ResourceMetrics::default();
            if reader.collect(&mut rm).is_ok() {
                let request: ExportMetricsServiceRequest = (&rm).into();
                exporter::queue(crate::otel::Event::Metrics(request.resource_metrics));
            }
        }
    }
}

// referenced only by symbol name via `set_function("metrics_worker")`
const _KEEP: unsafe extern "C-unwind" fn() = metrics_worker;

/// Register the metrics worker (only valid when loaded via
/// `shared_preload_libraries`, i.e. when we're in the postmaster).
pub unsafe fn install() {
    if pg_sys::IsUnderPostmaster {
        return; // loaded by CREATE EXTENSION in a backend; bgworkers not possible
    }
    BackgroundWorkerBuilder::new("pg_otel_exporter metrics worker")
        .set_type("pg_otel_exporter")
        .set_start_time(BgWorkerStartTime::ConsistentState)
        .enable_spi_access()
        .set_restart_time(Some(Duration::from_secs(1)))
        .set_library("pg_otel_exporter")
        .set_function("metrics_worker")
        .load();
}
