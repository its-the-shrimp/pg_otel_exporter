//! OpenTelemetry pipeline: SDK providers + the export queue.
//!
//! Entities (resources, spans, log records, metrics) are constructed through
//! the `opentelemetry` / `opentelemetry_sdk` APIs; only the final OTLP/HTTP
//! POST hop is ours (see [`exporter`]).
//!
//! Providers are initialized lazily, once per process, on the backend main
//! thread (GUCs may only be read there).

pub mod exporter;

use {
    opentelemetry::{KeyValue, logs::LoggerProvider as _, trace::TracerProvider as _},
    opentelemetry_proto::tonic::{
        logs::v1::ResourceLogs, metrics::v1::ResourceMetrics, trace::v1::ResourceSpans,
    },
    opentelemetry_sdk::{
        Resource,
        logs::SdkLoggerProvider,
        trace::{SdkTracerProvider, Tracer},
    },
    std::{
        sync::{Mutex, OnceLock},
        time::Duration,
    },
};

/// One queued export unit. The exporter thread merges these into a single
/// OTLP request per signal per flush.
#[derive(Debug)]
pub enum Event {
    Spans(Vec<ResourceSpans>),
    Logs(Vec<ResourceLogs>),
    Metrics(Vec<ResourceMetrics>),
}

static RESOURCE: OnceLock<Resource> = OnceLock::new();
static TRACER: OnceLock<Tracer> = OnceLock::new();
static LOGGER: OnceLock<SdkLoggerProvider> = OnceLock::new();

// ---- config snapshot --------------------------------------------------------

struct SnapshotState {
    snapshot: Option<crate::config::Snapshot>,
    at: Option<std::time::Instant>,
}

static SNAPSHOT_STATE: Mutex<SnapshotState> = Mutex::new(SnapshotState {
    snapshot: None,
    at: None,
});

/// True when exporting is on at all. Main thread only: refreshes the config
/// snapshot (reading GUCs) from the backend main thread when it goes stale.
pub fn active() -> bool {
    let mut state = SNAPSHOT_STATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if state
        .at
        .is_none_or(|t| t.elapsed() > Duration::from_secs(1))
    {
        state.snapshot = Some(crate::config::take_snapshot());
        state.at = Some(std::time::Instant::now());
    }
    state
        .snapshot
        .as_ref()
        .is_some_and(|s| s.export_enabled && s.endpoint.is_some())
}

/// Latest config snapshot; safe from any thread.
///
/// Non-main threads (the exporter thread) must not read GUCs, so they read the
/// last snapshot published here. The main thread refreshes it on every
/// statement (`trace_enabled_here` → [`active`]), so staleness is bounded by
/// statement activity — an idle backend exports nothing anyway.
pub fn snapshot() -> Option<crate::config::Snapshot> {
    SNAPSHOT_STATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .snapshot
        .clone()
}

// ---- resource ---------------------------------------------------------------

fn hostname() -> String {
    // `libc::gethostname` is portable across Unix-like systems (Linux, macOS,
    // BSDs); the previous /proc read only worked on Linux.
    //
    // SAFETY: `gethostname` writes at most `buf.len()` bytes into the buffer
    // and NUL-terminates it if there is room (POSIX); 256 bytes is more than
    // the POSIX HOST_NAME_MAX minimum (64) and typical limits (255).
    unsafe {
        let mut buf = [0u8; 256];
        if libc::gethostname(buf.as_mut_ptr().cast(), buf.len()) != 0 {
            return "unknown".to_string();
        }
        std::ffi::CStr::from_ptr(buf.as_ptr().cast())
            .to_string_lossy()
            .into_owned()
    }
}

/// The shared `OTel` `Resource` describing this PostgreSQL instance.
/// Main thread only (reads GUCs + database name).
pub fn resource() -> &'static Resource {
    ensure_init();
    RESOURCE.get().expect("pg_otel_exporter not initialized")
}

// ---- providers ----------------------------------------------------------------

/// Runs [`ensure_init`] exactly once per process.
static INIT: std::sync::Once = std::sync::Once::new();

/// Initialize the telemetry pipeline for THIS process. Main thread only.
///
/// Must NOT be called from the postmaster during `shared_preload_libraries`
/// `_PG_init` (threads/providers don't survive `fork()`); backends call it
/// lazily from the executor/log hooks, the metrics worker from its main loop.
///
/// Safe to call on every hook invocation: [`std::sync::Once`] guarantees that
/// the providers are built exactly once even if two threads race into here,
/// so no thread can build a provider that then gets thrown away.
pub fn ensure_init() {
    INIT.call_once(|| {
        let snap = crate::config::take_snapshot();
        let hostname = hostname();
        let resource = Resource::builder()
            .with_attributes([
                KeyValue::new("service.name", snap.service_name),
                KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
                KeyValue::new(
                    "service.instance.id",
                    format!("{}:{}", hostname, unsafe { pgrx::pg_sys::PostPortNumber }),
                ),
                KeyValue::new("db.system", "postgresql"),
                KeyValue::new(
                    "db.version",
                    pgrx::pg_sys::PG_VERSION.to_string_lossy().into_owned(),
                ),
                KeyValue::new("db.name", db_name()),
                KeyValue::new("host.name", hostname),
            ])
            .build();

        let tracer_provider = SdkTracerProvider::builder()
            .with_resource(resource.clone())
            .with_simple_exporter(exporter::QueueSpanExporter)
            .build();
        let tracer = tracer_provider.tracer("pg_otel_exporter");

        let logger_provider = SdkLoggerProvider::builder()
            .with_resource(resource.clone())
            .with_log_processor(exporter::QueueLogProcessor)
            .build();

        let _ = RESOURCE.set(resource);
        let _ = TRACER.set(tracer);
        // keep the providers alive for the lifetime of the process
        let _ = LOGGER.set(logger_provider);
    });
}

fn db_name() -> String {
    unsafe {
        // `get_database_name` does a syscache/catalog lookup, which requires
        // both a valid database connection and an active transaction.
        if pgrx::pg_sys::MyDatabaseId == pgrx::pg_sys::InvalidOid
            || !pgrx::pg_sys::IsTransactionState()
        {
            return "unknown".to_string();
        }
        let name = pgrx::pg_sys::get_database_name(pgrx::pg_sys::MyDatabaseId);
        if name.is_null() {
            "unknown".to_string()
        } else {
            std::ffi::CStr::from_ptr(name)
                .to_string_lossy()
                .into_owned()
        }
    }
}

/// The process' tracer (spans). Initializes the pipeline on first use.
pub fn tracer() -> &'static Tracer {
    ensure_init();
    TRACER.get().expect("pg_otel_exporter not initialized")
}

/// A logger from the process' [`SdkLoggerProvider`]. Initializes the pipeline
/// on first use.
pub fn logger() -> opentelemetry_sdk::logs::SdkLogger {
    ensure_init();
    LOGGER
        .get()
        .expect("pg_otel_exporter not initialized")
        .logger("pg_otel_exporter")
}
