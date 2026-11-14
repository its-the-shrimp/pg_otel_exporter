//! OTLP/HTTP+protobuf exporter running on a dedicated OS thread, plus the
//! SDK-side bridges (`SpanExporter` / `LogProcessor`) that transform finished
//! SDK entities into protobuf pieces via `opentelemetry-proto` and hand them
//! to the export queue.
//!
//! Backends (and the metrics bgworker) push [`Event`]s onto a crossbeam
//! channel; this thread batches them and POSTs protobuf payloads to
//! `{endpoint}/v1/{traces,metrics,logs}`. Network I/O never happens on a
//! backend's main thread.

use {
    super::{Event, config}, crossbeam_channel::{Receiver, RecvTimeoutError, Sender}, opentelemetry::InstrumentationScope, opentelemetry_proto::{
        tonic::{
            logs::v1::{LogsData, ResourceLogs},
            metrics::v1::{MetricsData, ResourceMetrics},
            trace::v1::{ResourceSpans, TracesData},
        },
        transform::common::tonic::ResourceAttributesWithSchema,
    }, opentelemetry_sdk::{
        error::OTelSdkResult,
        logs::{LogProcessor, SdkLogRecord},
        trace::{SpanData, SpanExporter},
    }, pgrx::{pg_sys, prelude::pg_guard}, prost::Message, std::{
        future::Future, mem::replace, panic::catch_unwind, sync::{Mutex, OnceLock, PoisonError}, thread::{Builder, JoinHandle, sleep}, time::{Duration, Instant},
    },
};

// ---- queue ------------------------------------------------------------------

/// Upper bound on queued events; when full, `queue` drops new events.
/// Prevents unbounded memory growth when the collector is down or slow.
const QUEUE_CAP: usize = 10_240;

/// Sender, receiver, and thread handle for the exporter thread, guarded by
/// one mutex: they are created together and only ever taken together (exit
/// flush).
struct Exporter {
    sender: Option<Sender<Event>>,
    receiver: Option<Receiver<Event>>,
    handle: Option<JoinHandle<()>>,
    /// Set first inside the lock so [`ensure_started_with`] runs at most once.
    spawned: bool,
    /// Whether the `on_proc_exit` hook has been registered.
    exit_hook: bool,
}

static EXPORTER: Mutex<Exporter> = Mutex::new(Exporter {
    sender: None,
    receiver: None,
    handle: None,
    spawned: false,
    exit_hook: false,
});

/// Shared HTTP agent — keeps connections alive between export requests
/// instead of paying full connection setup for every single POST.
///
/// Deliberately separate from [`EXPORTER`]: `http_post` must never hold the
/// shared exporter lock across network I/O.
static HTTP_AGENT: OnceLock<ureq::Agent> = OnceLock::new();

/// POST `body` to `{endpoint}{path}` over the shared [`HTTP_AGENT`].
fn http_post(endpoint: &str, path: &str, body: &[u8]) -> Result<(), String> {
    let url = format!("{endpoint}{path}");
    let agent = HTTP_AGENT.get_or_init(|| {
        ureq::Agent::new_with_config(
            ureq::Agent::config_builder()
                .timeout_global(Some(Duration::from_secs(2)))
                .build(),
        )
    });
    match agent
        .post(&url)
        .header("content-type", "application/x-protobuf")
        .send(body)
    {
        Ok(_) => Ok(()),
        // 4xx/5xx (http_status_as_error is on by default)
        Err(ureq::Error::StatusCode(status)) => Err(format!("{url} -> HTTP {status}")),
        Err(e) => Err(format!("{url}: {e}")),
    }
}

/// Spawn the exporter thread in THIS process if not yet running.
///
/// Must NOT be called from the postmaster (during `shared_preload_libraries`
/// `_PG_init`) because threads do not survive `fork()` into backends — hence
/// this is called lazily from [`queue`] on first use in each backend/worker.
fn ensure_started_with(exporter: &mut Exporter) {
    if replace(&mut exporter.spawned, true) {
        return;
    }
    if !replace(&mut exporter.exit_hook, true) {
        unsafe {
            pg_sys::on_proc_exit(Some(exit_flush), pg_sys::Datum::from(0usize));
        }
    }
    let (tx, rx) = crossbeam_channel::bounded(QUEUE_CAP);
    exporter.receiver = Some(rx);
    exporter.sender = Some(tx);
    // Spawned while the lock is still held: `run` blocks on this mutex until
    // the struct above is fully initialized, then clones its receiver and
    // releases it.
    exporter.handle = Builder::new()
        .name("pg_otel_exporter".into())
        .spawn(run)
        .ok();
}

/// Queue an event; never blocks, never panics into the backend.
/// Main thread (backend) or worker main thread.
///
/// When the queue is full (collector down/slow), the event is dropped:
/// losing telemetry beats exhausting backend memory.
pub fn queue(event: Event) {
    if !super::active() {
        return;
    }
    // Clone the sender under the lock, then release it: `try_send` needs only
    // the sender half, and the lock must never be held across channel ops the
    // exporter thread might block on.
    let tx = {
        let mut guard = EXPORTER.lock().unwrap_or_else(PoisonError::into_inner);
        ensure_started_with(&mut guard);
        guard.sender.clone()
    };
    let Some(tx) = tx else {
        return;
    };
    if tx.try_send(event).is_err() {
        eprintln!("pg_otel_exporter: export queue full; event dropped");
    }
}

// ---- exit-time synchronous flush ---------------------------------------------

#[pg_guard]
unsafe extern "C-unwind" fn exit_flush(_code: i32, _arg: pg_sys::Datum) {
    // Short-lived backends may exit before the periodic flush fires; push
    // their telemetry out synchronously before the process goes away.
    let _ = catch_unwind(|| {
        if !config().is_some_and(|s| s.export_enabled && s.endpoint.is_some()) {
            return;
        }
        // One pass under the lock: dropping the stored sender disconnects the
        // channel (so `run` sees Disconnected, flushes its batch, and exits),
        // and taking the receiver lets this thread drain the leftovers here.
        let (handle, rx) = {
            let mut guard = EXPORTER.lock().unwrap_or_else(PoisonError::into_inner);
            guard.sender.take(); // dropping the stored sender disconnects the channel
            (guard.handle.take(), guard.receiver.take())
        };
        // Wait for the thread to finish that final flush.
        if let Some(handle) = handle {
            let _ = handle.join();
        }
        // The thread is gone now, so draining is uncontended.
        let mut spans: Vec<ResourceSpans> = Vec::new();
        let mut logs: Vec<ResourceLogs> = Vec::new();
        let mut metrics: Vec<ResourceMetrics> = Vec::new();
        if let Some(rx) = rx {
            while let Ok(event) = rx.try_recv() {
                match event {
                    Event::Spans(mut v) => spans.append(&mut v),
                    Event::Logs(mut v) => logs.append(&mut v),
                    Event::Metrics(mut v) => metrics.append(&mut v),
                }
            }
        }
        post_all(&mut spans, &mut logs, &mut metrics);
    });
}

// ---- HTTP thread --------------------------------------------------------------

fn run() {
    // Clone the receiver under the lock, then release it: the exporter thread
    // must never hold the shared lock across blocking recv waits, or `queue`
    // and the exit flush would stall on it.
    let rx = EXPORTER
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .receiver
        .clone();
    let Some(rx) = rx else {
        return;
    };
    let mut spans: Vec<ResourceSpans> = Vec::new();
    let mut logs: Vec<ResourceLogs> = Vec::new();
    let mut metrics: Vec<ResourceMetrics> = Vec::new();

    loop {
        let snap = match config() {
            Some(s) if s.export_enabled && s.endpoint.is_some() => s,
            _ => {
                // nothing configured (yet); drain and wait
                while rx.try_recv().is_ok() {}
                sleep(Duration::from_millis(1000));
                continue;
            }
        };

        let flush_ms = snap.bsp_schedule_delay_ms;
        let batch = snap.bsp_max_export_batch_size;
        let deadline = Instant::now() + Duration::from_millis(flush_ms);
        let mut count = 0usize;
        let mut timed_out = false;

        while !timed_out && count < batch {
            let wait = if count == 0 {
                Duration::from_millis(flush_ms)
            } else {
                deadline.saturating_duration_since(Instant::now())
            };
            match rx.recv_timeout(wait) {
                Ok(event) => {
                    let n = match event {
                        Event::Spans(mut v) => {
                            let n = v.len();
                            spans.append(&mut v);
                            n
                        }
                        Event::Logs(mut v) => {
                            let n = v.len();
                            logs.append(&mut v);
                            n
                        }
                        Event::Metrics(mut v) => {
                            let n = v.len();
                            metrics.append(&mut v);
                            n
                        }
                    };
                    count += n;
                }
                Err(RecvTimeoutError::Timeout) => timed_out = true,
                Err(RecvTimeoutError::Disconnected) => {
                    // sender dropped (process exiting): flush the local batch
                    post_all(&mut spans, &mut logs, &mut metrics);
                    return;
                }
            }
        }

        post_all(&mut spans, &mut logs, &mut metrics);
    }
}

/// POST one request per non-empty signal. Shared by the periodic thread and
/// the exit-time synchronous flush.
fn post_all(
    spans: &mut Vec<ResourceSpans>,
    logs: &mut Vec<ResourceLogs>,
    metrics: &mut Vec<ResourceMetrics>,
) {
    if spans.is_empty() && logs.is_empty() && metrics.is_empty() {
        return;
    }
    let snap = match config() {
        Some(s) if s.export_enabled && s.endpoint.is_some() => s,
        _ => {
            spans.clear();
            logs.clear();
            metrics.clear();
            return;
        }
    };
    let endpoint = snap.endpoint.unwrap_or_default();

    let mut errors = Vec::new();
    if !spans.is_empty() {
        let body = TracesData {
            resource_spans: std::mem::take(spans),
        }
        .encode_to_vec();
        if let Err(e) = http_post(&endpoint, "/v1/traces", &body) {
            errors.push(e);
        }
    }
    if !metrics.is_empty() {
        let body = MetricsData {
            resource_metrics: std::mem::take(metrics),
        }
        .encode_to_vec();
        if let Err(e) = http_post(&endpoint, "/v1/metrics", &body) {
            errors.push(e);
        }
    }
    if !logs.is_empty() {
        let body = LogsData {
            resource_logs: std::mem::take(logs),
        }
        .encode_to_vec();
        if let Err(e) = http_post(&endpoint, "/v1/logs", &body) {
            errors.push(e);
        }
    }
    let joined = errors.join("; ");
    if !joined.is_empty() {
        eprintln!("pg_otel_exporter: OTLP export error: {joined}");
    }
}

// ---- SDK bridges ----------------------------------------------------------------

/// `SpanExporter` fed to `SdkTracerProvider::with_simple_exporter`: turns
/// finished SDK spans into protobuf and queues them for export.
#[derive(Debug)]
pub struct QueueSpanExporter;

impl SpanExporter for QueueSpanExporter {
    fn export(&self, batch: Vec<SpanData>) -> impl Future<Output = OTelSdkResult> + Send {
        let result = {
            let resource = ResourceAttributesWithSchema::from(super::resource());
            let spans: Vec<ResourceSpans> = batch
                .into_iter()
                .map(|span| ResourceSpans::new(span, &resource))
                .collect();
            queue(Event::Spans(spans));
            Ok(())
        };
        std::future::ready(result)
    }

    fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
        Ok(())
    }
}

/// `LogProcessor` fed to `SdkLoggerProvider`: turns emitted SDK log records
/// into protobuf and queues them for export.
#[derive(Debug)]
pub struct QueueLogProcessor;

impl LogProcessor for QueueLogProcessor {
    fn emit(&self, record: &mut SdkLogRecord, instrumentation: &InstrumentationScope) {
        let resource = ResourceAttributesWithSchema::from(super::resource());
        let rl = ResourceLogs::from(((&*record, instrumentation), &resource));
        queue(Event::Logs(vec![rl]));
    }

    fn force_flush(&self) -> OTelSdkResult {
        Ok(())
    }

    fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
        Ok(())
    }
}
