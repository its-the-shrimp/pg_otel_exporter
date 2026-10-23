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
    super::{Event, snapshot},
    crossbeam_channel::{Receiver, RecvTimeoutError, Sender, never},
    opentelemetry::InstrumentationScope,
    opentelemetry_proto::{
        tonic::{
            logs::v1::{LogsData, ResourceLogs},
            metrics::v1::{MetricsData, ResourceMetrics},
            trace::v1::{ResourceSpans, TracesData},
        },
        transform::common::tonic::ResourceAttributesWithSchema,
    },
    opentelemetry_sdk::{
        error::OTelSdkResult,
        logs::{LogProcessor, SdkLogRecord},
        trace::{SpanData, SpanExporter},
    },
    pgrx::prelude::pg_guard,
    prost::Message,
    std::{
        future::Future,
        sync::{Mutex, OnceLock, PoisonError},
        thread::sleep,
        time::{Duration, Instant},
    },
};

// ---- queue ------------------------------------------------------------------

static THREAD: OnceLock<()> = OnceLock::new();
static THREAD_HANDLE: Mutex<Option<std::thread::JoinHandle<()>>> = Mutex::new(None);
static SENDER: Mutex<Option<Sender<Event>>> = Mutex::new(None);
static RECEIVER: OnceLock<Mutex<Receiver<Event>>> = OnceLock::new();
static EXIT_HOOK: OnceLock<()> = OnceLock::new();

/// Upper bound on queued events; when full, `queue` drops new events.
/// Prevents unbounded memory growth when the collector is down or slow.
const QUEUE_CAP: usize = 10_240;

/// Shared HTTP agent — keeps connections alive between export requests
/// instead of paying full connection setup for every single POST.
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
fn ensure_started() {
    THREAD.get_or_init(|| {
        let (tx, rx) = crossbeam_channel::bounded(QUEUE_CAP);
        if let Ok(mut guard) = SENDER.lock() {
            *guard = Some(tx);
        }
        let _ = RECEIVER.set(Mutex::new(rx));
        let spawned = std::thread::Builder::new()
            .name("pg_otel_exporter".into())
            .spawn(run);
        if let Ok(mut guard) = THREAD_HANDLE.lock() {
            *guard = spawned.ok();
        }
    });
    EXIT_HOOK.get_or_init(|| unsafe {
        pgrx::pg_sys::on_proc_exit(Some(exit_flush), pgrx::pg_sys::Datum::from(0usize));
    });
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
    ensure_started();
    let Ok(guard) = SENDER.lock() else {
        return;
    };
    let Some(tx) = guard.as_ref() else {
        return;
    };
    if tx.try_send(event).is_err() {
        eprintln!("pg_otel_exporter: export queue full; event dropped");
    }
}

// ---- exit-time synchronous flush ---------------------------------------------

/// Drain everything currently queued and POST it synchronously. Used by the
/// on-proc-exit last-chance flush.
fn flush_sync() {
    let Some(rx) = RECEIVER.get() else { return };
    let mut spans: Vec<ResourceSpans> = Vec::new();
    let mut logs: Vec<ResourceLogs> = Vec::new();
    let mut metrics: Vec<ResourceMetrics> = Vec::new();
    {
        let rx = rx.lock().unwrap_or_else(PoisonError::into_inner);
        while let Ok(event) = rx.try_recv() {
            match event {
                Event::Spans(mut v) => spans.append(&mut v),
                Event::Logs(mut v) => logs.append(&mut v),
                Event::Metrics(mut v) => metrics.append(&mut v),
            }
        }
    }
    post_all(&mut spans, &mut logs, &mut metrics);
}

#[pg_guard]
unsafe extern "C-unwind" fn exit_flush(_code: i32, _arg: pgrx::pg_sys::Datum) {
    // Short-lived backends may exit before the periodic flush fires; push
    // their telemetry out synchronously before the process goes away.
    let _ = std::panic::catch_unwind(|| {
        let snap = match snapshot() {
            Some(s) if s.export_enabled && s.endpoint.is_some() => s,
            _ => return,
        };
        let _ = snap;
        // 1. stop the exporter thread cleanly: dropping the sender makes its
        //    recv() return Disconnected, at which point it flushes whatever it
        //    had batched locally.
        if let Ok(mut guard) = SENDER.lock() {
            guard.take();
        }
        // 2. wait for it to finish that final flush.
        if let Ok(mut guard) = THREAD_HANDLE.lock()
            && let Some(handle) = guard.take()
        {
            let _ = handle.join();
        }
        // 3. drain anything still queued and flush it here.
        flush_sync();
    });
}

// ---- HTTP thread --------------------------------------------------------------

fn run() {
    // RECEIVER is populated in `ensure_started` before this thread is
    // spawned, so it is always available here.
    let rx = RECEIVER.get_or_init(|| Mutex::new(never()));
    let mut spans: Vec<ResourceSpans> = Vec::new();
    let mut logs: Vec<ResourceLogs> = Vec::new();
    let mut metrics: Vec<ResourceMetrics> = Vec::new();

    loop {
        let snap = match snapshot() {
            Some(s) if s.export_enabled && s.endpoint.is_some() => s,
            _ => {
                // nothing configured (yet); drain and wait
                let rx = rx.lock().unwrap_or_else(PoisonError::into_inner);
                while rx.try_recv().is_ok() {}
                drop(rx);
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
            let guard = rx.lock().unwrap_or_else(PoisonError::into_inner);
            match guard.recv_timeout(wait) {
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
                    drop(guard);
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
    let snap = match snapshot() {
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
