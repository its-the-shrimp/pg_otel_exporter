//! Server-log capture via `emit_log_hook` -> `OTel` log records.
//!
//! Records are constructed through the `opentelemetry` logging API
//! (`Logger::create_log_record`) and emitted to the SDK logger provider,
//! whose processor hands them to our export queue. Correlation with the
//! running query is set via the record's trace context.

use std::{cell::Cell, ffi::CStr, time::SystemTime};

use {
    crate::traces::active_span_context,
    opentelemetry::logs::{AnyValue, LogRecord as _, Logger as _, Severity},
    pgrx::prelude::*,
};

static PREV_EMIT_LOG: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

thread_local! {
    /// Reentrancy guard: logging about our own log processing would recurse
    /// into ERRORDATA_STACK_SIZE panics.
    static IN_HOOK: Cell<bool> = const { Cell::new(false) };
}

type EmitLogFn = unsafe extern "C-unwind" fn(*mut pg_sys::ErrorData);

fn prev_emit_log() -> Option<EmitLogFn> {
    let p = PREV_EMIT_LOG.load(std::sync::atomic::Ordering::SeqCst);
    if p == 0 {
        None
    } else {
        // SAFETY: stored from a valid fn pointer of exactly this type
        Some(unsafe { std::mem::transmute::<usize, EmitLogFn>(p) })
    }
}

/// Map postgres elevel -> (`OTel` severity, text).
const fn severity(elevel: i32) -> Option<(Severity, &'static str)> {
    match elevel {
        10..=14 => Some((Severity::Debug, "DEBUG")), // DEBUG5..DEBUG1
        15..=17 => Some((Severity::Info, "INFO")),   // LOG, LOG_SERVER_ONLY, INFO
        18 => Some((Severity::Info, "NOTICE")),
        19 | 20 => Some((Severity::Warn, "WARNING")), // WARNING, WARNING_CLIENT_ONLY
        21 => Some((Severity::Error, "ERROR")),
        22 => Some((Severity::Fatal, "FATAL")),
        23 => Some((Severity::Fatal4, "PANIC")),
        _ => None,
    }
}

struct HookGuard;
impl Drop for HookGuard {
    fn drop(&mut self) {
        IN_HOOK.with(|g| g.set(false));
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn emit_log_hook(edata: *mut pg_sys::ErrorData) {
    if let Some(prev) = prev_emit_log() {
        prev(edata);
    }

    if IN_HOOK.with(std::cell::Cell::get) {
        return;
    }
    // background workers are too fragile a place to capture logs (their
    // error handling paths recurse); skip them entirely
    if pg_sys::MyBackendType == pg_sys::BackendType::B_BG_WORKER {
        return;
    }
    if !crate::otel::active() || !crate::config::LOGS.get() || edata.is_null() {
        return;
    }

    let _guard = HookGuard;
    IN_HOOK.with(|g| g.set(true));

    let elevel = (*edata).elevel;
    let Some((sev, sev_text)) = severity(elevel) else {
        return;
    };

    // level filter
    if let Some(min) = crate::config::log_level()
        && elevel < min
    {
        return;
    }

    let message = if (*edata).message.is_null() {
        String::new()
    } else {
        CStr::from_ptr((*edata).message)
            .to_string_lossy()
            .into_owned()
    };
    // don't recurse on our own noise
    if message.starts_with("pg_otel_exporter:") {
        return;
    }

    let mut attrs: Vec<(String, String)> = vec![
        ("db.system".to_string(), "postgresql".to_string()),
        ("db.severity".to_string(), sev_text.to_string()),
    ];
    if !(*edata).funcname.is_null() {
        attrs.push((
            "code.function".to_string(),
            CStr::from_ptr((*edata).funcname)
                .to_string_lossy()
                .into_owned(),
        ));
    }
    if !(*edata).filename.is_null() {
        attrs.push((
            "code.filepath".to_string(),
            CStr::from_ptr((*edata).filename)
                .to_string_lossy()
                .into_owned(),
        ));
    }
    if !(*edata).detail.is_null() {
        attrs.push((
            "db.log.detail".to_string(),
            CStr::from_ptr((*edata).detail)
                .to_string_lossy()
                .into_owned(),
        ));
    }
    attrs.push((
        "db.pid".to_string(),
        unsafe { pg_sys::MyProcPid }.to_string(),
    ));

    let now = SystemTime::now();
    let logger = crate::otel::logger();
    let mut record = logger.create_log_record();
    record.set_timestamp(now);
    record.set_observed_timestamp(now);
    record.set_severity_text(sev_text);
    record.set_severity_number(sev);
    record.set_body(AnyValue::String(message.into()));
    record.add_attributes(
        attrs
            .into_iter()
            .map(|(k, v)| (opentelemetry::Key::new(k), AnyValue::String(v.into()))),
    );

    // correlate with the active query span, if any
    if let Some(sc) = active_span_context() {
        record.set_trace_context(sc.trace_id(), sc.span_id(), Some(sc.trace_flags()));
    }

    logger.emit(record);
}

pub unsafe fn install() {
    if let Some(prev) = pg_sys::emit_log_hook {
        PREV_EMIT_LOG.store(prev as usize, std::sync::atomic::Ordering::SeqCst);
    }

    pg_sys::emit_log_hook = Some(emit_log_hook);
}
