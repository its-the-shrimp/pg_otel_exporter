//! Query-span capture: ExecutorStart/End hooks + `ProcessUtility` hook, with
//! W3C trace-context extracted from sqlcommenter-style SQL comments (see
//! [`crate::sql`]).
//!
//! Spans are created through the `opentelemetry` tracer API and finished via
//! `Span::end_with_timestamp`, at which point the SDK's processor hands them
//! to our export queue.

use {
    crate::{config::TRACES, otel::{self, tracer}, sql::traceparent_from_statement}, opentelemetry::{
        Context, KeyValue,
        trace::{
            Span, SpanContext, SpanId, SpanKind, TraceContextExt, TraceFlags, TraceId, TraceState,
            Tracer as _,
        },
    }, opentelemetry_sdk::trace::Span as SdkSpan, pgrx::prelude::*, std::{borrow::Cow, cell::RefCell, ffi::{CStr, c_char}, time::SystemTime},
};

/// Identifies the statement an [`Active`] entry belongs to.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Origin {
    /// A statement run by the executor, identified by its `QueryDesc`.
    Executor(*mut pg_sys::QueryDesc),
    /// A utility statement (no `QueryDesc`).
    Utility,
}

/// An in-flight statement in this backend.
pub struct Active {
    /// The SDK span; absent when sampling dropped this statement.
    span: Option<SdkSpan>,
    /// This statement's own span context (for children + log correlation).
    sc: Option<SpanContext>,
    /// Which statement this entry belongs to.
    origin: Origin,
}

thread_local! {
    /// Stack of in-flight statements in this backend. Statements run from
    /// within other statements (SPI, plpgsql) become child spans.
    pub static QUERY_STACK: RefCell<Vec<Active>> = const { RefCell::new(Vec::new()) };

    /// Depth of `QUERY_STACK` at the start of each open subtransaction.
    ///
    /// When a subtransaction aborts, the stack is unwound back to the depth
    /// recorded for it: a statement that errored out never reaches
    /// `executor_end_hook` (Postgres `longjmp`s past it), so its entry would
    /// otherwise stay on the stack forever — and every later statement would
    /// be nested under a dead span.
    static SUBXACT_BASES: RefCell<Vec<usize>> = const { RefCell::new(Vec::new()) };
}

/// The innermost active statement's span context, if any (for log records).
pub fn active_span_context() -> Option<SpanContext> {
    QUERY_STACK.with(|s| s.borrow().last().and_then(|a| a.sc.clone()))
}

/// Current depth of the in-flight statement stack.
///
/// Exposed for tests: a leaked entry (from a statement that errored out)
/// shows up here as a non-zero depth after the statement finished.
#[cfg(any(test, feature = "pg_test"))]
pub fn query_stack_depth() -> usize {
    QUERY_STACK.with(|s| s.borrow().len())
}

/// Pop entries off `QUERY_STACK` down to `base`, ending the orphaned spans.
///
/// Ending them right here is safe: this runs during normal transaction-abort
/// processing, on the backend's main thread, with thread-locals alive —
/// `block_on` in the exporter and the raw GUC reads it does are fine. Parking
/// the spans for later disposal instead would be fatal: a backend that exits
/// right after the error would drop them during thread-local teardown, where
/// the span `Drop` → exporter → `block_on` path panics ("cannot access TLS
/// during destruction") and aborts the process.
fn unwind_stack_to(base: usize) {
    QUERY_STACK.with(|s| {
        let mut stack = s.borrow_mut();
        while stack.len() > base {
            let Some(mut active) = stack.pop() else {
                break;
            };
            if let Some(mut span) = active.span.take() {
                span.end_with_timestamp(SystemTime::now());
            }
        }
    });
}

// ---- previous hook storage ------------------------------------------------

type ExecutorStartFn = unsafe extern "C-unwind" fn(*mut pg_sys::QueryDesc, i32);
// Postgres 18 removed ExecutorRun's `execute_once` argument.
#[cfg(not(any(feature = "pg18", feature = "pg19")))]
type ExecutorRunFn =
    unsafe extern "C-unwind" fn(*mut pg_sys::QueryDesc, pg_sys::ScanDirection::Type, u64, bool);
#[cfg(any(feature = "pg18", feature = "pg19"))]
type ExecutorRunFn =
    unsafe extern "C-unwind" fn(*mut pg_sys::QueryDesc, pg_sys::ScanDirection::Type, u64);
type ExecutorEndFn = unsafe extern "C-unwind" fn(*mut pg_sys::QueryDesc);
type ProcessUtilityFn = unsafe extern "C-unwind" fn(
    *mut pg_sys::PlannedStmt,
    *const c_char,
    bool,
    pg_sys::ProcessUtilityContext::Type,
    pg_sys::ParamListInfo,
    *mut pg_sys::QueryEnvironment,
    *mut pg_sys::DestReceiver,
    *mut pg_sys::QueryCompletion,
);

struct PrevHooks {
    executor_start: Option<ExecutorStartFn>,
    executor_run: Option<ExecutorRunFn>,
    executor_end: Option<ExecutorEndFn>,
    process_utility: Option<ProcessUtilityFn>,
}
// SAFETY: written once in _PG_init before hooks fire; read-only afterwards
unsafe impl Sync for PrevHooks {}
static mut PREV: PrevHooks = PrevHooks {
    executor_start: None,
    executor_run: None,
    executor_end: None,
    process_utility: None,
};

/// Sampling roll for root spans without propagated context.
#[expect(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
fn roll_sample() -> bool {
    let ratio = crate::config::TRACES_SAMPLER_ARG.get();
    if ratio >= 1.0 {
        return true;
    }
    if ratio <= 0.0 {
        return false;
    }
    let mut buf = [0u8; 8];
    let ok = unsafe { pg_sys::pg_strong_random(buf.as_mut_ptr().cast(), buf.len()) };
    let v = if ok {
        u64::from_be_bytes(buf)
    } else {
        // extremely defensive fallback
        SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos() as u64)
            ^ (u64::from(std::process::id()) << 32)
            | 1
    };
    (v as f64 / u64::MAX as f64) < ratio
}

fn trace_enabled_here() -> bool {
    if !otel::active() || !TRACES.get() {
        return false;
    }
    // the metrics bgworker must not emit query spans
    unsafe {
        if pg_sys::MyBackendType == pg_sys::BackendType::B_BG_WORKER {
            return false;
        }
    }
    true
}

// ---- helpers ---------------------------------------------------------------

fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

unsafe fn cstr_or(p: *const c_char, default: &str) -> Cow<'_, str> {
    if p.is_null() {
        Cow::Borrowed(default)
    } else {
        CStr::from_ptr(p).to_string_lossy()
    }
}

fn statement_attributes(statement: &str) -> impl Iterator<Item = KeyValue> {
    unsafe {
        // Catalog lookups (`get_database_name`, `GetUserNameFromId`) are only
        // safe inside a live transaction. Utility statements still run
        // after a failed statement in an aborted transaction block (SET,
        // SHOW, ROLLBACK), i.e. in `TRANS_DEFAULT`/`TRANS_ABORT` state where
        // the syscache asserts — so gate them behind `IsTransactionState()`.
        let in_txn = pg_sys::IsTransactionState();
        let db = if pg_sys::MyDatabaseId == pg_sys::InvalidOid || !in_txn {
            "unknown".to_string()
        } else {
            cstr_or(pg_sys::get_database_name(pg_sys::MyDatabaseId), "unknown").into_owned()
        };
        let user = if in_txn {
            cstr_or(
                pg_sys::GetUserNameFromId(pg_sys::GetUserId(), true),
                "unknown",
            )
            .into_owned()
        } else {
            "unknown".to_string()
        };
        let app = cstr_or(pg_sys::application_name, "").into_owned();
        [
            KeyValue::new("db.system", "postgresql"),
            KeyValue::new("db.name", db),
            KeyValue::new("db.statement", statement.trim().to_string()),
            KeyValue::new("db.user", user),
            KeyValue::new("db.application_name", app),
        ]
        .into_iter()
    }
}

/// Begin tracking a statement: resolve the parent context (from a comment or
/// the innermost enclosing statement), start the SDK span, push the stack.
///
/// `origin` identifies the statement, so that the matching end hook pops
/// *this* entry and not an unrelated one.
fn push_span(statement: &str, tag: &'static str, origin: Origin) {
    let depth = QUERY_STACK.with(|s| s.borrow().len());

    // Resolve trace context: explicit (comment) for top-level, otherwise
    // parent = enclosing statement's span.
    let explicit = if depth == 0 {
        traceparent_from_statement(statement)
    } else {
        None
    };

    let sampled = explicit.is_some() || depth > 0 || roll_sample();
    let active = if sampled {
        let tracer = tracer();
        let parent_cx = match explicit {
            Some(ctx) => {
                // join the propagated trace: caller's span as (remote) parent
                let sc = SpanContext::new(
                    TraceId::from_hex(&hex_encode(&ctx.trace_id)).unwrap_or(TraceId::INVALID),
                    SpanId::from_hex(&hex_encode(&ctx.parent_span_id)).unwrap_or(SpanId::INVALID),
                    TraceFlags::new(ctx.flags),
                    true,
                    TraceState::NONE,
                );
                Context::current().with_remote_span_context(sc)
            }
            None => match active_span_context() {
                // enclosing statement's span as parent
                Some(parent) => Context::current().with_remote_span_context(parent),
                None => Context::current(),
            },
        };
        let builder = tracer
            .span_builder(tag)
            .with_kind(SpanKind::Internal)
            .with_attributes(statement_attributes(statement))
            .with_start_time(SystemTime::now());
        let span = tracer.build_with_context(builder, &parent_cx);
        let sc = span.span_context().clone();
        Active {
            span: Some(span),
            sc: Some(sc),
            origin,
        }
    } else {
        Active {
            span: None,
            sc: None,
            origin,
        }
    };
    QUERY_STACK.with(|s| s.borrow_mut().push(active));
}

/// Finish the innermost statement span.
fn pop_span(rows: Option<u64>) {
    let Some(active) = QUERY_STACK.with(|s| s.borrow_mut().pop()) else {
        return;
    };
    let Some(mut span) = active.span else {
        return;
    };
    if let Some(rows) = rows {
        span.set_attribute(KeyValue::new(
            "db.rows_affected",
            i64::try_from(rows).unwrap_or(i64::MAX),
        ));
    }
    span.end_with_timestamp(SystemTime::now());
}

/// Finish the span belonging to `origin`.
///
/// An end hook may be called for a statement whose start never pushed
/// anything (tracing disabled for this backend), and a statement that errored
/// out may have been unwound already. Only pop when the top of the stack
/// really belongs to this statement, so an unrelated entry is never removed.
fn pop_span_for(origin: Origin, rows: Option<u64>) {
    let matches = QUERY_STACK.with(|s| s.borrow().last().is_some_and(|a| a.origin == origin));
    if matches {
        pop_span(rows);
    }
}

// ---- Executor hooks --------------------------------------------------------

#[pg_guard]
unsafe extern "C-unwind" fn executor_start_hook(query_desc: *mut pg_sys::QueryDesc, eflags: i32) {
    if let Some(prev) = PREV.executor_start {
        prev(query_desc, eflags);
    } else {
        pg_sys::standard_ExecutorStart(query_desc, eflags);
    }

    if !trace_enabled_here() {
        return;
    }

    let source = if !query_desc.is_null() && !(*query_desc).sourceText.is_null() {
        CStr::from_ptr((*query_desc).sourceText)
            .to_string_lossy()
            .into_owned()
    } else {
        cstr_or(pg_sys::debug_query_string, "").into_owned()
    };
    push_span(&source, "query", Origin::Executor(query_desc));
}

/// `ExecutorRun` hook, Postgres 13–17 signature (with `execute_once`).
#[cfg(not(any(feature = "pg18", feature = "pg19")))]
#[pg_guard]
unsafe extern "C-unwind" fn executor_run_hook(
    query_desc: *mut pg_sys::QueryDesc,
    direction: pg_sys::ScanDirection::Type,
    count: u64,
    execute_once: bool,
) {
    if let Some(prev) = PREV.executor_run {
        prev(query_desc, direction, count, execute_once);
    } else {
        pg_sys::standard_ExecutorRun(query_desc, direction, count, execute_once);
    }
}

/// `ExecutorRun` hook, Postgres 18+ signature (`execute_once` was removed).
#[cfg(any(feature = "pg18", feature = "pg19"))]
#[pg_guard]
unsafe extern "C-unwind" fn executor_run_hook(
    query_desc: *mut pg_sys::QueryDesc,
    direction: pg_sys::ScanDirection::Type,
    count: u64,
) {
    if let Some(prev) = PREV.executor_run {
        prev(query_desc, direction, count);
    } else {
        pg_sys::standard_ExecutorRun(query_desc, direction, count);
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn executor_end_hook(query_desc: *mut pg_sys::QueryDesc) {
    let rows = if !query_desc.is_null() && !(*query_desc).estate.is_null() {
        Some((*(*query_desc).estate).es_processed)
    } else {
        None
    };

    if let Some(prev) = PREV.executor_end {
        prev(query_desc);
    } else {
        pg_sys::standard_ExecutorEnd(query_desc);
    }

    pop_span_for(Origin::Executor(query_desc), rows);
}

// ---- ProcessUtility hook ---------------------------------------------------

/// Invoke the previous `ProcessUtility` hook, or the standard one.
unsafe fn call_prev_process_utility(
    pstmt: *mut pg_sys::PlannedStmt,
    query_string: *const c_char,
    read_only_tree: bool,
    context: pg_sys::ProcessUtilityContext::Type,
    params: pg_sys::ParamListInfo,
    query_env: *mut pg_sys::QueryEnvironment,
    dest: *mut pg_sys::DestReceiver,
    qc: *mut pg_sys::QueryCompletion,
) {
    if let Some(prev) = PREV.process_utility {
        prev(
            pstmt,
            query_string,
            read_only_tree,
            context,
            params,
            query_env,
            dest,
            qc,
        );
    } else {
        pg_sys::standard_ProcessUtility(
            pstmt,
            query_string,
            read_only_tree,
            context,
            params,
            query_env,
            dest,
            qc,
        );
    }
}

#[pg_guard]
#[allow(clippy::too_many_arguments)]
unsafe extern "C-unwind" fn process_utility_hook(
    pstmt: *mut pg_sys::PlannedStmt,
    query_string: *const c_char,
    read_only_tree: bool,
    context: pg_sys::ProcessUtilityContext::Type,
    params: pg_sys::ParamListInfo,
    query_env: *mut pg_sys::QueryEnvironment,
    dest: *mut pg_sys::DestReceiver,
    qc: *mut pg_sys::QueryCompletion,
) {
    // Only top-level utility statements; nested ones are covered via the
    // executor or are internal bookkeeping.
    let depth = QUERY_STACK.with(|s| s.borrow().len());
    let capture = depth == 0 && trace_enabled_here() && !query_string.is_null();
    if capture {
        let statement = CStr::from_ptr(query_string).to_string_lossy().into_owned();
        push_span(&statement, "utility", Origin::Utility);
        call_prev_process_utility(
            pstmt,
            query_string,
            read_only_tree,
            context,
            params,
            query_env,
            dest,
            qc,
        );
        let rows = if qc.is_null() {
            None
        } else {
            Some(unsafe { (*qc).nprocessed })
        };
        pop_span_for(Origin::Utility, rows);
        return;
    }

    call_prev_process_utility(
        pstmt,
        query_string,
        read_only_tree,
        context,
        params,
        query_env,
        dest,
        qc,
    );
}

// ---- transaction abort handling --------------------------------------------

/// Subtransaction callback: unwinds the query stack when a subtransaction
/// aborts.
///
/// A statement that fails inside a subtransaction (e.g. a PL/pgSQL block with
/// an `EXCEPTION` handler) never reaches `executor_end_hook`, because
/// Postgres `longjmp`s straight out of the executor. Its stack entry would
/// therefore survive the rollback of the subtransaction and corrupt the
/// nesting of every statement that follows.
unsafe extern "C-unwind" fn subxact_callback(
    event: pg_sys::SubXactEvent::Type,
    _my_subid: pg_sys::SubTransactionId,
    _parent_subid: pg_sys::SubTransactionId,
    _arg: *mut std::ffi::c_void,
) {
    use pg_sys::SubXactEvent;

    match event {
        SubXactEvent::SUBXACT_EVENT_START_SUB => {
            let depth = QUERY_STACK.with(|s| s.borrow().len());
            SUBXACT_BASES.with(|b| b.borrow_mut().push(depth));
        }
        SubXactEvent::SUBXACT_EVENT_ABORT_SUB => {
            // Unwind to the depth the subtransaction started at. Any entry
            // above it belongs to a statement that failed inside this
            // subtransaction.
            let base = SUBXACT_BASES.with(|b| b.borrow_mut().pop()).unwrap_or(0);
            unwind_stack_to(base);
        }
        SubXactEvent::SUBXACT_EVENT_COMMIT_SUB | SubXactEvent::SUBXACT_EVENT_PRE_COMMIT_SUB => {
            // The subtransaction completed normally: its statements all
            // ended their spans, so nothing to unwind. Just drop the
            // recorded base.
            SUBXACT_BASES.with(|b| {
                b.borrow_mut().pop();
            });
        }
        _ => {}
    }
}

/// Transaction callback: unwinds the query stack when the transaction aborts.
unsafe extern "C-unwind" fn xact_callback(
    event: pg_sys::XactEvent::Type,
    _arg: *mut std::ffi::c_void,
) {
    use pg_sys::XactEvent;

    match event {
        XactEvent::XACT_EVENT_ABORT | XactEvent::XACT_EVENT_PARALLEL_ABORT => {
            // The whole transaction is gone; every statement in it must be
            // unwound, including the ones that failed.
            SUBXACT_BASES.with(|b| b.borrow_mut().clear());
            unwind_stack_to(0);
        }
        _ => {
            // On commit the statements all completed and popped their spans.
            // Drop any leftover bases.
            SUBXACT_BASES.with(|b| b.borrow_mut().clear());
        }
    }
}

// ---- init ------------------------------------------------------------------

pub unsafe fn install() {
    PREV.executor_start = pg_sys::ExecutorStart_hook;
    pg_sys::ExecutorStart_hook = Some(executor_start_hook);
    PREV.executor_run = pg_sys::ExecutorRun_hook;
    pg_sys::ExecutorRun_hook = Some(executor_run_hook);
    PREV.executor_end = pg_sys::ExecutorEnd_hook;
    pg_sys::ExecutorEnd_hook = Some(executor_end_hook);
    PREV.process_utility = pg_sys::ProcessUtility_hook;
    pg_sys::ProcessUtility_hook = Some(process_utility_hook);

    // Unwind the query stack when a (sub)transaction aborts.
    if std::env::var("PG_OTEL_NO_XACT_CB").is_err() {
        pg_sys::RegisterXactCallback(Some(xact_callback), std::ptr::null_mut());
        pg_sys::RegisterSubXactCallback(Some(subxact_callback), std::ptr::null_mut());
    }
}
