//! Query-span capture: ExecutorStart/End hooks + `ProcessUtility` hook, with
//! W3C trace-context from two channels: a sqlcommenter-style SQL
//! comment (see [`crate::sql`]) or the `traceparent` procedure, which latches
//! a value for the rest of its transaction.
//!
//! Explicit transaction blocks (`BEGIN` … `COMMIT`) additionally get a
//! `transaction` span wrapping their statements. It is created lazily by the
//! block's first captured statement — which also consumes the fresh trace
//! context (the comment, else the latched `traceparent`) — and sits at the
//! bottom of the statement stack, so everything the transaction does nests
//! under it. Its start time is backdated to the `BEGIN` that opened the
//! block; it ends at pre-commit (or prepare), or with an error status when
//! the transaction aborts.
//!
//! Spans are created through the `opentelemetry` tracer API and finished via
//! `Span::end_with_timestamp`, at which point the SDK's processor hands them
//! to our export queue.

use {
    crate::{
        config::TRACES,
        otel::{self, tracer},
        sql::{TraceCtx, parse_traceparent, traceparent_from_statement},
    },
    opentelemetry::{
        Context, KeyValue,
        trace::{
            Span, SpanContext, SpanId, SpanKind, Status, TraceContextExt, TraceFlags, TraceId,
            TraceState, Tracer as _,
        },
    },
    opentelemetry_sdk::trace::Span as SdkSpan,
    pgrx::{pg_getarg, pg_return_void, prelude::*},
    std::{
        borrow::Cow,
        cell::RefCell,
        ffi::{CStr, c_char},
        sync::OnceLock,
        time::SystemTime,
    },
};

/// Identifies the statement an [`Active`] entry belongs to.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Origin {
    /// A statement run by the executor, identified by its `QueryDesc`.
    Executor(*mut pg_sys::QueryDesc),
    /// A utility statement (no `QueryDesc`).
    Utility,
    /// The transaction span wrapping an explicit transaction block.
    Transaction,
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

/// Per-transaction state that precedes the transaction span's creation.
#[derive(Clone, Copy)]
struct PendingTxn {
    /// When the transaction's `BEGIN` was processed (the transaction span is
    /// backdated to it).
    started_at: Option<SystemTime>,
    /// Traceparent latched by the `traceparent` procedure for this
    /// transaction; consumed when the transaction span is created.
    latch: Option<TraceCtx>,
}

thread_local! {
    /// Stack of in-flight statements in this backend. Statements run from
    /// within other statements (SPI, plpgsql) become child spans. An
    /// explicit transaction block's transaction span sits at the bottom.
    pub static QUERY_STACK: RefCell<Vec<Active>> = const { RefCell::new(Vec::new()) };

    /// Depth of `QUERY_STACK` at the start of each open subtransaction.
    ///
    /// When a subtransaction aborts, the stack is unwound back to the depth
    /// recorded for it: a statement that errored out never reaches
    /// `executor_end_hook` (Postgres `longjmp`s past it), so its entry would
    /// otherwise stay on the stack forever — and every later statement would
    /// be nested under a dead span.
    static SUBXACT_BASES: RefCell<Vec<usize>> = const { RefCell::new(Vec::new()) };

    /// Per-transaction state that exists before the transaction span does:
    /// the transaction's start time and the traceparent latched by the
    /// `traceparent` procedure. Per-backend state: a backend is a
    /// single-threaded process, so no mutex; cleared by [`xact_callback`] at
    /// every transaction end, so nothing ever outlives its transaction.
    static PENDING: RefCell<PendingTxn> = const {
        RefCell::new(PendingTxn { started_at: None, latch: None })
    };
}

#[cfg(any(test, feature = "pg_test"))]
thread_local! {
    /// Fresh trace context the most recent [`push_span`] resolved — `None`
    /// for nested statements, which never resolve. Test-only observability:
    /// the stack depth alone can't assert lineage.
    static LAST_RESOLVED: RefCell<Option<TraceCtx>> = const { RefCell::new(None) };
}

/// The innermost active entry's span context, if any (for child spans and
/// log records): the innermost statement, else the transaction span of the
/// enclosing explicit block.
pub fn active_span_context() -> Option<SpanContext> {
    QUERY_STACK.with(|s| s.borrow().last().and_then(|a| a.sc.clone()))
}

/// True when a transaction span is in flight (the stack's bottom entry).
fn txn_span_active() -> bool {
    QUERY_STACK.with(|s| {
        s.borrow()
            .first()
            .is_some_and(|a| a.origin == Origin::Transaction)
    })
}

/// Number of statement entries on the stack: the transaction span at the
/// bottom (when present) is not a statement.
fn stmt_depth() -> usize {
    QUERY_STACK.with(|s| {
        let stack = s.borrow();
        stack.len()
            - usize::from(
                stack
                    .first()
                    .is_some_and(|a| a.origin == Origin::Transaction),
            )
    })
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
                if active.origin == Origin::Transaction {
                    span.set_status(Status::error("transaction aborted"));
                }
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

/// Postgres's `IsParallelWorker()` macro: `ParallelWorkerNumber >= 0`.
/// pgrx binds the variable, not the macro.
fn is_parallel_worker() -> bool {
    unsafe { pg_sys::ParallelWorkerNumber >= 0 }
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

/// The W3C span context a parsed traceparent carries: the caller's span, as
/// a remote parent.
fn remote_ctx(ctx: &TraceCtx) -> SpanContext {
    SpanContext::new(
        TraceId::from_hex(&hex_encode(&ctx.trace_id)).unwrap_or(TraceId::INVALID),
        SpanId::from_hex(&hex_encode(&ctx.parent_span_id)).unwrap_or(SpanId::INVALID),
        TraceFlags::new(ctx.flags),
        true,
        TraceState::NONE,
    )
}

/// Attributes shared by statement and transaction spans: the DB semantic
/// conventions' identity keys.
///
/// Catalog lookups (`get_database_name`, `GetUserNameFromId`) are only safe
/// inside a live transaction. Utility statements still run after a failed
/// statement in an aborted transaction block (SET, SHOW, ROLLBACK), i.e. in
/// `TRANS_DEFAULT`/`TRANS_ABORT` state where the syscache asserts — so gate
/// them behind `IsTransactionState()`.
fn common_attributes() -> [KeyValue; 4] {
    unsafe {
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
            KeyValue::new("db.user", user),
            KeyValue::new("db.application_name", app),
        ]
    }
}

fn statement_attributes(statement: &str) -> impl Iterator<Item = KeyValue> {
    common_attributes()
        .into_iter()
        .chain([KeyValue::new("db.statement", statement.trim().to_string())])
}

fn transaction_attributes() -> impl Iterator<Item = KeyValue> {
    common_attributes().into_iter()
}

/// Create the transaction span of an explicit transaction block and push it
/// as the stack bottom. Returns the fresh trace context it resolved.
///
/// Called from [`push_span`] before the block's first captured statement
/// creates its own span (which then finds the stack holding only this
/// entry). The transaction span consumes the fresh context — the statement's
/// sqlcommenter comment, else the value latched by the `traceparent`
/// procedure — and joins the caller's trace as one subtree; every statement
/// of the block nests under it. Fresh context is resolved exactly once per
/// transaction: later comments and latches are ignored.
fn push_txn_span(statement: &str) -> Option<TraceCtx> {
    let fresh = resolve_fresh(statement);
    let active = if fresh.is_some() || roll_sample() {
        let tracer = tracer();
        let parent_cx = match &fresh {
            // join the propagated trace: caller's span as (remote) parent
            Some(ctx) => Context::current().with_remote_span_context(remote_ctx(ctx)),
            None => Context::current(),
        };
        let builder = tracer
            .span_builder("transaction")
            .with_kind(SpanKind::Internal)
            .with_attributes(transaction_attributes())
            // backdated: the transaction began before its first statement ran
            .with_start_time(
                PENDING
                    .with(|p| p.borrow().started_at)
                    .unwrap_or_else(SystemTime::now),
            );
        let span = tracer.build_with_context(builder, &parent_cx);
        let sc = span.span_context().clone();
        Active {
            span: Some(span),
            sc: Some(sc),
            origin: Origin::Transaction,
        }
    } else {
        // Sampling dropped the transaction: no span, no context — its
        // statements are not recorded either.
        Active {
            span: None,
            sc: None,
            origin: Origin::Transaction,
        }
    };
    QUERY_STACK.with(|s| s.borrow_mut().push(active));
    fresh
}

/// Begin tracking a statement: ensure the enclosing explicit transaction
/// block has its transaction span (the block's first captured statement
/// creates it), resolve the statement's parent context, start the SDK span,
/// push the stack.
///
/// `origin` identifies the statement, so that the matching end hook pops
/// *this* entry and not an unrelated one.
fn push_span(statement: &str, tag: &'static str, origin: Origin) {
    let depth = QUERY_STACK.with(|s| s.borrow().len());

    let top_level = depth == 0 && !is_parallel_worker();
    let in_block = top_level && unsafe { pg_sys::IsTransactionBlock() };

    // Fresh trace context only for top-level statements of the leader
    // process: inside an explicit transaction block the transaction span
    // resolves it (exactly once, at its creation — the remaining statements
    // inherit the transaction span); an autocommit statement resolves its
    // own. Comment first, then the value latched by the `traceparent`
    // procedure. Nested statements (triggers, SPI) and parallel workers
    // never resolve.
    let resolved = if in_block {
        push_txn_span(statement)
    } else if top_level {
        resolve_fresh(statement)
    } else {
        None
    };
    #[cfg(any(test, feature = "pg_test"))]
    LAST_RESOLVED.with(|c| *c.borrow_mut() = resolved);

    // Inside a block the propagated context belongs to the transaction span
    // (it was adopted at the span's creation); the statement itself parents
    // to the transaction span, never to the remote context.
    let fresh = if in_block { None } else { resolved };

    // A transaction span whose sampling roll dropped it left nothing to
    // inherit: none of the transaction's statements are recorded either.
    let txn_dropped = QUERY_STACK.with(|s| {
        s.borrow()
            .first()
            .is_some_and(|a| a.origin == Origin::Transaction && a.sc.is_none())
    });

    let depth = QUERY_STACK.with(|s| s.borrow().len());
    let sampled = !txn_dropped && (fresh.is_some() || depth > 0 || roll_sample());
    let active = if sampled {
        let tracer = tracer();
        let parent_cx = match fresh {
            Some(ctx) => {
                // join the propagated trace: caller's span as (remote) parent
                Context::current().with_remote_span_context(remote_ctx(&ctx))
            }
            None => match active_span_context() {
                // innermost enclosing entry's span as parent: the enclosing
                // statement, or the transaction span when this is the
                // block's first statement
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

/// End the transaction span at the stack bottom, if one is in flight.
///
/// Only reached from the pre-commit/prepare path, where every statement has
/// completed normally; an aborted transaction unwinds through
/// [`unwind_stack_to`], which ends its transaction span with an error status.
fn end_txn_span() {
    QUERY_STACK.with(|s| {
        let mut stack = s.borrow_mut();
        if !stack
            .first()
            .is_some_and(|a| a.origin == Origin::Transaction)
        {
            return;
        }
        if let Some(mut span) = stack.remove(0).span {
            span.end_with_timestamp(SystemTime::now());
        }
    });
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

/// True when the transaction-control statement is transaction control
/// (`BEGIN`, `COMMIT`, `SAVEPOINT`, ...).
///
/// Decided from the parse node, not the string: in a multi-statement message
/// every hook call receives the full text, so a leading-keyword check would
/// skip the whole batch.
unsafe fn is_tx_control_utility(pstmt: *mut pg_sys::PlannedStmt) -> bool {
    is_tx_control_node(unsafe { (*pstmt).utilityStmt })
}

/// True when the parse node is a transaction-control statement.
pub fn is_tx_control_node(utility: *mut pg_sys::Node) -> bool {
    !utility.is_null() && unsafe { (*utility).type_ } == pg_sys::NodeTag::T_TransactionStmt
}

/// True when the parse node opens an explicit transaction block (`BEGIN`,
/// `START TRANSACTION`).
pub fn is_begin_txn_node(utility: *mut pg_sys::Node) -> bool {
    if !is_tx_control_node(utility) {
        return false;
    }
    // SAFETY: parse nodes are palloc'd, hence MAXALIGN'd (8), so casting the
    // 4-byte-aligned `Node` header to the node's real (wider-aligned) type
    // never misaligns.
    #[expect(clippy::cast_ptr_alignment)]
    let kind = unsafe { (*(utility.cast::<pg_sys::TransactionStmt>())).kind };
    matches!(
        kind,
        pg_sys::TransactionStmtKind::TRANS_STMT_BEGIN
            | pg_sys::TransactionStmtKind::TRANS_STMT_START
    )
}

/// Record when the current transaction began: the transaction span (created
/// lazily by the block's first captured statement) is backdated to this.
fn record_txn_start() {
    PENDING.with(|p| p.borrow_mut().started_at = Some(SystemTime::now()));
}

/// True when the utility statement is a `CALL` of the extension's own
/// `traceparent` procedure.
///
/// Such a call is instrumentation plumbing: it must not emit a span of its
/// own, and it must not create the transaction span *before* the value it
/// latches exists — the first real statement of the transaction adopts the
/// latch into the transaction span. Identified by the procedure's OID
/// (resolved through `to_regprocedure` per `search_path`), so a same-named
/// procedure elsewhere is never mistaken for it.
pub unsafe fn is_traceparent_call(pstmt: *mut pg_sys::PlannedStmt) -> bool {
    let utility = unsafe { (*pstmt).utilityStmt };
    if utility.is_null() || unsafe { (*utility).type_ } != pg_sys::NodeTag::T_CallStmt {
        return false;
    }
    let funcexpr = unsafe {
        // SAFETY: parse nodes are palloc'd, hence MAXALIGN'd (8), so casting
        // the 4-byte-aligned `Node` header to the node's real (wider-aligned)
        // type never misaligns.
        #[expect(clippy::cast_ptr_alignment)]
        let call = utility.cast::<pg_sys::CallStmt>();
        (*call).funcexpr
    };
    if funcexpr.is_null() {
        return false;
    }
    let funcid = unsafe { (*funcexpr).funcid };
    funcid != pg_sys::InvalidOid && funcid == traceparent_proc_oid()
}

/// OID of the extension's `traceparent(text)` procedure.
///
/// Resolved through `to_regprocedure` on first use and cached for the
/// backend's lifetime. A miss (the library can be preloaded into databases
/// where the extension's SQL was never installed) is not cached: the lookup
/// retries until the procedure exists.
fn traceparent_proc_oid() -> pg_sys::Oid {
    static OID: OnceLock<pg_sys::Oid> = OnceLock::new();
    if let Some(oid) = OID.get() {
        return *oid;
    }
    match unsafe {
        pgrx::direct_function_call::<pg_sys::Oid>(
            pg_sys::to_regprocedure,
            &["traceparent(text)".into_datum()],
        )
    } {
        Some(oid) if oid != pg_sys::InvalidOid => *OID.get_or_init(|| oid),
        _ => pg_sys::InvalidOid,
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
    // A `BEGIN`/`START TRANSACTION` opens an explicit block: record when, so
    // the transaction span can be backdated to it.
    if is_begin_txn_node(unsafe { (*pstmt).utilityStmt }) {
        record_txn_start();
    }

    // The `traceparent` procedure's own call emits no span, at any depth.
    if unsafe { is_traceparent_call(pstmt) } {
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
        return;
    }

    // Only top-level utility statements; nested ones are covered via the
    // executor or are internal bookkeeping.
    let capture = stmt_depth() == 0
        && trace_enabled_here()
        && !query_string.is_null()
        && !is_tx_control_utility(pstmt);
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

/// Transaction callback: manages the transaction span and the pending
/// per-transaction state.
///
/// - abort: unwind every statement span of the transaction (its transaction
///   span, when one exists, is ended last with an error status).
/// - pre-commit / prepare: end the transaction span and drop the pending
///   state — the latched traceparent must never outlive its transaction, so
///   pool handbacks are always clean.
///
/// (The transaction's start time is recorded on the `BEGIN` statement, in
/// the `ProcessUtility` hook — Postgres fires no event at transaction start.)
unsafe extern "C-unwind" fn xact_callback(
    event: pg_sys::XactEvent::Type,
    _arg: *mut std::ffi::c_void,
) {
    use pg_sys::XactEvent;

    match event {
        XactEvent::XACT_EVENT_ABORT | XactEvent::XACT_EVENT_PARALLEL_ABORT => {
            // The whole transaction is gone; every statement in it must be
            // unwound, including the ones that failed. The transaction span
            // (bottom entry) ends last, with an error status.
            SUBXACT_BASES.with(|b| b.borrow_mut().clear());
            unwind_stack_to(0);
            clear_pending();
        }
        XactEvent::XACT_EVENT_PRE_COMMIT | XactEvent::XACT_EVENT_PREPARE => {
            // The transaction is about to end or be prepared (a prepared
            // transaction also ends the session's involvement): end its
            // span, drop the pending state.
            SUBXACT_BASES.with(|b| {
                b.borrow_mut().clear();
            });
            end_txn_span();
            clear_pending();
        }
        _ => {
            // On commit the statements all completed and popped their spans.
            // Drop any leftover bases.
            SUBXACT_BASES.with(|b| b.borrow_mut().clear());
        }
    }
}

/// Reset the pending per-transaction state.
fn clear_pending() {
    PENDING.with(|p| {
        *p.borrow_mut() = PendingTxn {
            started_at: None,
            latch: None,
        }
    });
}

// ---- traceparent procedure ingestion ---------------------------------------

// The `traceparent` procedure. Objects can't live in a schema named after
// the extension (Postgres reserves the `pg_` prefix), so the procedure is
// created unqualified and lands in the extension's installation schema;
// call it with whatever qualification puts it on `search_path`.
extension_sql!(
    r"CREATE PROCEDURE traceparent(tp text)
       LANGUAGE c AS 'MODULE_PATHNAME', 'set_traceparent';",
    name = "traceparent_proc",
);

/// Entry logic of the `traceparent` procedure.
///
/// An invalid W3C traceparent is an `ERROR` (programming error, fail-closed
/// at call time). Outside an explicit transaction the call emits a `WARNING`
/// and is ignored: instrumentation must never break the workload, and a
/// latched value must never spill into other transactions. Inside a
/// transaction the value latches until the transaction span is created by
/// the transaction's first captured statement (a later call overwrites; last
/// call wins); a call arriving after that has no effect and warns —
/// statements resolve no fresh context of their own anymore, so the value
/// would go nowhere.
pub fn ingest_traceparent(tp: &str, in_tx: bool) {
    let ctx = parse_traceparent(tp).unwrap_or_else(|| {
        ereport!(
            PgLogLevel::ERROR,
            PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
            "invalid W3C traceparent"
        );
        unreachable!("ereport(ERROR) does not return")
    });
    if !in_tx {
        ereport!(
            PgLogLevel::WARNING,
            PgSqlErrorCode::ERRCODE_WARNING,
            "call is ignored outside a transaction"
        );
        return;
    }
    if txn_span_active() {
        ereport!(
            PgLogLevel::WARNING,
            PgSqlErrorCode::ERRCODE_WARNING,
            "call is ignored: the transaction span already started; call traceparent before any other statement of the transaction"
        );
        return;
    }
    PENDING.with(|p| p.borrow_mut().latch = Some(ctx));
}

/// C entry of the `traceparent` procedure. pgrx has no procedure attribute,
/// so the SQL above declares it and this raw handler extracts the argument.
/// An invalid `traceparent` argument raises a Postgres `ERROR`.
#[pg_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn set_traceparent(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
    let tp = unsafe { pg_getarg::<String>(fcinfo, 0) };
    let in_tx = unsafe { pg_sys::IsTransactionBlock() };
    ingest_traceparent(tp.as_deref().unwrap_or(""), in_tx);
    pg_return_void()
}

/// The `PG_FUNCTION_INFO_V1` metadata Postgres looks up next to the symbol.
#[unsafe(no_mangle)]
#[doc(hidden)]
pub const extern "C" fn pg_finfo_set_traceparent() -> &'static pg_sys::Pg_finfo_record {
    const V1_API: pg_sys::Pg_finfo_record = pg_sys::Pg_finfo_record { api_version: 1 };
    &V1_API
}

/// Fresh trace context for a top-level statement: the sqlcommenter comment,
/// else the value latched by the `traceparent` procedure.
///
/// When both channels provide a traceparent the comment wins (with a
/// warning); the latch is kept for the remaining statements of its
/// transaction.
fn resolve_fresh(statement: &str) -> Option<TraceCtx> {
    let latched = PENDING.with(|p| p.borrow().latch);
    if let Some(ctx) = traceparent_from_statement(statement) {
        if latched.is_some() {
            ereport!(
                PgLogLevel::WARNING,
                PgSqlErrorCode::ERRCODE_WARNING,
                "traceparent was provided both via the procedure & via the comment; using the one from the comment"
            );
        }
        return Some(ctx);
    }
    latched
}

/// What a top-level statement would resolve as fresh trace context right
/// now: the comment on `statement`, else the latched value.
///
/// Test-only: real resolution happens inside [`push_span`], whose depth-0
/// path can't be exercised from a pgrx test — tests always run inside the
/// executor, so their statements are nested.
#[cfg(any(test, feature = "pg_test"))]
pub fn resolve_fresh_ctx(statement: &str) -> Option<TraceCtx> {
    resolve_fresh(statement)
}

/// The fresh trace context the most recent statement resolved (`None` for
/// nested statements, which never resolve). Test-only: the stack depth alone
/// can't assert lineage.
#[cfg(any(test, feature = "pg_test"))]
pub fn last_resolved_ctx() -> Option<TraceCtx> {
    LAST_RESOLVED.with(|c| *c.borrow())
}

/// Test hook: run the transaction callback as Postgres would at a commit or
/// abort boundary. Real boundaries are unreachable from a pgrx test, which
/// always executes inside a transaction (SPI runs atomically).
#[cfg(any(test, feature = "pg_test"))]
pub fn run_xact_callback(event: pg_sys::XactEvent::Type) {
    unsafe { xact_callback(event, std::ptr::null_mut()) };
}

/// Test hook: simulate a transaction span in flight, as [`push_txn_span`]
/// pushes it — here the sampling-dropped variant, which carries no span and
/// no context. Real entries are created inside the executor hooks,
/// unreachable from a pgrx test.
#[cfg(any(test, feature = "pg_test"))]
pub fn push_dropped_txn_entry() {
    QUERY_STACK.with(|s| {
        s.borrow_mut().push(Active {
            span: None,
            sc: None,
            origin: Origin::Transaction,
        });
    });
}

/// Test hook: remove the transaction span entry at the stack bottom — real
/// (pushed by `push_txn_span`) or synthetic. pg_tests run inside the
/// framework's own explicit transaction, whose span is in flight by the time
/// a test body runs; tests that exercise latch mechanics remove it first, so
/// `CALL traceparent` behaves as it would as the first statement of a block.
#[cfg(any(test, feature = "pg_test"))]
pub fn pop_txn_entry() {
    QUERY_STACK.with(|s| {
        let mut stack = s.borrow_mut();
        if stack
            .first()
            .is_some_and(|a| a.origin == Origin::Transaction)
        {
            stack.remove(0);
        }
    });
}

/// Test hook: record a transaction start, as the `BEGIN` handling of the
/// `ProcessUtility` hook would. Real hook invocations for top-level
/// statements are unreachable from a pgrx test (tests run nested inside the
/// executor).
#[cfg(any(test, feature = "pg_test"))]
pub fn record_txn_start_for_test() {
    record_txn_start();
}

/// Test hook: the pending transaction state's recorded start time.
#[cfg(any(test, feature = "pg_test"))]
pub fn pending_started_at() -> Option<SystemTime> {
    PENDING.with(|p| p.borrow().started_at)
}

/// Test hook: the OID resolved for the `traceparent(text)` procedure.
#[cfg(any(test, feature = "pg_test"))]
pub fn traceparent_proc_oid_for_test() -> pg_sys::Oid {
    traceparent_proc_oid()
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
    pg_sys::RegisterXactCallback(Some(xact_callback), std::ptr::null_mut());
    pg_sys::RegisterSubXactCallback(Some(subxact_callback), std::ptr::null_mut());
}
