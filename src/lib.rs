//! `pg_otel_exporter` — emit OpenTelemetry traces, metrics and logs from inside
//! PostgreSQL.
//!
//! - **Traces**: every executed statement becomes a span (executor +
//!   `ProcessUtility` hooks). Parent context can be attached via a W3C
//!   `traceparent`, provided through either of two equal channels: a
//!   sqlcommenter-style SQL comment, or
//!   `CALL traceparent(...)` inside an explicit transaction.
//! - **Metrics**: a background worker samples `pg_stat`_* views on an interval,
//!   mirroring otelcol-contrib's Postgres receiver metric set.
//!   Requires `shared_preload_libraries = 'pg_otel_exporter'`.
//! - **Logs**: `emit_log_hook` exports server log messages as `OTel` log records.
//!   Requires `shared_preload_libraries` or per-backend load via
//!   `session_preload_libraries`.
//!
//! Export uses OTLP over HTTP with protobuf to `pg_otel_exporter.endpoint`.

// this crate is glue code around postgres FFI: unsafe-in-unsafe and static
// hook pointers are pervasive and intentional here
#![allow(unsafe_op_in_unsafe_fn)]
#![allow(static_mut_refs)]
#![allow(clippy::doc_markdown)]

// No default Postgres version: every build must select one explicitly. In
// practice pgrx's build script rejects a feature-less build before we compile,
// but this keeps the contract enforced and discoverable in the crate itself.
#[cfg(not(any(
    feature = "pg14",
    feature = "pg15",
    feature = "pg16",
    feature = "pg17",
    feature = "pg18",
    feature = "pg19",
)))]
compile_error!(
    "no Postgres version selected: build with --no-default-features --features pg14..pg19"
);

use pgrx::prelude::*;

::pgrx::pg_module_magic!(name, version);

mod config;
mod logs;
mod metrics;
mod otel;
mod sql;
mod traces;

#[pg_guard]
extern "C-unwind" fn _PG_init() {
    unsafe {
        config::define_all();
        traces::install();
        logs::install();
        metrics::install();
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use {
        crate::traces::{last_resolved_ctx, query_stack_depth, resolve_fresh_ctx},
        pgrx::prelude::*,
    };

    /// Two valid traceparents with distinct trace and span ids.
    const TP_A: &str = "00-0af7651916cd43dd8448eb8c8defd531-00f067aa0ba902b7-01";
    const TP_B: &str = "00-11111111111111111111111111111111-2222222222222222-02";

    #[pg_test]
    fn test_pg_module_magic() {
        // The extension loaded and the module magic was accepted — reaching
        // this point at all is the assertion. Sanity-check that the GUCs
        // the extension defines are actually registered.
        let value: String = Spi::get_one("SHOW pg_otel_exporter.traces")
            .expect("SHOW failed")
            .expect("pg_otel_exporter.traces not set");
        assert_eq!(value, "on");
    }

    /// A statement that fails must not leave its entry on the in-flight
    /// statement stack: Postgres `longjmp`s past `executor_end_hook` when a
    /// statement errors, so the entry pushed by `executor_start_hook` would
    /// otherwise leak and corrupt the nesting depth of all later statements.
    #[pg_test]
    fn test_errored_statement_does_not_leak_stack_entry() {
        Spi::run("CREATE TEMP TABLE otel_leak_test(i int)").expect("failed to create temp table");
        Spi::run("INSERT INTO otel_leak_test VALUES (0)").expect("failed to insert test row");

        // Baseline depth (the test itself runs inside the executor, so it may
        // already be > 0).
        let before = query_stack_depth();

        // A PL/pgSQL block with an exception handler: the failing statement
        // errors out, but the handler catches it and the session continues.
        //
        // NB: the statement must be a runtime error. A constant expression
        // like `1/0` is folded during *planning*, which errors out before
        // `executor_start_hook` (so nothing is pushed and nothing leaks).
        // Reading the zero from a table forces the division to happen during
        // execution, after the executor hook has pushed.
        Spi::run(
            "DO $$ BEGIN
                 BEGIN
                     PERFORM 1/i FROM otel_leak_test;
                 EXCEPTION WHEN division_by_zero THEN
                     NULL;
                 END;
             END $$;",
        )
        .expect("DO block with handled exception should succeed");

        assert_eq!(
            query_stack_depth(),
            before,
            "QUERY_STACK leaked an entry for the errored statement"
        );
    }

    /// `CALL traceparent` inside a transaction latches the value: statements
    /// *after* the call resolve it as fresh parent context, statements before
    /// it don't (the context applies only from the call onward).
    #[pg_test]
    fn test_traceparent_call_latches_for_subsequent_statements() {
        assert!(
            resolve_fresh_ctx("SELECT 1").is_none(),
            "nothing latched yet"
        );

        Spi::run(&format!("CALL traceparent('{TP_A}')")).expect("CALL traceparent failed");

        let ctx = resolve_fresh_ctx("SELECT 1").expect("latched traceparent not resolved");
        assert_eq!(
            ctx.trace_id,
            [
                0x0a, 0xf7, 0x65, 0x19, 0x16, 0xcd, 0x43, 0xdd, 0x84, 0x48, 0xeb, 0x8c, 0x8d, 0xef,
                0xd5, 0x31
            ]
        );
        assert_eq!(
            ctx.parent_span_id,
            [0x00, 0xf0, 0x67, 0xaa, 0x0b, 0xa9, 0x02, 0xb7]
        );
    }

    /// A later `CALL` overwrites the latch (last call wins).
    #[pg_test]
    fn test_traceparent_call_overwrites_previous_latch() {
        Spi::run(&format!("CALL traceparent('{TP_A}')")).expect("CALL traceparent failed");
        Spi::run(&format!("CALL traceparent('{TP_B}')")).expect("CALL traceparent failed");

        let ctx = resolve_fresh_ctx("SELECT 1").expect("latched traceparent not resolved");
        assert_eq!(
            ctx.parent_span_id,
            [0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22]
        );
    }

    /// The latch is dropped at the commit boundary (`XACT_EVENT_PRE_COMMIT`),
    /// so it never outlives its transaction — pool handbacks are clean.
    ///
    /// Real commit boundaries are unreachable from a pgrx test (tests run
    /// inside a transaction and SPI is atomic), so the callback is invoked
    /// with the real event, as Postgres would.
    #[pg_test]
    fn test_traceparent_latch_cleared_at_commit() {
        Spi::run(&format!("CALL traceparent('{TP_A}')")).expect("CALL traceparent failed");
        assert!(resolve_fresh_ctx("SELECT 1").is_some());

        crate::traces::run_xact_callback(pg_sys::XactEvent::XACT_EVENT_PRE_COMMIT);
        assert!(
            resolve_fresh_ctx("SELECT 1").is_none(),
            "latch survived the commit boundary"
        );
    }

    /// The latch is dropped when the transaction aborts (`XACT_EVENT_ABORT`).
    #[pg_test]
    fn test_traceparent_latch_cleared_at_abort() {
        Spi::run(&format!("CALL traceparent('{TP_A}')")).expect("CALL traceparent failed");
        assert!(resolve_fresh_ctx("SELECT 1").is_some());

        crate::traces::run_xact_callback(pg_sys::XactEvent::XACT_EVENT_ABORT);
        assert!(
            resolve_fresh_ctx("SELECT 1").is_none(),
            "latch survived the abort boundary"
        );
    }

    /// A *handled* PL/pgSQL exception aborts only a subtransaction, not the
    /// transaction: the query stack is unwound (no leaked entries), but the
    /// latch must survive — the request continues and stays in its trace.
    #[pg_test]
    fn test_traceparent_latch_survives_handled_subxact_exception() {
        Spi::run("CREATE TEMP TABLE otel_tp_latch_test(i int)")
            .expect("failed to create temp table");
        Spi::run("INSERT INTO otel_tp_latch_test VALUES (0)").expect("failed to insert test row");
        let before = query_stack_depth();

        Spi::run(&format!("CALL traceparent('{TP_A}')")).expect("CALL traceparent failed");
        Spi::run(
            "DO $$ BEGIN
                 BEGIN
                     PERFORM 1/i FROM otel_tp_latch_test;
                 EXCEPTION WHEN division_by_zero THEN
                     NULL;
                 END;
             END $$;",
        )
        .expect("DO block with handled exception should succeed");

        assert_eq!(query_stack_depth(), before, "QUERY_STACK leaked an entry");
        assert!(
            resolve_fresh_ctx("SELECT 1").is_some(),
            "handled subtransaction exception must not clear the latch"
        );
    }

    /// `CALL` outside an explicit transaction warns and is ignored.
    ///
    /// The pgrx harness always runs tests inside a transaction, so this calls
    /// the entry logic with `in_tx = false` — exactly what
    /// `IsTransactionBlock()` yields for a real autocommit call.
    #[pg_test]
    fn test_traceparent_call_outside_tx_is_ignored() {
        crate::traces::ingest_traceparent(TP_A, false);
        assert!(
            resolve_fresh_ctx("SELECT 1").is_none(),
            "call outside a transaction must not latch"
        );
    }

    /// An invalid W3C traceparent is an `ERROR` at call time.
    #[pg_test(expected = "invalid W3C traceparent")]
    fn test_traceparent_invalid_format_errors() {
        Spi::run("CALL traceparent('not-a-traceparent')").expect("invalid traceparent must error");
    }

    /// When both channels provide a traceparent for the same statement, the
    /// comment wins — and the latch is kept for the remaining statements of
    /// the transaction.
    #[pg_test]
    fn test_comment_wins_over_latched_traceparent() {
        Spi::run(&format!("CALL traceparent('{TP_A}')")).expect("CALL traceparent failed");

        let stmt = format!("SELECT 1 /* traceparent='{TP_B}' */");
        let ctx = resolve_fresh_ctx(&stmt).expect("comment traceparent not resolved");
        assert_eq!(
            ctx.trace_id, [0x11u8; 16],
            "the comment must win over the latched value"
        );

        let ctx = resolve_fresh_ctx("SELECT 1").expect("latch lost after comment statement");
        assert_eq!(
            ctx.parent_span_id,
            [0x00, 0xf0, 0x67, 0xaa, 0x0b, 0xa9, 0x02, 0xb7]
        );
    }

    /// Nested statements (triggers run via SPI at depth > 0) never resolve a
    /// fresh context, even when a value is latched: they parent to the
    /// enclosing statement's span.
    #[pg_test]
    fn test_nested_statements_do_not_resolve_latch() {
        Spi::run(&format!("CALL traceparent('{TP_A}')")).expect("CALL traceparent failed");
        Spi::run("CREATE TEMP TABLE otel_tp_nested(i int)").expect("failed to create temp table");
        Spi::run(
            "CREATE FUNCTION pg_temp.otel_tp_nested_fn() RETURNS trigger LANGUAGE plpgsql
             AS $$ BEGIN PERFORM 1; RETURN NEW; END $$;",
        )
        .expect("failed to create trigger function");
        Spi::run(
            "CREATE TRIGGER otel_tp_nested_trg BEFORE INSERT ON otel_tp_nested
             FOR EACH ROW EXECUTE FUNCTION pg_temp.otel_tp_nested_fn()",
        )
        .expect("failed to create trigger");
        Spi::run("INSERT INTO otel_tp_nested VALUES (1)").expect("failed to insert");

        assert!(
            last_resolved_ctx().is_none(),
            "nested statements must not resolve the latch"
        );
    }

    /// Transaction-control utility statements are detected from the parse
    /// node, so a `BEGIN; …; COMMIT` batch (whose every hook call carries the
    /// full text) doesn't lose spans for its non-transaction statements.
    #[pg_test]
    fn test_tx_control_node_detection() {
        use crate::traces::is_tx_control_node;

        let mut tx = pg_sys::Node {
            type_: pg_sys::NodeTag::T_TransactionStmt,
        };
        let mut drop = pg_sys::Node {
            type_: pg_sys::NodeTag::T_DropStmt,
        };
        assert!(is_tx_control_node(&raw mut tx));
        assert!(!is_tx_control_node(&raw mut drop));
        assert!(!is_tx_control_node(std::ptr::null_mut()));
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec!["shared_preload_libraries = 'pg_otel_exporter'"]
    }
}
