//! `pg_otel_exporter` — emit OpenTelemetry traces, metrics and logs from inside
//! PostgreSQL.
//!
//! - **Traces**: every executed statement becomes a span (executor +
//!   `ProcessUtility` hooks). Parent context can be attached via a W3C
//!   `traceparent` in a sqlcommenter-style SQL comment.
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
    use pgrx::prelude::*;

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
        let before = crate::traces::query_stack_depth();

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
            crate::traces::query_stack_depth(),
            before,
            "QUERY_STACK leaked an entry for the errored statement"
        );
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
