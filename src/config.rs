//! GUC-based configuration — everything lives in postgresql.conf.

use {
    pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting},
    std::ffi::CString,
};

pub static SDK_DISABLED: GucSetting<bool> = GucSetting::<bool>::new(false);
pub static ENDPOINT: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"http://127.0.0.1:4318"));
pub static SERVICE_NAME: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"postgresql"));
pub static TRACES: GucSetting<bool> = GucSetting::<bool>::new(true);
pub static TRACES_SAMPLER_ARG: GucSetting<f64> = GucSetting::<f64>::new(1.0);
pub static METRICS: GucSetting<bool> = GucSetting::<bool>::new(false);
pub static METRIC_EXPORT_INTERVAL: GucSetting<i32> = GucSetting::<i32>::new(60_000);
pub static LOGS: GucSetting<bool> = GucSetting::<bool>::new(true);
pub static LOG_LEVEL: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"warning"));
pub static BSP_SCHEDULE_DELAY: GucSetting<i32> = GucSetting::<i32>::new(1000);
pub static BSP_MAX_EXPORT_BATCH_SIZE: GucSetting<i32> = GucSetting::<i32>::new(512);

// values from utils/elog.h
pub const LEVELS: &[(&str, i32)] = &[
    ("debug", 10),
    ("debug5", 10),
    ("debug4", 11),
    ("debug3", 12),
    ("debug2", 13),
    ("debug1", 14),
    ("log", 15),
    ("info", 17),
    ("notice", 18),
    ("warning", 19),
    ("error", 21),
    ("fatal", 22),
    ("panic", 23),
];

pub fn level_number(name: &str) -> Option<i32> {
    let lower = name.trim().to_ascii_lowercase();
    LEVELS.iter().find(|(k, _)| *k == lower).map(|(_, v)| *v)
}

pub fn log_level() -> Option<i32> {
    LOG_LEVEL
        .get()
        .as_ref()
        .and_then(|cs| cs.to_str().ok())
        .and_then(level_number)
}

/// Thread-safe snapshot of the GUC-derived configuration.
///
/// Postgres GUCs may only be touched from the backend's main thread; the
/// exporter thread works exclusively off copies of this.
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub export_enabled: bool,
    pub endpoint: Option<String>,
    pub service_name: String,
    pub bsp_schedule_delay_ms: u64,
    pub bsp_max_export_batch_size: usize,
}

/// Must only be called from the backend main thread.
pub fn take_snapshot() -> Snapshot {
    Snapshot {
        export_enabled: !SDK_DISABLED.get(),
        endpoint: endpoint(),
        service_name: service_name(),
        bsp_schedule_delay_ms: BSP_SCHEDULE_DELAY
            .get()
            .clamp(100, 60_000)
            .cast_unsigned()
            .into(),
        bsp_max_export_batch_size: BSP_MAX_EXPORT_BATCH_SIZE
            .get()
            .clamp(1, 100_000)
            .cast_unsigned() as usize,
    }
}

pub fn endpoint() -> Option<String> {
    ENDPOINT
        .get()
        .as_ref()
        .and_then(|c| c.to_str().ok())
        .map(|s| s.trim_end_matches('/').to_string())
}

pub fn service_name() -> String {
    SERVICE_NAME
        .get()
        .as_ref()
        .and_then(|c| c.to_str().ok())
        .unwrap_or("postgresql")
        .to_string()
}

pub fn define_all() {
    GucRegistry::define_bool_guc(
        c"pg_otel_exporter.sdk_disabled",
        c"Disable the OpenTelemetry SDK",
        c"Equivalent of OTEL_SDK_DISABLED: when true, no signals are exported.",
        &SDK_DISABLED,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_string_guc(
        c"pg_otel_exporter.endpoint",
        c"OTLP endpoint base URL",
        c"e.g. http://localhost:4318 — signals are POSTed to {endpoint}/v1/{traces,metrics,logs}.",
        &ENDPOINT,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_string_guc(
        c"pg_otel_exporter.service_name",
        c"OTel service.name resource attribute",
        c"Defaults to 'postgresql'.",
        &SERVICE_NAME,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        c"pg_otel_exporter.traces",
        c"Emit query spans",
        c"Report each executed statement as an OTel span.",
        &TRACES,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_float_guc(
        c"pg_otel_exporter.traces_sampler_arg",
        c"Trace sampling ratio",
        c"Equivalent of OTEL_TRACES_SAMPLER_ARG: probability 0.0..1.0 that a root span (query without propagated context) is sampled.",
        &TRACES_SAMPLER_ARG,
        0.0,
        1.0,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        c"pg_otel_exporter.metrics",
        c"Enable OTel metrics collection",
        c"Collected by the background worker (requires shared_preload_libraries).",
        &METRICS,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"pg_otel_exporter.metric_export_interval",
        c"Metric export interval in milliseconds",
        c"Equivalent of OTEL_METRIC_EXPORT_INTERVAL: how often the background worker samples pg_stat_* views and exports metrics.",
        &METRIC_EXPORT_INTERVAL,
        1_000,
        3_600_000,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        c"pg_otel_exporter.logs",
        c"Export server log messages as OTel log records",
        c"",
        &LOGS,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_string_guc(
        c"pg_otel_exporter.log_level",
        c"Minimum log level to export",
        c"Equivalent of OTEL_LOG_LEVEL; accepts Postgres levels: debug5..debug1, info, notice, warning, error, log, fatal, panic.",
        &LOG_LEVEL,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"pg_otel_exporter.bsp_schedule_delay",
        c"Batching processor schedule delay in milliseconds",
        c"Equivalent of OTEL_BSP_SCHEDULE_DELAY: how often buffered telemetry is POSTed.",
        &BSP_SCHEDULE_DELAY,
        100,
        60_000,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"pg_otel_exporter.bsp_max_export_batch_size",
        c"Max events per OTLP export request",
        c"Equivalent of OTEL_BSP_MAX_EXPORT_BATCH_SIZE.",
        &BSP_MAX_EXPORT_BATCH_SIZE,
        1,
        100_000,
        GucContext::Userset,
        GucFlags::default(),
    );
}
