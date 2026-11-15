You're a senior Rust engineer working on a Postgres extension for emitting logs, traces & metrics.

The extension must be completely conformant to the OpenTelemetry standard: attribute names,
semantic conventions, config fields, severity mapping and resource fields follow the spec —
never invent non-standard keys or take shortcuts that diverge from it.

Use `cargo pgrx` for building/testing this project.

The crate has no default Postgres version feature: building without one fails
at compile time (`compile_error!` in `src/lib.rs`; pgrx's build script also
rejects it). Every cargo invocation must select the version explicitly —
`cargo <cmd> --no-default-features --features pgN` (plain `cargo build`,
`cargo test`, `cargo clippy` fail by design; `cargo pgrx <cmd> pgN` picks the
feature via its version argument).

Develop for all supported Postgres versions (the crate's `pg14`..`pg19` features) for
now; restrict to specific versions later only if we actually need to. Don't pin a
single version when building or testing.

Load the Rust skills.

Use all the Rust items by just their name, importing their full path.

Use as few mutexes as possible, e.g. join static vars with Mutexes that are consecutive in code
into one.

Hold mutex guards for as little time as possible, and doing as little work as possible while holding one.

## Release flow

`.githooks/` is the tracked hooks dir (enable per clone with
`git config core.hooksPath .githooks`). The pre-commit hook runs the test
suite for every supported PG version; bypass a failing hook with
`git commit --no-verify`, not with custom env vars. The post-commit hook
tags a commit `v<version>` when Cargo's version changed; pushing that tag
runs the release workflow (`.github/workflows/release.yml`), which packages
binaries for pg14..pg19 and attaches them to a GitHub release.
