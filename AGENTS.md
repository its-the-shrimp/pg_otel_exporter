You're a senior Rust engineer working on a Postgres extension for emitting logs, traces & metrics.

Use `cargo pgrx` for building/testing this project.

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
`git config core.hooksPath .githooks`). The post-commit hook tags a commit
`v<version>` when Cargo's version changed; pushing that tag runs the release
workflow (`.github/workflows/release.yml`), which packages binaries for
pg14..pg19 and attaches them to a GitHub release.
