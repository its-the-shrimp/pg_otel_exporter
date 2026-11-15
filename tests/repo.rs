//! Repo-level invariants that need no extension code and no database.
//!
//! Plain `cargo test` (or `cargo pgrx test`, which wraps it) runs these like
//! any other test; the pre-commit hook (`.githooks/pre-commit`) relies on
//! them, so a violation blocks the commit instead of surfacing in review or
//! CI.

use std::{fs, process::Command};

/// The whole repo must be rustfmt-clean under the repo config
/// (`rustfmt.toml` needs nightly for `imports_granularity`).
#[test]
fn test_formatting_is_clean() {
    let status = Command::new("cargo")
        .args(["+nightly", "fmt", "--check"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .status()
        .expect("failed to run `cargo +nightly fmt --check`; is nightly rustfmt installed?");
    assert!(
        status.success(),
        "repo is not rustfmt-clean; run `cargo +nightly fmt` and commit the result"
    );
}

/// The crate has no default Postgres version feature: every build must pick
/// one explicitly (`--no-default-features -F pg14`..`pg19`). Building without
/// one fails at compile time (pgrx's build script, plus the `compile_error!`
/// in src/lib.rs); this test guards against a future accidental default.
#[test]
fn test_no_default_pg_version() {
    let manifest = fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"))
        .expect("failed to read Cargo.toml");
    let features = manifest
        .split("[features]")
        .nth(1)
        .expect("no [features] section in Cargo.toml")
        .split("[")
        .next()
        .expect("empty [features] section");
    let default = features
        .lines()
        .find(|line| {
            let trimmed = line.trim();
            trimmed.starts_with("default") && !trimmed.starts_with('#')
        })
        .expect("no `default` entry in [features]");
    let enabled: Vec<&str> = default
        .split('=')
        .nth(1)
        .expect("malformed `default` entry")
        .trim()
        .trim_start_matches('[')
        .trim_end_matches(']')
        .split(',')
        .map(|feature| feature.trim())
        .filter(|feature| !feature.is_empty())
        .collect();
    assert!(
        enabled.is_empty(),
        "Cargo.toml enables features by default ({enabled:?}); \
         every build must select a pg version explicitly"
    );
}
