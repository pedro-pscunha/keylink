//! Two binaries, two type sets, one contract each.
//!
//! `demo/Cargo.toml` defines `swap-variants`, which reorders two identically
//! shaped unit variants of `Currency`. postcard encodes a variant by its
//! declaration index, so a peer built with that feature decodes every `Brl` as
//! `Usd` — no error, wrong money. Defeating that is the reason the ALPN is
//! derived from a schema hash, and it is the first claim the README makes.
//!
//! Until this file existed, nothing built that feature. The nearest test,
//! `protocol.rs::a_peer_on_a_different_schema_is_reported_as_a_schema_mismatch`,
//! dials a hand-written ALPN literal: it proves keylink refuses an ALPN it does
//! not recognise, which is a weaker statement than it looks, because the
//! literal was typed by a person rather than derived from a second type set. As
//! CONTRIBUTING.md puts it, a test cannot catch a change it participates in.
//!
//! So this file compiles `demo` twice and asks the binaries themselves.
//!
//! ## What it costs, measured
//!
//! It is in `gate` rather than behind `#[ignore]`, because a check no hook runs
//! is a check that rots — CONTRIBUTING.md tells the story of `cargo deny` being
//! broken for three commits for exactly that reason. On an unloaded machine:
//!
//! | | |
//! |---|---|
//! | `cargo build -p demo` | 0.60s |
//! | the same with `swap-variants` | 0.61s |
//! | three child processes | 0.76s |
//!
//! About 2s, against a 7.7s gate. Note that `cargo test --workspace` does *not*
//! build the demo binary — it builds demo's test target, a different artifact —
//! so both builds are this file's to pay. Cargo fingerprints each feature set
//! separately, so neither build evicts the other and a repeat run is 0.11s.
//!
//! ## What it does not do
//!
//! It proves the two binaries derive different contracts. It does not yet run
//! them against each other, because `demo serve` waits on `Endpoint::online()`,
//! which returns only once a home relay connects, and `demo connect` takes a
//! bare `EndpointId` that only the public n0 DNS can resolve. Neither works on
//! loopback. Proving the refused handshake needs an offline path through
//! `demo/src/main.rs`, which is why one is proposed rather than assumed here.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::OnceLock;

/// The directory cargo puts finished binaries in, derived from this test
/// binary's own location (`<build>/deps/drift-<hash>`) rather than assumed, so
/// a `CARGO_TARGET_DIR` moves both together.
fn build_dir() -> PathBuf {
    let exe = std::env::current_exe().expect("the test binary knows its own path");
    exe.parent()
        .and_then(Path::parent)
        .expect("the test binary sits in <build>/deps")
        .to_path_buf()
}

/// Build `demo` with `features`, and keep the result somewhere the next build
/// will not overwrite.
///
/// The copy is the whole point. Both feature sets land on the same
/// `<build>/demo` path, so without it the second build destroys the first.
/// Cargo fingerprints each feature set separately and keeps both artifacts
/// under `deps/`, so a repeat run is a fingerprint check rather than a compile.
fn build_demo(name: &str, features: &[&str]) -> PathBuf {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("keylink/ sits inside the workspace");

    let mut cargo = Command::new(option_env!("CARGO").unwrap_or("cargo"));
    cargo
        .current_dir(workspace)
        .args(["build", "--package", "demo"]);
    for feature in features {
        cargo.args(["--features", feature]);
    }

    let status = cargo.status().expect("cargo runs");
    assert!(status.success(), "building demo {features:?} failed");

    let kept = build_dir().join("keylink-drift").join(name);
    std::fs::create_dir_all(kept.parent().expect("a parent")).expect("the directory is creatable");
    std::fs::copy(build_dir().join("demo"), &kept).expect("the built binary is copyable");
    kept
}

/// The two binaries, built once however many tests ask for them.
///
/// Swapped is built first so the tree is left in its default feature state,
/// and `OnceLock` keeps two test threads from running two cargo processes at
/// each other.
fn binaries() -> &'static (PathBuf, PathBuf) {
    static BINARIES: OnceLock<(PathBuf, PathBuf)> = OnceLock::new();
    BINARIES.get_or_init(|| {
        let swapped = build_demo("demo-swapped", &["demo/swap-variants"]);
        let baseline = build_demo("demo-baseline", &[]);
        (baseline, swapped)
    })
}

/// Any endpoint id that parses. `demo connect` parses its argument before it
/// binds anything, and this test never lets the dial happen, so nothing about
/// this id has to be reachable.
fn an_endpoint_id(demo: &Path) -> String {
    let out = Command::new(demo)
        .arg("id")
        .stderr(Stdio::null())
        .output()
        .expect("demo id runs");
    assert!(out.status.success(), "demo id failed");
    String::from_utf8(out.stdout)
        .expect("an endpoint id is utf-8")
        .trim()
        .to_string()
}

/// The ALPN a binary presents, taken from the binary rather than from a
/// literal.
///
/// `demo connect` prints its ALPN before it dials, so this reads one line and
/// kills the child. No network, and no waiting for a dial that is meant to
/// fail.
fn alpn_of(demo: &Path, peer: &str) -> String {
    let mut child = Command::new(demo)
        .args(["connect", peer])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("demo connect starts");

    let stdout = child.stdout.take().expect("stdout was piped");
    let alpn = BufReader::new(stdout)
        .lines()
        .map_while(Result::ok)
        .find_map(|line| {
            line.strip_prefix("alpn:")
                .map(|rest| rest.trim().to_string())
        });

    // Before the unwrap, so a missing line does not also leak a process.
    let _ = child.kill();
    let _ = child.wait();

    alpn.expect("demo connect prints its alpn before it dials")
}

/// The property the ALPN scheme exists for, asked of two real binaries.
///
/// Reordering two unit variants changes nothing a compiler would complain
/// about and nothing postcard would refuse to decode. It has to change the
/// contract, or two peers silently disagree about what a price means.
#[test]
fn two_type_sets_derive_two_contracts() {
    let (baseline, swapped) = binaries();

    // Deliberately no `assert_ne!` on the two files' bytes. It says nothing the
    // ALPN comparison below does not say better, and on failure `assert_ne!`
    // prints both operands — 583 MB of Mach-O, which buries the one line that
    // explains what went wrong.
    let peer = an_endpoint_id(baseline);
    let (from_baseline, from_swapped) = (alpn_of(baseline, &peer), alpn_of(swapped, &peer));

    assert_ne!(
        from_baseline, from_swapped,
        "swapping two same-shaped variants must move the contract, or the drift is invisible"
    );

    // Same service, same protocol name: only the schema hash may differ. A
    // difference in the prefix would mean this test passed for the wrong
    // reason.
    let prefix = |alpn: &str| {
        alpn.rsplit_once('/')
            .expect("<protocol>/<hash>")
            .0
            .to_string()
    };
    assert_eq!(
        prefix(&from_baseline),
        prefix(&from_swapped),
        "only the hash may move: {from_baseline} vs {from_swapped}"
    );
}
