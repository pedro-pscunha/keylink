//! Two binaries, two type sets, one refused handshake.
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
//! So this file compiles `demo` twice and puts the two binaries in a room.
//!
//! ## The trap this file is built to avoid
//!
//! Both builds report the same artifact path. Cargo keeps a separate object per
//! feature set under `deps/`, but it *uplifts* whichever it built last to one
//! `<build>/demo`, and `--message-format=json` names only that path. Build
//! twice, run `<build>/demo` twice, and you have run one binary against itself:
//! two identical peers agree, the test passes, and it has proved nothing —
//! planted inside the test whose whole purpose is to catch exactly that.
//!
//! `build_demo` therefore copies the uplifted file to a distinct path the
//! moment each build finishes, and every test below runs two different paths.
//! `CARGO_BIN_EXE_demo` cannot help here whatever package this file lives in:
//! it names one path, and this test needs two.
//!
//! ## What it costs, measured
//!
//! It is in `gate` rather than behind `#[ignore]`, because a check no hook runs
//! is a check that rots — CONTRIBUTING.md tells that story about `cargo deny`
//! being broken for three commits.
//!
//! The delta is **about 1.1s**, measured as two back-to-back `cargo test
//! --workspace` runs differing only in `--skip`: 59.19s without these two tests,
//! 60.25s with them. That covers both builds and every child process, including
//! the 8 MiB transfer the control ends up doing.
//!
//! Two things that number depends on. `cargo test --workspace` does *not* build
//! the demo binary — it builds demo's test target, a different artifact — so
//! both builds are this file's to pay. And cargo fingerprints each feature set
//! separately, so neither build evicts the other and a settled repeat is 0.11s.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdout, Command, Stdio};
use std::sync::OnceLock;

/// The identity every client below dials with.
///
/// A fixed secret rather than a fresh one, because with no discovery the server
/// must allowlist its peer by an id known *before* either process starts, and
/// `bind_local()` alone takes a new identity every run. `PEER_ID` is the public
/// key of `PEER_SECRET`; re-derive it any time with
/// `KEYLINK_SECRET=<secret> demo id`.
///
/// It doubles as the dial target in `alpn_of`, which only needs something that
/// parses: `demo connect` parses its argument before it binds anything, and
/// that child is killed before the dial matters.
const PEER_SECRET: &str = "2222222222222222222222222222222222222222222222222222222222222222";
const PEER_ID: &str = "a09aa5f47a6759802ff955f8dc2d2a14a5c99d23be97f864127ff9383455a4f0";

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

/// Build `demo` with `features`, and copy the result somewhere the next build
/// cannot overwrite. See the module doc: the copy is what stops this whole file
/// from testing one binary against itself.
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
/// Swapped is built first so the tree is left in its default feature state, and
/// `OnceLock` keeps two test threads from running two cargo processes at each
/// other.
///
/// **The `OnceLock` is only half the guard now.** It spans threads, and the gate
/// runs this file under nextest, which gives every test its own PROCESS. The
/// other half lives in `.config/nextest.toml`, as a `max-threads = 1` test group
/// filtered to `binary(drift)`. Delete that group and these two tests build and
/// copy at each other again — see the comment there for what that actually
/// costs. Neither half is sufficient alone.
fn binaries() -> &'static (PathBuf, PathBuf) {
    static BINARIES: OnceLock<(PathBuf, PathBuf)> = OnceLock::new();
    BINARIES.get_or_init(|| {
        let swapped = build_demo("demo-swapped", &["demo/swap-variants"]);
        let baseline = build_demo("demo-baseline", &[]);
        (baseline, swapped)
    })
}

/// The ALPN a binary presents, taken from the binary rather than from a
/// literal.
///
/// `demo connect` prints its ALPN **before anything that can fail**, so this
/// reads one line and kills the child rather than waiting for a dial that is
/// meant to fail.
///
/// *Before it dials* is what this said, and it was the weaker property: the
/// print sat under `dial_target` and the bind, so a transient failure in either
/// exited the child before the line and left this reading EOF. `main.rs`'s
/// comment at that `println!` is the other half of this pair.
///
/// **The child's stderr is inherited, and that is this function's only
/// diagnostic.** It was `Stdio::null()`, which cost two blocked pushes: when the
/// child dies before printing, the reader below sees EOF, `alpn` is `None`, and
/// the panic could say only that no line arrived — whatever the child said about
/// why went to `/dev/null`. Inheriting costs nothing on the passing path, since
/// nextest discards a passing test's captured output and prints it on failure.
///
/// It is inherited rather than piped **because piping can hang this function**:
/// nothing here drains stderr until the stdout read finishes, so a child that
/// filled the stderr pipe buffer would block writing it, never write stdout, and
/// leave the reader below waiting on an EOF that cannot come.
fn alpn_of(demo: &Path) -> String {
    let mut child = Command::new(demo)
        .args(["connect", PEER_ID])
        .env("KEYLINK_OFFLINE", "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("demo connect starts");

    let stdout = child.stdout.take().expect("stdout was piped");
    // Not `map_while(Result::ok)`, which **ends the scan on a read error and says
    // nothing** — the caller then sees exactly what it sees at a clean EOF. That
    // is the same defect as the `Stdio::null()` this function used to carry, one
    // layer in: the failure arrives as an absence. Read errors are kept and
    // reported.
    let mut alpn = None;
    let mut read_error = None;
    for line in BufReader::new(stdout).lines() {
        match line {
            Ok(line) => {
                if let Some(rest) = line.strip_prefix("alpn:") {
                    alpn = Some(rest.trim().to_string());
                    break;
                }
            }
            Err(error) => {
                read_error = Some(error);
                break;
            }
        }
    }

    // **Ask before killing, because `kill` is SIGKILL and would overwrite the
    // answer.** The first version of this reported `child.wait()` after
    // `child.kill()` and claimed it showed what the child exited with. That holds
    // only when the child was already dead. When it was still running — which is
    // what a read error leaves behind — the status read `signal: 9 (SIGKILL)`,
    // which is this harness describing its own kill. A diagnostic that reports
    // the observer's action as the subject's fate is worse than none.
    let ended = match child.try_wait() {
        Ok(Some(status)) => format!("it ended on its own with {status}"),
        // Deliberately not "it hung". Stdout reaches EOF when the write end
        // closes, which for this child happens as it exits, so a `try_wait` here
        // races the exit it just observed: a child that is merely slow to be
        // reaped reads exactly like one that is wedged. Measured — the forced
        // missing-line path reports this branch for a child that was on its way
        // out. The wording carries the ambiguity because the call cannot resolve
        // it, and a harness that names its own race is worth more than one that
        // guesses which side of it won.
        Ok(None) => "it had not exited when asked, which a child merely slow to \
                     exit also reports, so this harness killed it"
            .to_owned(),
        Err(error) => format!("its state was unreadable ({error})"),
    };

    // After the question above, and before the panic, so a missing line does not
    // also leak a process.
    let _ = child.kill();
    let _ = child.wait();

    alpn.unwrap_or_else(|| match read_error {
        Some(error) => panic!(
            "demo connect prints its alpn before it dials; reading its stdout failed \
             ({error}) and {ended}"
        ),
        None => panic!(
            "demo connect prints its alpn before it dials; its stdout ended without \
             one and {ended}"
        ),
    })
}

/// A `demo serve` running on loopback, and the address a peer needs to reach it.
struct Serving {
    child: Child,
    dial: String,
    /// Held only to keep the pipe open. Dropping the reader would close our end,
    /// and the next thing the server printed would kill it with `EPIPE`.
    _stdout: BufReader<ChildStdout>,
}

impl Drop for Serving {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Start a server with no relay and no discovery, and wait for it to say where
/// it can be reached.
///
/// `KEYLINK_OFFLINE` is what makes this hermetic. Without it `demo serve` waits
/// on `Endpoint::online()`, which returns only once a home relay reports
/// connected, and the peer would have to be found through n0's public DNS.
fn serve(demo: &Path) -> Serving {
    let mut child = Command::new(demo)
        .arg("serve")
        .env("KEYLINK_OFFLINE", "1")
        .env("KEYLINK_ALLOWED_IDS", PEER_ID)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("demo serve starts");

    let mut reader = BufReader::new(child.stdout.take().expect("stdout was piped"));
    let mut dial = None;
    let mut line = String::new();
    while reader.read_line(&mut line).unwrap_or(0) > 0 {
        if let Some(rest) = line.strip_prefix("dial:") {
            dial = Some(rest.trim().to_string());
            break;
        }
        line.clear();
    }

    match dial {
        Some(dial) => Serving {
            child,
            dial,
            _stdout: reader,
        },
        None => {
            let _ = child.kill();
            let _ = child.wait();
            panic!("demo serve never printed a dial line");
        }
    }
}

/// Everything `demo connect` said, stdout and stderr together.
///
/// The interesting line is on stderr: `main` returns the error and the runtime
/// prints it there.
fn connect_to(demo: &Path, dial: &str) -> String {
    let out = Command::new(demo)
        .args(["connect", dial])
        .env("KEYLINK_OFFLINE", "1")
        .env("KEYLINK_SECRET", PEER_SECRET)
        .output()
        .expect("demo connect runs");

    let mut said = format!("[{}]\n", out.status);
    said.push_str(&String::from_utf8_lossy(&out.stdout));
    said.push_str(&String::from_utf8_lossy(&out.stderr));
    said
}

/// The property the ALPN scheme exists for, asked of two real binaries.
///
/// Reordering two unit variants changes nothing a compiler would complain about
/// and nothing postcard would refuse to decode. It has to change the contract,
/// or two peers silently disagree about what a price means.
#[test]
fn two_type_sets_derive_two_contracts() {
    let (baseline, swapped) = binaries();

    // Deliberately no `assert_ne!` on the two files' bytes. It says nothing the
    // ALPN comparison does not say better, and on failure `assert_ne!` prints
    // both operands — 583 MB of Mach-O burying the one useful line.
    let (from_baseline, from_swapped) = (alpn_of(baseline), alpn_of(swapped));

    assert_ne!(
        from_baseline, from_swapped,
        "swapping two same-shaped variants must move the contract, or the drift is invisible"
    );

    // Same service, same protocol name: only the schema hash may move. A
    // difference in the prefix would mean this passed for the wrong reason.
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

/// The claim the README leads with, run end to end for the first time.
///
/// Two processes, two type sets, a real QUIC handshake over loopback. The
/// second half is the control, and it is the half that would catch a broken
/// harness: if these two binaries were secretly the same file, or if the ALPN
/// gate were not doing the work, the swapped client would fail the same way the
/// baseline one does instead of differently.
#[test]
fn a_peer_built_from_other_types_is_refused_at_the_handshake() {
    let (baseline, swapped) = binaries();
    let server = serve(baseline);

    let refused = connect_to(swapped, &server.dial);
    assert!(
        refused.contains("schema mismatch"),
        "a peer built from other types must be refused for the schema, and told so \
         rather than sent to look at the network. It said:\n{refused}"
    );

    // The control: same types on both ends, so the whole conversation runs.
    let agreed = connect_to(baseline, &server.dial);
    assert!(
        !agreed.contains("schema mismatch"),
        "two binaries from one type set must agree on the contract. It said:\n{agreed}"
    );
    assert!(
        agreed.contains("3 items"),
        "the agreeing client must get a real reply, not merely a connection. \
         It said:\n{agreed}"
    );

    // And the payoff. `Brl` is what the swapped build would have decoded as
    // `Usd`, silently, had the handshake let it through. Reading it back
    // correctly is the thing the whole ALPN scheme is protecting.
    assert!(
        agreed.contains("Brl"),
        "prices must come back in the currency they were sent in. It said:\n{agreed}"
    );
}
