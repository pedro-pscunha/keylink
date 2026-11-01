//! Fast gate target: all native integration suites that are safe to share one process.
//!
//! Cargo otherwise starts one process per file. Most of these tests leave iroh's
//! endpoint cleanup to process teardown, so that repeats the same two-second drain
//! for every file. Keeping the modules in one libtest process pays it once.
//!
//! Six suites stay separate: `actor_memory` and `mailbox_latency` use process-wide
//! measurements, `drift` runs nested Cargo builds, `keyed_topic_allocation` and
//! `grant_allocation` own a process-global allocator, and `reliable` needs
//! process-level scheduling isolation.
//! `browser` is a wasm-only target. `every_test_file_is_in_the_gate` makes this
//! list and both isolated runner selections fail closed when a file is added.

#![allow(clippy::duplicate_mod, clippy::expect_used, clippy::unwrap_used)]

macro_rules! gate_modules {
    ($($module:ident => $path:literal),+ $(,)?) => {
        $(
            #[path = $path]
            mod $module;
        )+

        const INCLUDED: &[&str] = &[$($path),+];
    };
}

gate_modules! {
    actors => "../tests/actors.rs",
    admission => "../tests/admission.rs",
    api => "../tests/api.rs",
    away => "../tests/away.rs",
    blobs => "../tests/blobs.rs",
    blocking_work => "../tests/blocking_work.rs",
    compose => "../tests/compose.rs",
    cycles => "../tests/cycles.rs",
    declaration_seam => "../tests/declaration_seam.rs",
    defer => "../tests/defer.rs",
    durable => "../tests/durable.rs",
    feeds => "../tests/feeds.rs",
    idempotency => "../tests/idempotency.rs",
    inspect => "../tests/inspect.rs",
    observe => "../tests/observe.rs",
    peer => "../tests/peer.rs",
    persist => "../tests/persist.rs",
    protocol => "../tests/protocol.rs",
    push => "../tests/push.rs",
    relay => "../tests/relay.rs",
    singleton => "../tests/singleton.rs",
    supervision => "../tests/supervision.rs",
    surfaces => "../tests/surfaces.rs",
    tell => "../tests/tell.rs",
    trust => "../tests/trust.rs",
    watch => "../tests/watch.rs",
    versions => "../tests/versions.rs",
}

#[test]
fn every_test_file_is_in_the_gate() {
    use std::collections::BTreeSet;
    use std::path::Path;

    const NATIVE_SEPARATE: &[&str] = &[
        "actor_memory",
        "drift",
        "grant_allocation",
        "keyed_topic_allocation",
        "mailbox_latency",
        "reliable",
    ];
    const NON_NATIVE: &[&str] = &["browser"];
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let here = manifest.join("tests");
    let actual: BTreeSet<_> = std::fs::read_dir(here)
        .expect("the integration-test directory exists")
        .map(|entry| entry.expect("the directory is readable").path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "rs"))
        .map(|path| path.file_name().expect("a file has a name").to_owned())
        .collect();
    let listed: BTreeSet<_> = INCLUDED
        .iter()
        .map(|path| {
            Path::new(path)
                .file_name()
                .expect("an included test path has a file name")
                .to_owned()
        })
        .chain(
            NATIVE_SEPARATE
                .iter()
                .chain(NON_NATIVE)
                .map(|name| format!("{name}.rs").into()),
        )
        .collect();

    assert_eq!(
        actual, listed,
        "add every new suite above or to an explicitly selected isolated set"
    );

    let root = manifest
        .parent()
        .expect("the crate belongs to the workspace");
    let mise = std::fs::read_to_string(root.join("mise.toml"))
        .expect("the repository test task is readable");
    let native_task = mise
        .split_once("[tasks.test]")
        .and_then(|(_, rest)| rest.split_once("[tasks.examples]"))
        .map(|(task, _)| task)
        .expect("mise.toml contains the test task before the examples task");
    let native_commands = native_task
        .lines()
        .filter(|line| !line.trim_start().starts_with('#'))
        .collect::<Vec<_>>()
        .join("\n");
    let native_words: Vec<_> = native_commands.split_ascii_whitespace().collect();
    for name in NATIVE_SEPARATE {
        assert_eq!(
            native_words
                .windows(2)
                .filter(|words| words[0] == "--test" && words[1] == *name)
                .count(),
            1,
            "the native test task must select isolated suite {name} exactly once"
        );
    }

    let browser = std::fs::read_to_string(root.join(".mise/tasks/wasm-browser"))
        .expect("the browser test task is readable");
    let browser_commands = browser
        .lines()
        .filter(|line| !line.trim_start().starts_with('#'))
        .collect::<Vec<_>>()
        .join("\n");
    let browser_words: Vec<_> = browser_commands.split_ascii_whitespace().collect();
    for name in NON_NATIVE {
        assert_eq!(
            browser_words
                .windows(2)
                .filter(|words| words[0] == "--test" && words[1] == *name)
                .count(),
            1,
            "the browser test task must select non-native suite {name} exactly once"
        );
    }
}
