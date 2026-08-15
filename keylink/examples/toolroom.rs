//! keylink from a program that is not async, and has no intention of becoming one.
//!
//! ```sh
//! cargo run -p keylink --example toolroom
//! ```
//!
//! Two nodes on the loopback interface. Nothing here is `async`, there is no
//! `#[tokio::main]`, and no future is awaited anywhere in this file.
//!
//! # What the synchronous API is for
//!
//! A command-line tool, a build step, a script that talks to one service and
//! exits. Making such a program async costs it a runtime, a `main` attribute
//! and a colour on every function it already had. [`keylink::blocking`] is the
//! answer: every type mirrors its async twin method for method, and a [`Node`]
//! owns a private runtime it shares with every client and server built from it.
//!
//! # Who owns the runtime, and why that is not a global
//!
//! Asking the caller for a runtime would put back exactly the plumbing this
//! module removes. A process-wide one would be a resource nobody can shut down.
//! So the node owns it, and two blocking nodes cost two runtimes — the right
//! trade for a program that has one node.
//!
//! It is **multi-threaded on purpose**. iroh keeps background work alive
//! between calls, and a current-thread runtime only advances tasks while a
//! `block_on` is in progress, so a connection would stall between calls.
//!
//! # Calling it from async code is an error rather than a deadlock
//!
//! Blocking on a future from inside a runtime parks a worker on work that same
//! runtime has to perform. Every entry point checks first and refuses. Act 4
//! measures that.
//!
//! # One condition, two error types, and the second is not redundant
//!
//! A crate-level door answers [`Error::BlockingInAsync`](keylink::Error), one
//! arm among many. A *generated* method answers
//! [`BlockingError<Error>`](keylink::BlockingError) — the async error type with
//! exactly one arm added. Widening `Error` instead would have been simpler and
//! would have cost the thing that matters: `Operation` carries the async failure
//! untouched, so a caller who has ruled out its own misuse is left holding
//! precisely what the async surface would have handed it. Act 5 measures both
//! arms.
//!
//! # An actor needs the runtime lent to it, and the guard is exclusive
//!
//! `actor()` and `spawn()` start a task, and `tokio::spawn` reads the current
//! runtime from a thread-local a synchronous thread does not have — it panics
//! rather than returning an error, so there is nothing for this module to
//! convert. [`Node::enter`](keylink::blocking::Node) lends the runtime for
//! exactly that.
//!
//! **Build and serve inside one guarded scope, then drop the guard.** While it
//! is held, everything routed through `Runtime::run` answers
//! `BlockingInAsync`, because the check cannot tell a borrowed context from a
//! real one — that is every client call. `serve` is the other way round: it
//! never reaches that check and panics inside iroh without a runtime context,
//! so it wants the guard rather than refusing it. Act 2 has both lines inside
//! the braces for that reason.
//!
//! # What is not mirrored
//!
//! `send` and `recv` stay async-only. Their bodies are `AsyncRead`/`AsyncWrite`
//! by nature, and a synchronous wrapper would either buffer a whole file in
//! memory or hand back a handle that only works inside a runtime. This service
//! declares neither.

use std::error::Error as StdError;

use keylink::prelude::*;
use keylink::{Allowlist, BlockingError, Caller, Error, IdempotencyKey, KeyState, SecretKey};

keylink::service! {
    service Toolroom;

    /// Book a tool out. Keyed below, so a repeat is a replay.
    rpc Borrow(tool: String) -> u32;

    /// How many bookings actually ran.
    rpc Booked() -> u32;
}

/// The toolroom. An ordinary actor — nothing about it knows its callers are
/// synchronous.
#[derive(Default)]
struct Toolroom {
    booked: u32,
}

impl toolroom::Actor for Toolroom {
    async fn borrow(&mut self, _cx: Caller<'_>, _tool: String) -> u32 {
        self.booked += 1;
        self.booked
    }

    async fn booked(&mut self, _cx: Caller<'_>) -> u32 {
        self.booked
    }
}

/// The shop's identity, fixed so the allowlist below can name it in advance.
const SHOP: [u8; 32] = [31; 32];

fn main() -> Result<(), Box<dyn StdError>> {
    // ── 1. Two nodes, bound from a synchronous `main`.
    //
    //  `bind_local` binds with no relay and no discovery, so this runs offline.
    //  It is the synchronous twin of the constructor every other example here
    //  uses, and until this wave it did not exist — which is why keylink had no
    //  blocking example at all.
    let shop = keylink::blocking::Node::bind_local_with_secret(SecretKey::from_bytes(&SHOP))?;
    let bench = keylink::blocking::Node::bind_local()?;
    println!("the shop is {}", shop.id());

    // ── 2. An actor, built under a lent runtime.
    //
    //  The braces are load bearing, and BOTH lines belong inside them.
    //
    //  `actor()` spawns, and `tokio::spawn` reads a thread-local a synchronous
    //  thread does not have. `serve` reaches iroh's router, which spawns too —
    //  so without the guard it does not answer `BlockingInAsync`, it PANICS
    //  inside iroh with `there is no reactor running`. `Node::enter`'s own
    //  documentation said the reverse until this example was written.
    //
    //  What the guard does refuse is anything routed through `Runtime::run`,
    //  which is every client call below. Hence the drop before act 3.
    let (_counter, _serving) = {
        let _guard = shop.enter()?;
        let counter = toolroom::actor(Toolroom::default());
        let serving = shop.serve([counter.service(Allowlist::only([bench.id()])).build()])?;
        (counter, serving)
    };

    // ── 3. A client, and a keyed call, with no runtime in sight.
    //
    //  `connect` takes a full `EndpointAddr`, which carries direct socket
    //  addresses — so a loopback dial needs no discovery service.
    let client = toolroom::blocking::Client::connect(&bench, shop.inner().endpoint().addr())?;

    let ticket = IdempotencyKey::new("job-4120")?;
    let first = client
        .with_idempotency(ticket.clone())
        .borrow("lathe".to_string())?;
    let again = client
        .with_idempotency(ticket.clone())
        .borrow("lathe".to_string())?;
    println!("booked {first}, then the retry replayed {again}");
    assert_eq!(first, again, "the retry must replay, not re-book");
    assert_eq!(client.booked()?, 1, "one job, one booking");

    //  `BlockingWire` is the synchronous half of the `Wire` extension trait, and
    //  `key_state` is the question a caller asks before deciding anything.
    assert_eq!(
        BlockingWire::key_state(&client, ticket)?,
        KeyState::Done,
        "a settled key reads the same from a synchronous caller"
    );

    // ── 4. The one difference that is not merely a spelling.
    //
    //  Blocking on a future from inside a runtime parks a worker on work that
    //  runtime has to perform. It is refused rather than deadlocked, and the
    //  refusal is its own variant so a caller can tell it from a transport
    //  failure.
    let refused = std::thread::spawn(|| {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| error.to_string())?;
        runtime.block_on(async {
            match keylink::blocking::Node::bind_local() {
                Ok(_) => Err("binding inside a runtime must be refused".to_string()),
                Err(error) => Ok(error),
            }
        })
    })
    .join()
    .map_err(|_| "the probe thread panicked")??;
    assert!(
        matches!(refused, Error::BlockingInAsync),
        "the refusal names its own cause, got {refused:?}"
    );
    println!("from inside a runtime it refuses rather than deadlocking: {refused}");

    // ── 5. The same condition, through a generated method, has a different type.
    //
    //  A crate-level door answers [`Error`], whose `BlockingInAsync` variant is
    //  one arm among many. A generated method answers
    //  [`BlockingError<Error>`](keylink::BlockingError) instead, which is the
    //  async error type with **exactly one** arm added. That is the whole reason
    //  the second type exists: wrapping the async error rather than widening it
    //  keeps `Operation`'s payload precise, so a caller who has ruled out its own
    //  misuse is left holding the same `Error` the async surface would have given.
    let probe = toolroom::blocking::Client::connect(&bench, shop.inner().endpoint().addr())?;
    let wrapped = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| error.to_string())?;
        runtime.block_on(async {
            match probe.booked() {
                Ok(_) => Err("a generated call inside a runtime must be refused".to_string()),
                Err(error) => Ok(error),
            }
        })
    })
    .join()
    .map_err(|_| "the probe thread panicked")??;
    assert!(
        matches!(wrapped, BlockingError::BlockingInAsync),
        "the generated door names the same cause in its own type, got {wrapped:?}"
    );
    println!("and a generated method says it in its own type: {wrapped}");

    //  The other arm carries the async failure untouched. A node the shop does
    //  not allowlist is refused by the shop, which is an ordinary [`Error`] that
    //  has nothing to do with blocking — and it arrives inside `Operation`
    //  rather than flattened into a variant beside `BlockingInAsync`.
    let stranger = keylink::blocking::Node::bind_local()?;
    let outsider = toolroom::blocking::Client::connect(&stranger, shop.inner().endpoint().addr())?;
    let denied = outsider
        .booked()
        .err()
        .ok_or("a peer outside the allowlist must be refused")?;
    match denied {
        BlockingError::Operation(error) => {
            println!("a refused peer, unwrapped: {error}");
            assert!(
                !matches!(error, Error::BlockingInAsync),
                "the inner error is the async one, untouched"
            );
        }
        BlockingError::BlockingInAsync => {
            return Err("this thread has no runtime, so this arm is wrong".into())
        }
        other => return Err(format!("unexpected: {other:?}").into()),
    }

    // ── 6. Shutting down, which is also synchronous.
    //
    //  `close` is `BlockingWire`'s, and it takes `self` — the same shape the
    //  async `Wire::close` has, so a reader moving between the two rewrites
    //  nothing.
    BlockingWire::close(client)?;
    println!("the client closed; the shop is still serving");
    Ok(())
}
