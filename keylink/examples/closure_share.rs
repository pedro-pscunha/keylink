//! One grant for a whole tree: have/want, and taking it back.
//!
//! ```sh
//! cargo run -p keylink --example closure_share
//! cargo run -p keylink --example closure_share -- serve [--members N]
//! cargo run -p keylink --example closure_share -- fetch <EndpointId> [--preseed K]
//! ```
//!
//! A holder lists N blobs in one hash sequence and hands a caller that sequence
//! as [`Content::Closure`]. The caller holds **one** grant and may fetch all N
//! blobs, which a ledger that granted blob by blob could not do past its cap.
//! The fetcher already holds K of them, so it asks its own store what it lacks
//! and moves only the other N - K. The holder then takes the grant back.
//!
//! # Two ways to run it
//!
//! With no arguments both sides run in this process over [`Node::bind_local`]:
//! no network, and this is what `mise run examples` runs.
//!
//! `serve` and `fetch` run the two sides on two machines over [`Node::bind`]:
//! n0's public relays and discovery, so a bare [`EndpointId`] is enough to dial
//! and the path is a hole punch or a relay. Start `serve` on one machine, copy
//! the id it prints, and run `fetch <id>` on the other; allow UDP at a firewall
//! prompt. `serve` uses [`Allowlist::anyone`], which is a demo: any node that
//! learns its id may call it. `fetch` prints each value and the time it took
//! (printed, never asserted), and ends with one line, `closure_share: OK`, or
//! exits non-zero on the first mismatch.

use std::error::Error as StdError;
use std::time::Instant;

use keylink::{Allowlist, BlobId, Blobs, Context, EndpointId, Error, Grant, Missing, Node, Server};
use tokio::io::AsyncReadExt;

keylink::service! {
    service Shelf;

    /// The sequence: one grant for every blob it lists.
    blob Snapshot() -> u32;

    /// Take every grant the caller holds back. Answers how many were withdrawn.
    rpc Leave() -> u32;
}

/// 1024 bytes that depend on `nth`, so two members never share an address.
fn member(nth: u32) -> Vec<u8> {
    nth.to_le_bytes().repeat(256)
}

/// Add `member(range)` to `blobs` and return their addresses in order.
async fn add_members(
    blobs: &Blobs,
    range: std::ops::Range<u32>,
) -> Result<Vec<BlobId>, Box<dyn StdError>> {
    let mut ids = Vec::with_capacity(range.len());
    for nth in range {
        ids.push(blobs.add_bytes(member(nth)).await?);
    }
    Ok(ids)
}

struct Holder {
    blobs: Blobs,
    count: u32,
    seq: keylink::SeqId,
}

impl shelf::Handler for Holder {
    async fn snapshot(&self, _cx: &Context) -> (u32, Grant) {
        (self.count, self.seq.into())
    }

    async fn leave(&self, cx: &Context) -> u32 {
        self.blobs.revoke_peer(cx.peer()) as u32
    }
}

/// A node serving `members` blobs and the one sequence that lists them.
async fn serve(node: &Node, members: u32) -> Result<Server, Box<dyn StdError>> {
    let blobs = Blobs::memory();
    let ids = add_members(&blobs, 0..members).await?;
    let seq = blobs.add_seq(ids).await?;
    let holder = Holder {
        blobs: blobs.clone(),
        count: members,
        seq,
    };
    Ok(Server::builder(node)
        .service(
            shelf::service(holder, Allowlist::anyone())
                .with_blobs(&blobs)
                .build(),
        )
        .spawn()?)
}

/// The caller's side, against whichever holder `client` is connected to.
///
/// Every step is checked, and the first mismatch is the error.
async fn fetch(client: &shelf::Client, preseed: u32) -> Result<(), Box<dyn StdError>> {
    // The fetcher already holds the first `preseed` blobs.
    let local = Blobs::memory();
    add_members(&local, 0..preseed).await?;

    let started = Instant::now();
    let (count, handle) = client.snapshot().await?;
    println!(
        "snapshot: {count} blobs, one grant ({:?})",
        started.elapsed()
    );
    if preseed > count {
        return Err(format!("preseed {preseed} is more than the {count} the holder lists").into());
    }
    let lacking = u64::from(count - preseed);

    // 1. Have/want, answered from the fetcher's own store.
    let started = Instant::now();
    match handle.missing(&local).await? {
        Missing::Members(members) if members.len() as u64 == lacking && lacking > 0 => {
            println!(
                "missing: {} of {count} ({:?})",
                members.len(),
                started.elapsed()
            );
        }
        // Everything is already held, or there is nothing to hold.
        Missing::Nothing if lacking == 0 => {
            println!("missing: nothing of {count} ({:?})", started.elapsed());
        }
        other => return Err(format!("expected {lacking} members missing, got {other:?}").into()),
    }

    // 2. Fetch only that.
    let started = Instant::now();
    let moved = handle.fetch_into(&local).await?;
    println!(
        "fetch_into: {moved} payload bytes ({:?})",
        started.elapsed()
    );
    if moved != lacking * 1024 {
        return Err(format!("expected {} payload bytes, moved {moved}", lacking * 1024).into());
    }

    // 3. Nothing is missing now, and asking again moves nothing.
    let after = handle.missing(&local).await?;
    if after != Missing::Nothing {
        return Err(format!("expected nothing missing, got {after:?}").into());
    }
    let again = handle.fetch_into(&local).await?;
    if again != 0 {
        return Err(format!("a second fetch moved {again} bytes").into());
    }
    println!("missing: nothing; a second fetch_into moved 0 bytes");

    // 4. The holder takes the grant back, and the next request is refused.
    let withdrawn = client.leave().await?;
    if withdrawn < 1 {
        return Err("leave withdrew no grant".into());
    }
    match handle.open().await {
        Err(Error::BlobRefused { .. }) => {
            println!("leave: {withdrawn} grant withdrawn; open is refused")
        }
        other => return Err(format!("expected a refusal after leave, got {other:?}").into()),
    }
    Ok(())
}

/// `--flag N`, or `default` when the flag is absent.
fn flag(args: &[String], name: &str, default: u32) -> Result<u32, Box<dyn StdError>> {
    match args.iter().position(|arg| arg == name) {
        None => Ok(default),
        Some(at) => Ok(args
            .get(at + 1)
            .ok_or_else(|| format!("{name} needs a number"))?
            .parse()?),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn StdError>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        None => {
            // Both sides here, over loopback and with no discovery.
            let holder = Node::bind_local().await?;
            let caller = Node::bind_local().await?;
            let _server = serve(&holder, 200).await?;
            let client = shelf::Client::connect(&caller, holder.endpoint().addr()).await?;
            fetch(&client, 150).await?;
            // The same holder again, for a fetcher that holds all of it: nothing
            // is missing and nothing moves, which is not an error.
            fetch(&client, 200).await?;

            // And a closure that lists nothing.
            let empty = Node::bind_local().await?;
            let _empty = serve(&empty, 0).await?;
            let client = shelf::Client::connect(&caller, empty.endpoint().addr()).await?;
            fetch(&client, 0).await?;
        }
        Some("serve") => {
            let members = flag(&args, "--members", 5000)?;
            let node = Node::bind().await?;
            let _server = serve(&node, members).await?;
            node.online().await;
            println!("serving {} ({members} members)", node.id());
            // Until stdin closes, so the process ends when its terminal does.
            let mut sink = Vec::new();
            tokio::io::stdin().read_to_end(&mut sink).await?;
            return Ok(());
        }
        Some("fetch") => {
            let holder: EndpointId = args
                .get(1)
                .ok_or("fetch needs the EndpointId `serve` printed")?
                .parse()?;
            let preseed = flag(&args, "--preseed", 4000)?;
            let node = Node::bind().await?;
            let client = shelf::Client::connect(&node, holder).await?;
            fetch(&client, preseed).await?;
        }
        Some(other) => return Err(format!("unknown mode {other}; use serve or fetch").into()),
    }
    println!("closure_share: OK");
    Ok(())
}
