//! Replayable events over an ordinary RPC, with a server-side connection cut.
//!
//! No arguments: two loopback nodes in one process, no internet needed.
//! `serve`: bind with public discovery and print this server's EndpointId.
//! `follow <EndpointId>`: run on another machine, using Node::bind and the
//! public preset's free relay/discovery service (no paid relay is configured).
//! The follower checks 1..=20 and a second handshake, then prints `event_feed: OK`.
//!
//! This intentionally serves one demo run to anyone who knows the EndpointId.
//! A real service restricts its Allowlist. For dial-back RPC without replay, see
//! noticeboard.rs; a follower here does not have to serve a callback contract.

use std::error::Error as StdError;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use keylink::events::{follow, Cursor, EventLog, Lapse, Pull, Retention};
use keylink::prelude::*;
use keylink::{
    Admission, Admittance, AdmittedPeer, Allowlist, Context, EndpointAddr, EndpointId, Node, Peer,
    ProvenPeer, Revocation, Server,
};
use n0_future::StreamExt;

type Result<T> = std::result::Result<T, Box<dyn StdError>>;

keylink::service! {
    service EventFeed;
    rpc Events(from: Cursor) -> Pull<u64>;
    rpc BurstAndDisconnect() -> std::result::Result<(), String>;
    rpc Handshakes() -> u64;
}

#[derive(Default)]
struct Gate {
    live: Mutex<Option<Revocation>>,
    handshakes: AtomicU64,
}

impl Admission for Gate {
    fn admits(&self, _peer: &ProvenPeer) -> Admittance {
        self.handshakes.fetch_add(1, Ordering::SeqCst);
        Admittance::Admitted
    }

    fn admitted(&self, _peer: &AdmittedPeer, live: Revocation) {
        *self.live.lock().unwrap_or_else(PoisonError::into_inner) = Some(live);
    }
}

struct EventFeed {
    log: EventLog<u64>,
    gate: Arc<Gate>,
}

impl event_feed::Handler for EventFeed {
    async fn events(&self, _cx: &Context, from: Cursor) -> Pull<u64> {
        self.log.pull(from).await
    }

    async fn burst_and_disconnect(&self, _cx: &Context) -> std::result::Result<(), String> {
        for value in 11..=20 {
            self.log
                .append(value)
                .await
                .map_err(|error| error.to_string())?;
        }
        // Close before the response. The caller observes the departure itself,
        // rather than trusting the handler to say the connection was dropped.
        if let Some(live) = self
            .gate
            .live
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
        {
            live.close();
        }
        Ok(())
    }

    async fn handshakes(&self, _cx: &Context) -> u64 {
        self.gate.handshakes.load(Ordering::SeqCst)
    }
}

async fn serve(node: &Node) -> Result<Server> {
    let retention = Retention::new(
        NonZeroUsize::new(100).ok_or("nonzero depth")?,
        NonZeroU64::new(1 << 20).ok_or("nonzero budget")?,
    );
    let log = EventLog::memory(retention);
    for value in 1..=10 {
        log.append(value).await?;
    }
    let gate = Arc::new(Gate::default());
    Ok(Server::builder(node)
        .service(
            event_feed::service(
                EventFeed {
                    log,
                    gate: gate.clone(),
                },
                Allowlist::anyone(),
            )
            .admission(gate)
            .build(),
        )
        .spawn()?)
}

async fn run_follower(node: &Node, address: EndpointAddr) -> Result<()> {
    let peer = Arc::new(Peer::<event_feed::Client>::new(node, address));
    let mut events = follow(peer.clone(), Cursor::ORIGIN, |client, from| async move {
        client.events(from).await
    });
    for expected in 1..=20 {
        let event = loop {
            match events.next().await.ok_or("event stream ended")? {
                Ok(event) => break event,
                Err(Lapse::Retrying { cause, after }) => {
                    eprintln!("retrying in {after:?}: {cause:?}")
                }
                Err(error) => {
                    return Err(
                        std::io::Error::other(format!("event stream failed: {error:?}")).into(),
                    )
                }
            }
        };
        if event.seq.get() != expected || event.event != expected {
            return Err(format!("expected event {expected}, received {event:?}").into());
        }
        if events.cursor() != event.resume {
            return Err("cursor advanced past delivery".into());
        }
        if expected == 10 {
            let client = peer.connect().await?;
            let departed = client.watch();
            // Do NOT use Peer::call: this mutating demo method is not retry-safe.
            // Losing its response is expected; departure is the independent proof.
            if let Ok(result) = client.burst_and_disconnect().await {
                result?;
            }
            departed.await;
        }
    }
    let handshakes = peer
        .call(|client| async move { client.handshakes().await })
        .await?;
    if handshakes < 2 {
        return Err("the follower never reconnected".into());
    }
    println!("event_feed: OK");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [] => {
            let server_node = Node::bind_local().await?;
            let client_node = Node::bind_local().await?;
            let server = serve(&server_node).await?;
            let result = run_follower(&client_node, server_node.endpoint().addr()).await;
            server.shutdown().await?;
            result
        }
        [command] if command == "serve" => {
            let node = Node::bind().await?;
            let server = serve(&node).await?;
            node.online().await;
            println!("{}", node.id());
            tokio::signal::ctrl_c().await?;
            server.shutdown().await?;
            Ok(())
        }
        [command, id] if command == "follow" => {
            let id: EndpointId = id.parse()?;
            let node = Node::bind().await?;
            node.online().await;
            run_follower(&node, id.into()).await
        }
        _ => Err("usage: event_feed [serve | follow <EndpointId>]".into()),
    }
}
