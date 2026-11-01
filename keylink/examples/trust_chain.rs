//! Delegation, resource caveats, presentation, and revocation on real connections.
//!
//! `cargo run -p keylink --example trust_chain` runs offline in one process.
//! `serve` and `connect <EndpointId>` use Node::bind's public discovery and free
//! n0 relay preset on two machines. No paid relay or external account is needed.
//!
//! The open Setup service issues demonstration grants to anyone and hands out
//! their signed revocation. It is deliberately not a production enrollment door.
use keylink::prelude::*;
use keylink::trust::{
    self, Caveats, Certificate, Chain, Change, Delegation, Pattern, Presented, Revocation, Roots,
    Scope, Timestamp, Trust, Validity, WithinCaveats,
};
use keylink::{
    Allowlist, Context, EndpointAddr, Error, Node, Refusal, RemoteError, Revocable, SecretKey,
    Server,
};
use std::borrow::Cow;
use std::error::Error as StdError;
use std::sync::atomic::{AtomicU64, Ordering};

keylink::service! {
    service Archive;
    rpc Read(name: String) -> String;
}
struct Archive;
impl archive::Handler for Archive {
    async fn read(&self, cx: &Context, name: String) -> String {
        // A handler sees verified principal authority, not just the TLS key.
        let principal = cx.credential().map(|c| c.principal());
        format!("{name}: principal {principal:?}")
    }
}
keylink::service! {
    service Setup;
    rpc Grant() -> (Vec<u8>, Vec<u8>);
    rpc Revoke(record: Vec<u8>) -> bool;
}
struct Setup {
    serial: AtomicU64,
    root: SecretKey,
    trust: Trust,
}
fn delegation(
    subject: keylink::EndpointId,
    resource: &str,
    delegate: u8,
) -> Result<Delegation, Box<dyn StdError>> {
    Ok(Delegation {
        subject,
        caveats: Caveats {
            resources: Scope::only([Pattern::under(resource)?])?,
            operations: Scope::any(),
            validity: Validity::new(
                Timestamp::from_unix_secs(0),
                Timestamp::from_unix_secs(u64::MAX),
            )?,
        },
        delegate,
        serial: 0,
    })
}
impl setup::Handler for Setup {
    async fn grant(&self, cx: &Context) -> (Vec<u8>, Vec<u8>) {
        // Fixed demo middle key, never an application identity or persisted secret.
        let user = SecretKey::from_bytes(&[42; 32]);
        let result = (|| -> Result<_, Box<dyn StdError>> {
            let mut parent = delegation(user.public(), "repo", 1)?;
            parent.serial = self.serial.fetch_add(1, Ordering::Relaxed);
            let first = Certificate::issue(&self.root, parent)?;
            let revocation = Revocation::issue(&self.root, first.id());
            let chain =
                Chain::new(vec![first]).delegate(&user, delegation(cx.peer(), "repo/a", 0)?)?;
            Ok((chain.to_bytes(), revocation.to_bytes()))
        })();
        // The protocol has no untyped failure payload: an empty chain is refused
        // by the client's decoder rather than accepted as an issued credential.
        match result {
            Ok(bundle) => bundle,
            Err(error) => {
                eprintln!("demo grant failed: {error}");
                (Vec::new(), Vec::new())
            }
        }
    }
    async fn revoke(&self, _cx: &Context, record: Vec<u8>) -> bool {
        match Revocation::from_bytes(&record) {
            Ok(revocation) => !self.trust.apply(Change::Revoke(revocation)).is_empty(),
            Err(_) => false,
        }
    }
}
fn resource(request: &archive::Request) -> Option<Cow<'_, str>> {
    match request {
        archive::Request::Read { name } => Some(Cow::Borrowed(name)),
        archive::Request::__Status {} => None,
    }
}
fn serve(node: &Node) -> Result<Server, Box<dyn StdError>> {
    let root = node.endpoint().secret_key().clone();
    let trust = Trust::new(Roots::only([root.public()])?);
    let archive = trust
        .guard::<archive::Request, _>(WithinCaveats)
        .resource(resource)
        .install(|allow| archive::service(Archive, allow))
        .build();
    Ok(Server::builder(node)
        .service(archive)
        .service(trust.presentation().build())
        .service(
            setup::service(
                Setup {
                    root,
                    trust,
                    serial: AtomicU64::new(0),
                },
                Allowlist::anyone(),
            )
            .build(),
        )
        .spawn()?)
}
async fn connect(node: &Node, to: EndpointAddr) -> Result<(), Box<dyn StdError>> {
    let setup = setup::Client::connect(node, to.clone()).await?;
    let (bytes, revocation) = setup.grant().await?;
    let chain = Chain::from_bytes(&bytes)?;
    if !matches!(
        trust::present(node, to.clone(), &chain).await?,
        Presented::Held { .. }
    ) {
        return Err("presentation refused".into());
    }
    let client = archive::Client::connect(node, to).await?;
    let _ = client.read("repo/a/file".into()).await?;
    match client.read("repo/b/file".into()).await {
        Err(Error::Remote(RemoteError::Unauthorized(method))) if method == "Read" => (),
        other => return Err(format!("outside caveat was not refused: {other:?}").into()),
    }
    let watch = client.watch();
    if !setup.revoke(revocation).await? {
        return Err("revocation closed no connection".into());
    }
    let _ = watch.await;
    match client.read("repo/a/file".into()).await {
        Err(error) if error.refusal() == Some(Refusal::Denied) => (),
        other => return Err(format!("revocation did not close the connection: {other:?}").into()),
    }
    println!("trust_chain: OK");
    Ok(())
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn StdError>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [] => {
            let server = Node::bind_local().await?;
            let client = Node::bind_local().await?;
            let _running = serve(&server)?;
            connect(&client, server.endpoint().addr()).await
        }
        [command] if command == "serve" => {
            let node = Node::bind().await?;
            let _running = serve(&node)?;
            node.online().await;
            println!("connect {}", node.id());
            tokio::signal::ctrl_c().await?;
            Ok(())
        }
        [command, id] if command == "connect" => {
            let node = Node::bind().await?;
            connect(&node, id.parse::<keylink::EndpointId>()?.into()).await
        }
        _ => Err("usage: trust_chain [serve | connect <EndpointId>]".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn enrollment_works_after_a_previous_client_revokes() -> Result<(), Box<dyn StdError>> {
        let node = Node::bind_local().await?;
        let first = Node::bind_local().await?;
        let second = Node::bind_local().await?;
        let _running = serve(&node)?;
        connect(&first, node.endpoint().addr()).await?;
        connect(&second, node.endpoint().addr()).await
    }
}
