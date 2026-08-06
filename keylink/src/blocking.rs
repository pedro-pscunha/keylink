//! keylink for programs that are not async.
//!
//! Every type here mirrors its async twin, method for method, so a synchronous
//! program never writes runtime plumbing:
//!
//! ```ignore
//! fn main() -> keylink::Result<()> {
//!     let node = keylink::blocking::Node::bind()?;
//!     let client = inventory::blocking::Client::connect(&node, peer)?;
//!     println!("{:?}", client.get_item("ADU-014".to_string())?);
//!     client.close()
//! }
//! ```
//!
//! # Who owns the runtime
//!
//! A [`Node`] owns a private tokio runtime and shares it with every client and
//! server built from it. Asking the caller for a runtime would put back exactly
//! the plumbing this module exists to remove, and a global runtime would be a
//! process-wide resource nobody can shut down. The cost is that two blocking
//! nodes cost two runtimes — the right trade for a program that has one node.
//!
//! The runtime is **multi-threaded on purpose**. iroh keeps background work
//! alive between calls — path probes, relay keepalives, the accept loop — and a
//! current-thread runtime only advances tasks while a `block_on` is in
//! progress, so a connection would stall between calls.
//!
//! # Calling this from async code
//!
//! Blocking on a future from inside a runtime parks a worker thread on work
//! that same runtime has to perform. Every entry point here checks first and
//! returns [`Error::BlockingInAsync`] rather than deadlocking.
//!
//! # What is not mirrored
//!
//! Streaming methods (`send` and `recv` in `service!`) stay async-only. Their
//! bodies are `AsyncRead`/`AsyncWrite` by nature, and a synchronous wrapper
//! would either buffer a whole file in memory or hand back a handle that only
//! works inside a runtime. Use the async API for those.

use std::future::Future;
use std::sync::Arc;

use iroh::{EndpointAddr, EndpointId, SecretKey};

use crate::{Error, Result, Service};

/// The tokio runtime that drives blocking calls.
///
/// Created by [`Node::bind`] and shared by every client and server derived from
/// that node.
#[derive(Debug)]
pub struct Runtime {
    /// `Option` only so `Drop` can take ownership; see the `Drop` impl.
    inner: Option<tokio::runtime::Runtime>,
}

impl Runtime {
    fn new() -> Result<Self> {
        guard()?;
        let inner = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .thread_name("keylink-blocking")
            .build()?;
        Ok(Self { inner: Some(inner) })
    }

    /// Drive one future to completion on this runtime.
    ///
    /// Generated blocking clients call this and nothing else, so the
    /// async-context check lives in exactly one place.
    ///
    /// # Errors
    ///
    /// [`Error::BlockingInAsync`] when called from inside a tokio runtime.
    pub fn run<F: Future>(&self, fut: F) -> Result<F::Output> {
        guard()?;
        let Some(inner) = self.inner.as_ref() else {
            return Err(Error::BlockingInAsync);
        };
        Ok(inner.block_on(fut))
    }
}

impl Drop for Runtime {
    /// `Runtime::drop` blocks until tasks finish, and panics outright when it
    /// runs inside another runtime. Detaching instead means dropping a blocking
    /// client on an async thread costs a leaked shutdown, not a crash.
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            inner.shutdown_background();
        }
    }
}

/// Refuse to block a thread that a runtime is already using.
fn guard() -> Result<()> {
    if tokio::runtime::Handle::try_current().is_ok() {
        return Err(Error::BlockingInAsync);
    }
    Ok(())
}

/// A bound endpoint plus the runtime that drives it.
#[derive(Debug)]
pub struct Node {
    inner: crate::Node,
    runtime: Arc<Runtime>,
}

impl Node {
    /// Bind with a fresh random identity.
    pub fn bind() -> Result<Self> {
        let runtime = Arc::new(Runtime::new()?);
        let inner = runtime.run(crate::Node::bind())??;
        Ok(Self { inner, runtime })
    }

    /// Bind with a fixed identity, so the id peers dial survives restarts.
    pub fn bind_with_secret(secret: SecretKey) -> Result<Self> {
        let runtime = Arc::new(Runtime::new()?);
        let inner = runtime.run(crate::Node::bind_with_secret(secret))??;
        Ok(Self { inner, runtime })
    }

    /// Bind using a 64-hex secret from an environment variable.
    ///
    /// Gated on `env` as well as `blocking`: the synchronous API does not need
    /// a process environment, only this one method does.
    #[cfg(feature = "env")]
    pub fn bind_from_env(var: &str) -> Result<Self> {
        let runtime = Arc::new(Runtime::new()?);
        let inner = runtime.run(crate::Node::bind_from_env(var))??;
        Ok(Self { inner, runtime })
    }

    /// Wait until the endpoint has a reachable path.
    pub fn online(&self) -> Result<()> {
        self.runtime.run(async {
            self.inner.online().await;
        })
    }

    /// This endpoint's public key — the address peers dial.
    pub fn id(&self) -> EndpointId {
        self.inner.id()
    }

    /// The async node underneath, for anything this module does not mirror.
    pub fn inner(&self) -> &crate::Node {
        &self.inner
    }

    /// The shared runtime. Generated blocking clients need it.
    pub fn runtime(&self) -> &Arc<Runtime> {
        &self.runtime
    }

    /// Serve services from a synchronous program.
    pub fn serve<I: IntoIterator<Item = Service>>(&self, services: I) -> Result<Server> {
        let mut builder = crate::Server::builder(&self.inner);
        for service in services {
            builder = builder.service(service);
        }
        Ok(Server {
            inner: Some(builder.spawn()?),
            runtime: self.runtime.clone(),
        })
    }
}

/// A running server, driven by its node's runtime.
#[derive(Debug)]
#[must_use = "the server stops when dropped; hold it for as long as you serve"]
pub struct Server {
    inner: Option<crate::Server>,
    runtime: Arc<Runtime>,
}

impl Server {
    /// Stop accepting and wait for in-flight connections to finish.
    pub fn shutdown(mut self) -> Result<()> {
        let Some(inner) = self.inner.take() else {
            return Ok(());
        };
        self.runtime.run(inner.shutdown())?
    }

    /// Park the calling thread until the process is interrupted.
    ///
    /// The obvious synchronous "serve forever", without asking the caller to
    /// know that a server stops when it is dropped.
    pub fn wait_for_ctrl_c(&self) -> Result<()> {
        self.runtime.run(async {
            let _ = tokio::signal::ctrl_c().await;
        })
    }
}

/// Dial a peer from a synchronous program. Generated code calls this.
#[doc(hidden)]
pub fn connect_with<F, Fut, T>(node: &Node, peer: impl Into<EndpointAddr>, dial: F) -> Result<T>
where
    F: FnOnce(crate::Node, EndpointAddr) -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let addr = peer.into();
    let inner = node.inner.clone();
    node.runtime.run(dial(inner, addr))?
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use super::*;

    /// Blocking on a runtime from inside that runtime parks a worker on work it
    /// has to perform. It must be an error, not a hang.
    #[tokio::test]
    async fn blocking_from_inside_a_runtime_is_refused() {
        let Err(err) = Node::bind() else {
            panic!("must refuse, not deadlock");
        };
        assert!(
            matches!(err, Error::BlockingInAsync),
            "expected BlockingInAsync, got {err:?}"
        );
    }

    #[test]
    fn a_blocking_node_binds_and_reports_its_id() {
        let node = Node::bind().expect("binds off a runtime thread");
        assert_ne!(node.id().to_string(), "", "an id is available");
    }
}
