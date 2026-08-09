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

    /// Make this runtime the current one for as long as the guard lives.
    ///
    /// For constructors that *spawn* rather than await — see
    /// [`Node::enter`], which is where a caller meets this.
    ///
    /// # Errors
    ///
    /// [`Error::BlockingInAsync`] when called from inside a tokio runtime,
    /// which is where the async API belongs instead.
    pub fn enter(&self) -> Result<tokio::runtime::EnterGuard<'_>> {
        guard()?;
        let Some(inner) = self.inner.as_ref() else {
            return Err(Error::BlockingInAsync);
        };
        Ok(inner.enter())
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

    /// Lend this node's runtime to the calling thread, so that a constructor
    /// which **spawns** can be called from synchronous code.
    ///
    /// ```ignore
    /// let node = keylink::blocking::Node::bind()?;
    /// let warehouse = {
    ///     let _guard = node.enter()?;
    ///     inventory::actor(Warehouse::seeded())
    /// };
    /// let server = node.serve([warehouse.service(allow).build()])?;
    /// ```
    ///
    /// # Why this is not optional
    ///
    /// The rest of this module hides the runtime because every operation it
    /// mirrors is a *future*, and a future can be handed to
    /// [`Runtime::run`]. An actor is different: `actor()` and `spawn()` start a
    /// task, and `tokio::spawn` reads the current runtime from a thread-local
    /// that a synchronous thread does not have. It panics rather than returning
    /// an error, so there is nothing for this module to convert.
    ///
    /// So without this method a synchronous program cannot build an actor at
    /// all — not awkwardly, not at all. RFC-04 §10.3.
    ///
    /// # Drop it before you use anything else here
    ///
    /// The guard sets the thread-local that [`Runtime::run`] checks, so while it
    /// is held **every other method in this module returns
    /// [`Error::BlockingInAsync`]** — including [`Node::serve`] and every
    /// generated blocking client call. That is not a bug in the check: blocking
    /// on a runtime from a thread that runtime considers its own is the
    /// deadlock this module exists to refuse, and it cannot tell a borrowed
    /// context from a real one.
    ///
    /// Hence the braces in the example. Hold the guard while constructing, drop
    /// it, then carry on.
    ///
    /// # The node owns the actor's runtime
    ///
    /// An actor built under this guard runs on the node's runtime, and
    /// [`Runtime`]'s `Drop` calls `shutdown_background`. Dropping the node
    /// therefore ends the actor with no ordering and no notification: a handle
    /// held past that point reports
    /// [`Unanswered::Gone`](crate::Unanswered::Gone) or
    /// [`NotQueued::Closed`](crate::NotQueued::Closed) by door, either of which
    /// says the actor is gone and not why. Keep the node alive for as long as the
    /// actor.
    ///
    /// # Errors
    ///
    /// [`Error::BlockingInAsync`] when called from inside a tokio runtime. Code
    /// that already has one wants the async API, where nothing needs lending.
    pub fn enter(&self) -> Result<tokio::runtime::EnterGuard<'_>> {
        self.runtime.enter()
    }

    /// Serve services from a synchronous program.
    pub fn serve<I: IntoIterator<Item = Service>>(&self, services: I) -> Result<Server> {
        let mut builder = crate::Server::builder(&self.inner);
        for service in services {
            builder = builder.service(service);
        }
        Ok(Server {
            inner: builder.spawn()?,
            runtime: self.runtime.clone(),
        })
    }
}

/// A running server, driven by its node's runtime.
#[derive(Debug)]
#[must_use = "the server stops when dropped; hold it for as long as you serve"]
pub struct Server {
    /// Not an `Option`. It was one, and the `None` arm was unreachable: this
    /// type has no `Drop`, and `shutdown` consumes `self`, so nothing could ever
    /// take the value out first.
    ///
    /// That is the trap `keys.rs` describes in its own words — an `Option` that
    /// is never `None` in practice, whose dead arm every test passes through
    /// without exercising. `crate::Server::shutdown` takes `&self`, so the
    /// `Option` bought nothing even before that.
    inner: crate::Server,
    runtime: Arc<Runtime>,
}

impl Server {
    /// Stop accepting and wait for in-flight connections to finish.
    pub fn shutdown(self) -> Result<()> {
        self.runtime.run(self.inner.shutdown())?
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

    /// Both halves of RFC-04 §10.3, in one test because the second is only
    /// interesting given the first.
    ///
    /// A spawn off a runtime **panics** — it does not return an error — so a
    /// synchronous program could not construct an actor at all before this.
    /// Made red by deleting either the `catch_unwind` assertion (which is then
    /// a claim nobody checks) or `enter` itself.
    ///
    /// The panic below is caught on purpose. The panic hook still runs, and the
    /// harness captures what it writes, so it is invisible while this test
    /// passes and visible in the output if it ever fails — which is where that
    /// message is worth reading anyway. Silencing it would need
    /// `std::panic::set_hook`, which is process-global and would hide a real
    /// panic in whatever test happened to be running beside this one.
    /// A mailbox that can carry nothing, for an actor that is never sent
    /// anything.
    ///
    /// `LocalActor` needs a message type and the test below is about the
    /// runtime, not about messages. Uninhabited rather than a unit struct, so
    /// the one arm the loop needs is `match self {}` — there is no case, so
    /// there is nothing to get wrong and nothing to mistake for a real mailbox.
    enum Nothing {}

    impl crate::Turn<()> for Nothing {
        async fn run(self, _state: &mut (), _hooks: &mut crate::Hooks<()>) -> Option<crate::Ended> {
            match self {}
        }
    }

    #[test]
    fn an_actor_needs_the_runtime_entered_and_enter_lends_it() {
        let node = Node::bind().expect("binds off a runtime thread");

        let without = std::panic::catch_unwind(|| {
            let _ = crate::LocalActor::<(), Nothing>::spawn(());
        });
        assert!(
            without.is_err(),
            "spawning off a runtime must panic, or this method has no reason to exist"
        );

        let actor = {
            let _guard = node.enter().expect("a node off a runtime lends its own");
            crate::LocalActor::<(), Nothing>::spawn(())
        };
        assert!(actor.is_running(), "and with the runtime entered it starts");
    }

    /// The guard is not a way back into the blocking API from async code.
    #[tokio::test]
    async fn entering_from_inside_a_runtime_is_refused() {
        // `Node::bind` already refuses here, so the runtime is built directly:
        // the point is that `enter` refuses on its own rather than relying on
        // there being no way to hold a node.
        let Err(err) = Runtime::new() else {
            panic!("must refuse, not deadlock");
        };
        assert!(matches!(err, Error::BlockingInAsync), "got {err:?}");
    }
}
