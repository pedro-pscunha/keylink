//! Authorization.
//!
//! iroh proves *who* a peer is during the handshake. It says nothing about
//! whether that peer is welcome. This module is the second half.

use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;
use std::str::FromStr;

use iroh::endpoint::{Connection, HandshakeCompleted};
use iroh::EndpointId;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::rpc::{Checked, Preflight};
use crate::{Carries, ConfigError, Granting, RemoteError, Result, Rpc, ServiceBuilder};

/// A peer iroh proved during a TLS handshake.
///
/// **Identity, and nothing else.** This says the key on the other end of the
/// connection is genuinely that key. It says nothing at all about whether we
/// want to talk to it — that is [`Allowlist::admit`], and the type it returns.
///
/// # The trap this closes
///
/// The one constructor takes `&Connection<HandshakeCompleted>`. iroh's
/// `remote_id()` is infallible only on that state; on both zero-RTT states it
/// returns a `Result`, and on `Connecting` there is *also* an infallible
/// `remote_id()` — which reports **the peer we are dialling**, our own intent
/// rather than anything proven. A constructor over `&Connecting` would compile
/// and would prove nothing.
///
/// # What it deletes, and what it does not, stated plainly
///
/// It deletes exactly one check, and that check lived in a design rather than
/// in this crate. RFC-01 §5.5 keyed blobs admission on the
/// `Option<EndpointId>` iroh-blobs reports, opening its policy loop with a
/// `let Some(peer) = .. else { refuse }`. That arm is unreachable through
/// `BlobsProtocol` today — both construction sites fill the field in — so a
/// handler that wrote `_ => Ok(())` instead would have passed every test
/// anybody could write, and would have begun admitting unidentified peers the
/// day a transport reported none. keylink takes the identity from the
/// connection it accepted itself, so there is no `Option` and no arm to get
/// wrong.
///
/// It deletes **no check in keylink as it stands**. `HandshakeCompleted` is the
/// default type parameter, so every bare `Connection` in this crate already is
/// a `Connection<HandshakeCompleted>`. What naming the state buys is
/// prospective: a future zero-RTT accept path cannot reach an authorization
/// decision by accident. Worth having for a security type, and not a guard
/// removed today.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ProvenPeer(EndpointId);

impl ProvenPeer {
    /// Take the identity iroh proved on this connection.
    ///
    /// Public because generated accept code names it. A caller receives proven
    /// peers; it does not usually need to mint one.
    #[doc(hidden)]
    #[must_use]
    pub fn on(connection: &Connection<HandshakeCompleted>) -> Self {
        Self(connection.remote_id())
    }

    /// The proven key, for a log line or a map somebody else owns.
    #[must_use]
    pub fn id(&self) -> EndpointId {
        self.0
    }

    /// A deterministic identity for unit tests, which have no live connection.
    ///
    /// `#[cfg(test)]`, so it does not exist in a built library and cannot
    /// become the second constructor this type is shaped to refuse. It is
    /// named for what it is rather than for anything a caller would reach for
    /// by accident, and `representability.rs` covers the names that would.
    #[cfg(test)]
    pub(crate) fn for_tests(seed: u8) -> Self {
        Self(iroh::SecretKey::from_bytes(&[seed; 32]).public())
    }

    /// The same, for tests that need more than 256 distinct peers.
    #[cfg(test)]
    pub(crate) fn for_tests_wide(seed: u64) -> Self {
        let mut bytes = [7u8; 32];
        bytes[..8].copy_from_slice(&seed.to_le_bytes());
        Self(iroh::SecretKey::from_bytes(&bytes).public())
    }
}

/// A [`ProvenPeer`] that an [`Allowlist`] admitted.
///
/// **Level 2, and the invariant is admission rather than identity.** The two
/// are different questions and were previously answered by the same value: a
/// `bool` from `permits` that a caller had to remember to consult, beside an
/// `EndpointId` that carried no trace of the answer. A reader meeting a peer id
/// three functions later could not tell whether anything had said yes.
///
/// **The runtime check this deletes.** Every function downstream of an accept
/// used to be free to take a bare id, so each one either re-asked the allowlist
/// or trusted that somebody had. Taking an `AdmittedPeer` is the proof, so the
/// question is asked once, at the accept, in the place the type system
/// remembers.
///
/// The only constructor is [`Allowlist::admit`], which is why this type is
/// worth more than a comment: writing an authorization record now requires a
/// value that could not exist unless a policy produced it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AdmittedPeer(ProvenPeer);

impl AdmittedPeer {
    /// Mint one, for the accept path, one line after a policy said yes.
    ///
    /// `pub(crate)`, so [`Allowlist::admit`] remains the only constructor
    /// *reachable from outside this crate* — which is the invariant that
    /// matters. An [`Admission`](crate::Admission) implemented downstream
    /// answers [`Admittance::Admitted`](crate::Admittance::Admitted), which carries no
    /// value; keylink turns that into the proof here. Making the trait return
    /// an `AdmittedPeer` instead would have made the trait unimplementable
    /// outside the crate, or the type mintable inside anyone's.
    pub(crate) fn mint(peer: ProvenPeer) -> Self {
        Self(peer)
    }

    /// The identity underneath, for a ledger keyed on who rather than on
    /// whether.
    #[must_use]
    pub fn proven(&self) -> ProvenPeer {
        self.0
    }

    /// The proven key.
    #[must_use]
    pub fn id(&self) -> EndpointId {
        self.0.id()
    }
}

/// Who may talk to a served protocol.
///
/// There is deliberately no `Default`. Admitting everyone is a decision you
/// have to type out, so it can never be what you get by forgetting.
#[derive(Debug, Clone)]
pub enum Allowlist {
    /// Only these proven keys.
    Only(BTreeSet<EndpointId>),
    /// Anyone who can reach us. Sensible for a public service, catastrophic by accident.
    Anyone,
}

impl Allowlist {
    /// Admit exactly the given keys.
    pub fn only<I: IntoIterator<Item = EndpointId>>(ids: I) -> Self {
        Self::Only(ids.into_iter().collect())
    }

    /// Admit any peer that can reach us. Typed out on purpose.
    pub fn anyone() -> Self {
        Self::Anyone
    }

    /// Parse a comma-separated list of 64-hex endpoint ids.
    ///
    /// Errors on a malformed entry rather than skipping it: a typo that silently
    /// shrinks your allowlist is the kind of bug you find during an incident.
    ///
    /// # Errors
    ///
    /// [`ConfigError::MalformedEndpointId`] for an unparseable entry, and
    /// [`ConfigError::EmptyAllowlist`] when nothing at all parsed.
    pub fn parse(raw: &str) -> Result<Self> {
        let mut ids = BTreeSet::new();
        for entry in raw.split(',').map(str::trim).filter(|e| !e.is_empty()) {
            let id =
                EndpointId::from_str(entry).map_err(|source| ConfigError::MalformedEndpointId {
                    entry: entry.to_owned(),
                    source,
                })?;
            ids.insert(id);
        }
        if ids.is_empty() {
            return Err(ConfigError::EmptyAllowlist.into());
        }
        Ok(Self::Only(ids))
    }

    /// Read the allowlist from an environment variable.
    ///
    /// Fails at startup when unset, rather than admitting nobody at runtime and
    /// leaving you to work out why every peer is bouncing. A missing value must
    /// never be the thing that opens *or* silently closes the door.
    ///
    /// # Errors
    ///
    /// [`ConfigError::MissingEnvVar`] when unset, plus the errors of
    /// [`Self::parse`].
    #[cfg(feature = "env")]
    pub fn from_env(var: &str) -> Result<Self> {
        let raw = std::env::var(var).map_err(|_| ConfigError::MissingEnvVar {
            var: var.to_owned(),
        })?;
        Self::parse(&raw)
    }

    /// Decide about a proven peer, and hand back the proof of the decision.
    ///
    /// Replaces a `permits(&EndpointId) -> bool`. The `bool` was the problem:
    /// it could be computed and dropped, the id it was computed about carried
    /// no trace of the answer, and every function downstream took that bare id.
    /// An `Option<AdmittedPeer>` cannot be ignored into an admission, and the
    /// value it yields is the only way to reach anything that demands one.
    #[must_use]
    pub fn admit(&self, peer: ProvenPeer) -> Option<AdmittedPeer> {
        match self {
            Self::Anyone => Some(AdmittedPeer(peer)),
            Self::Only(ids) if ids.contains(&peer.id()) => Some(AdmittedPeer(peer)),
            Self::Only(_) => None,
        }
    }
}

/// Data-driven peer and method authorization rules.
///
/// Each TOML table names one peer and the methods that peer can call:
///
/// ```toml
/// [[peer]]
/// id = "a09aa5f47a6759802ff955f8dc2d2a14a5c99d23be97f864127ff9383455a4f0"
/// allow = ["GetItem", "Reserve"]
/// ```
///
/// Parse once, then [`compile`](Self::compile) against the generated request type.
/// Compilation rejects every method absent from that binary's [`Rpc::METHODS`].
/// A peer may appear once. Repeating a method name inside that peer's `allow`
/// array is harmless and denotes the same permission once.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rules {
    peer: Vec<PeerRules>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PeerRules {
    id: String,
    allow: Vec<String>,
}

impl Rules {
    /// Parse one `[[peer]]` table per peer.
    ///
    /// ```toml
    /// [[peer]]
    /// id = "a09aa5f47a6759802ff955f8dc2d2a14a5c99d23be97f864127ff9383455a4f0"
    /// allow = ["GetItem", "Reserve"]
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::MalformedRules`] when the input is not valid TOML
    /// with an `id` and `allow` list for each peer.
    pub fn parse(raw: &str) -> Result<Self> {
        toml::from_str(raw).map_err(|source| ConfigError::MalformedRules { error: source }.into())
    }

    /// Validate the `id` and `allow` fields in each `[[peer]]` table against `Req`.
    ///
    /// The result can be installed only on `Req` or `Addressed<Req>`. For
    /// example, `allow = ["GetItem", "Reserve"]` must name methods in
    /// `Req::METHODS`.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::MalformedEndpointId`] for a bad peer id and
    /// [`ConfigError::UnknownMethod`] for the first name absent from
    /// [`Rpc::METHODS`].
    pub fn compile<Req: Rpc>(self) -> Result<Compiled<Req>> {
        if self.peer.is_empty() {
            return Err(ConfigError::EmptyRules.into());
        }
        let mut allowed = BTreeMap::<EndpointId, BTreeSet<&'static str>>::new();
        for peer in self.peer {
            let id = EndpointId::from_str(&peer.id).map_err(|source| {
                ConfigError::MalformedEndpointId {
                    entry: peer.id.clone(),
                    source,
                }
            })?;
            if allowed.contains_key(&id) {
                return Err(ConfigError::DuplicatePeer { id }.into());
            }
            if peer.allow.is_empty() {
                return Err(ConfigError::EmptyPeerMethods { id }.into());
            }
            let mut methods = BTreeSet::new();
            for named in peer.allow {
                let Some(method) = Req::METHODS.iter().copied().find(|method| *method == named)
                else {
                    return Err(ConfigError::UnknownMethod {
                        service: Req::SERVICE,
                        named,
                    }
                    .into());
                };
                methods.insert(method);
            }
            allowed.insert(id, methods);
        }
        let allow = Allowlist::only(allowed.keys().copied());
        Ok(Compiled {
            allow,
            authorization: Authorization {
                allowed,
                request: PhantomData,
            },
        })
    }
}

/// Peer admission plus method authorization compiled for one generated request type.
///
/// The source uses `[[peer]]` tables with an endpoint `id` and an `allow` array
/// of generated method names. The policies have no independent projection.
/// [`install`](Self::install) gives the validated allowlist to the generated
/// builder closure and installs the matching preflight itself.
#[derive(Debug)]
pub struct Compiled<Req> {
    allow: Allowlist,
    authorization: Authorization<Req>,
}

impl<Req: Rpc + 'static> Compiled<Req> {
    /// Create a generated service builder with required admission and authorization.
    ///
    /// `WireReq` must be `Req` or `Addressed<Req>`. This bound keeps rules
    /// compiled for one generated request type off every unrelated service.
    ///
    /// ```no_run
    /// use keylink::{Allowlist, Context, Rules};
    ///
    /// keylink::service! {
    ///     service Inventory;
    ///     rpc GetItem(id: u64) -> Option<u64>;
    /// }
    ///
    /// struct Inventory;
    ///
    /// impl inventory::Handler for Inventory {
    ///     async fn get_item(&self, _cx: &Context, id: u64) -> Option<u64> {
    ///         Some(id)
    ///     }
    /// }
    ///
    /// let raw = r#"
    /// [[peer]]
    /// id = "a09aa5f47a6759802ff955f8dc2d2a14a5c99d23be97f864127ff9383455a4f0"
    /// allow = ["GetItem"]
    /// "#;
    /// let compiled = Rules::parse(raw)?.compile::<inventory::Request>()?;
    /// let service = compiled
    ///     .install(|allow| inventory::service(Inventory, allow))
    ///     .build();
    /// # let _ = service;
    /// # Ok::<(), keylink::Error>(())
    /// ```
    pub fn install<WireReq, Resp>(
        self,
        build: impl FnOnce(Allowlist) -> ServiceBuilder<WireReq, Resp>,
    ) -> ServiceBuilder<WireReq, Resp>
    where
        WireReq: Carries<Req> + DeserializeOwned + Send + Sync + 'static,
        Resp: Serialize + Granting + Send + Sync + 'static,
    {
        build(self.allow.clone())
            .require_admission(self.allow)
            .require(self.authorization)
    }
}

/// The method policy retained by [`Compiled`] for one generated request type.
///
/// Its fields and constructor are private. Required preflight runs before
/// idempotency and every layer, for direct and addressed requests.
#[doc(hidden)]
#[derive(Debug)]
pub struct Authorization<Req> {
    allowed: BTreeMap<EndpointId, BTreeSet<&'static str>>,
    request: PhantomData<fn() -> Req>,
}

impl<Req> Authorization<Req> {
    fn authorizes(&self, peer: EndpointId, method: &'static str) -> bool {
        self.allowed
            .get(&peer)
            .is_some_and(|methods| methods.contains(method))
    }
}

impl<Req: Rpc + 'static, W: Carries<Req>> Preflight<W> for Authorization<Req> {
    fn check<'a>(&'a self, peer: EndpointId, request: &'a W) -> Checked<'a> {
        let result = if self.authorizes(peer, request.method()) {
            Ok(None)
        } else {
            Err(RemoteError::Unauthorized(request.method().to_owned()))
        };
        Box::pin(std::future::ready(result))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::Error;

    const A: &str = "a09aa5f47a6759802ff955f8dc2d2a14a5c99d23be97f864127ff9383455a4f0";
    const B: &str = "d04ab232742bb4ab3a1368bd4615e4e6d0224ab71a016baf8520a332c9778737";

    /// `ProvenPeer::on` needs a live connection, and these tests need neither.
    /// The ladder's point is that `AdmittedPeer` has one source; nothing says
    /// `ProvenPeer` must be hard to build inside the module that defines it.
    fn proven(hex: &str) -> ProvenPeer {
        ProvenPeer(EndpointId::from_str(hex).unwrap())
    }

    #[test]
    fn parses_and_admits_only_listed() {
        let list = Allowlist::parse(&format!(" {A} , ")).unwrap();
        assert!(list.admit(proven(A)).is_some());
        assert!(list.admit(proven(B)).is_none());
    }

    #[test]
    fn an_admitted_peer_keeps_the_identity_it_was_admitted_for() {
        let admitted = Allowlist::anyone().admit(proven(A)).unwrap();
        assert_eq!(admitted.id(), EndpointId::from_str(A).unwrap());
        assert_eq!(admitted.proven(), proven(A));
    }

    #[test]
    fn rejects_malformed_entry_instead_of_skipping() {
        let err = Allowlist::parse(&format!("{A},notahexid")).unwrap_err();
        assert!(
            matches!(err, Error::Config(ConfigError::MalformedEndpointId { .. })),
            "got {err:?}"
        );
    }

    #[test]
    fn empty_is_an_error_not_a_deny_all() {
        let err = Allowlist::parse("  ,  ").unwrap_err();
        assert!(
            matches!(err, Error::Config(ConfigError::EmptyAllowlist)),
            "got {err:?}"
        );
    }

    /// Carries the same gate as the function it calls. Without this,
    /// `cargo test -p keylink --no-default-features` does not compile — and
    /// nothing noticed, because the feature-subset task checked `--lib`, which
    /// does not build test code.
    #[cfg(feature = "env")]
    #[test]
    fn unset_env_fails_closed() {
        assert!(Allowlist::from_env("KEYLINK_DEFINITELY_UNSET_VAR").is_err());
    }
}
