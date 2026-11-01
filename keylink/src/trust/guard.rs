use std::borrow::Cow;
use std::future::Future;

use iroh::EndpointId;
use serde::{de::DeserializeOwned, Serialize};

use super::{Credential, Timestamp, Trust};
use crate::rpc::{Checked, Preflight};
use crate::{Allowlist, Carries, Granting, RemoteError, Rpc, ServiceBuilder};

/// One call, after certificate caveats allowed it and before dispatch.
pub struct Call<'a> {
    /// The verified principal, holder and caveats.
    pub credential: &'a Credential,
    /// Generated service name.
    pub service: &'static str,
    /// Generated method name.
    pub method: &'static str,
    /// Actor id, actor id plus extracted name, extracted name, or no resource.
    pub resource: Option<&'a str>,
}
/// An application's refusal, distinct from a policy outage.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessError {
    /// This principal may not do this.
    Forbidden,
    /// The policy could not be consulted. Nothing was decided about this caller.
    Unavailable,
}
/// Application policy after caveats and before idempotency and every layer.
/// No panic recovery is provided.
pub trait Access: Send + Sync + 'static {
    /// Authorize one call. A refusal prevents all dispatch and layer execution.
    fn authorize(&self, call: Call<'_>) -> impl Future<Output = Result<(), AccessError>> + Send;
}
/// Add no application restrictions beyond the certificate caveats.
#[derive(Debug, Clone, Copy)]
pub struct WithinCaveats;
impl Access for WithinCaveats {
    async fn authorize(&self, _call: Call<'_>) -> Result<(), AccessError> {
        Ok(())
    }
}
type Extract<Req> = fn(&Req) -> Option<Cow<'_, str>>;
/// Required admission and per-call authority for one generated request type.
pub struct Guard<Req, A> {
    trust: Trust,
    access: A,
    resource: Option<Extract<Req>>,
}
impl<Req: Rpc + 'static, A: Access> Guard<Req, A> {
    pub(super) fn new(trust: Trust, access: A) -> Self {
        Self {
            trust,
            access,
            resource: None,
        }
    }
    /// Extract a resource from a typed request. On keyed services it refines the
    /// actor id to `actor/extracted`. If extraction returns `None`, a keyed
    /// call authorizes the actor id itself; a singleton has no resource and
    /// passes only an unrestricted resource scope. This is not a missing-name
    /// rejection policy: return an explicit name when the request needs one.
    #[must_use]
    pub fn resource(mut self, extract: Extract<Req>) -> Self {
        self.resource = Some(extract);
        self
    }
    /// Build and require both this trust's admission and its per-call guard.
    /// The closure receives `Allowlist::anyone`; later builder calls cannot
    /// replace the required trust or run layers before its check.
    pub fn install<WireReq, Resp>(
        self,
        build: impl FnOnce(Allowlist) -> ServiceBuilder<WireReq, Resp>,
    ) -> ServiceBuilder<WireReq, Resp>
    where
        WireReq: Carries<Req> + DeserializeOwned + Send + Sync + 'static,
        Resp: Serialize + Granting + Send + Sync + 'static,
    {
        build(Allowlist::anyone())
            .require_admission(self.trust.clone())
            .require(self)
    }
}
impl<Req: Rpc + 'static, W: Carries<Req> + Sync, A: Access> Preflight<W> for Guard<Req, A> {
    fn check<'a>(&'a self, peer: EndpointId, request: &'a W) -> Checked<'a> {
        Box::pin(async move {
            let extracted = self.resource.and_then(|extract| extract(request.carried()));
            let resource = match (request.actor(), extracted) {
                (Some(actor), Some(name)) => Some(Cow::Owned(format!("{}/{name}", actor.as_str()))),
                (Some(actor), None) => Some(Cow::Borrowed(actor.as_str())),
                (None, name) => name,
            };
            let method = request.method();
            let operation = format!("{}/{method}", Req::SERVICE);
            let credential = self
                .trust
                .permitting(peer, Timestamp::now(), resource.as_deref(), &operation)
                .ok_or_else(|| RemoteError::Unauthorized(method.to_owned()))?;
            self.access
                .authorize(Call {
                    credential: &credential,
                    service: Req::SERVICE,
                    method,
                    resource: resource.as_deref(),
                })
                .await
                .map_err(|error| match error {
                    AccessError::Forbidden => RemoteError::Unauthorized(method.to_owned()),
                    AccessError::Unavailable => RemoteError::AccessUnavailable,
                })?;
            self.recheck(peer, request, Some(&credential))?;
            Ok(Some(credential))
        })
    }
    fn recheck(
        &self,
        _peer: EndpointId,
        request: &W,
        credential: Option<&Credential>,
    ) -> Result<(), RemoteError> {
        if credential.is_some_and(|c| self.trust.current(c, Timestamp::now())) {
            Ok(())
        } else {
            Err(RemoteError::Unauthorized(request.method().to_owned()))
        }
    }
}
