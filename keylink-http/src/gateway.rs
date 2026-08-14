//! Ordered state-generic Axum routing.

use std::{error::Error as _, marker::PhantomData, sync::Arc};

use axum::{body::Body, routing::any, Router};
use http::{header, HeaderMap, Request, Response};
use keylink::ExternalService;

use crate::{
    __contract::{Route, RouteKind},
    error::GatewayError,
    service::JsonCodec,
    Access, AccessError, Addressing, Call, Challenge, ConfigError, HttpSurface, JsonLimits,
    Principal,
};

const IDEMPOTENCY_HEADER: &str = "idempotency-key";
const KEY_DOMAIN: &[u8] = b"keylink-http/idempotency/v2\0";

/// HTTP/JSON adapter for one already-connected generated singleton client.
pub struct Gateway<S, A>
where
    S: ExternalService,
    A: Access,
{
    client: S::Client,
    access: A,
    challenge: Challenge,
    limits: JsonLimits,
    service: PhantomData<fn() -> S>,
}

impl<S, A> Gateway<S, A>
where
    S: ExternalService,
    A: Access,
{
    /// Build a gateway with mandatory access and a validated cached challenge.
    #[must_use]
    pub fn new(client: S::Client, access: A, challenge: Challenge) -> Self {
        Self {
            client,
            access,
            challenge,
            limits: JsonLimits::default(),
            service: PhantomData,
        }
    }

    /// Replace the finite inclusive JSON byte ceilings.
    #[must_use]
    pub fn limits(mut self, limits: JsonLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Build the configured absolute routes for merging into an application router.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::WrongAddressing`] when this singleton gateway receives
    /// an actor surface, or [`ConfigError::MissingRuntimeRoute`] when the selected
    /// surface has no implemented singleton RPC route.
    pub fn router<State>(self, surface: HttpSurface) -> Result<Router<State>, ConfigError>
    where
        State: Clone + Send + Sync + 'static,
    {
        if surface.addressing() != Addressing::Singleton {
            return Err(ConfigError::WrongAddressing);
        }
        install(self, surface)
    }
}

/// Reaching one handler without a router in front of it.
///
/// Test-only, and deliberately not a production seam: an in-process unit test
/// drives a single handler with a hand-built request, and going through
/// [`Router`] would put path matching and extractor machinery between the test
/// and what it is measuring. Each is the same two lines the router runs, so no
/// handler body is duplicated here.
#[cfg(test)]
impl<S, A> Gateway<S, A>
where
    S: ExternalService + Send + Sync + 'static,
    A: Access,
{
    /// The method the router would have selected, or the refusal it would have
    /// answered.
    ///
    /// Returns the refusal rather than panicking, because several unit tests
    /// drive a handler with a method nobody serves and expect the `404` — the
    /// wrapper has to behave as `addressed` does or it is measuring a different
    /// gateway.
    fn only_method(
        &self,
        path: &str,
        route_path: &str,
        kind: keylink::ExternalKind,
    ) -> Result<&'static str, GatewayError> {
        let decoded = decode_method_in(S::METHODS, path, route_path, kind)?;
        S::METHODS
            .iter()
            .copied()
            .find(|candidate| candidate.name() == decoded && candidate.kind() == kind)
            .map(keylink::ExternalMethod::name)
            .ok_or(GatewayError::MethodNotFound)
    }

    #[expect(
        clippy::expect_used,
        reason = "test-only, and a singleton surface decodes nothing it could refuse"
    )]
    fn only(&self) -> Surface<'_, A> {
        self.surface("", "")
            .expect("a singleton surface always builds")
    }

    async fn handle_rpc(
        &self,
        request: Request<Body>,
        route: &'static Route,
        route_path: &str,
    ) -> Response<Body> {
        let method =
            match self.only_method(request.uri().path(), route_path, keylink::ExternalKind::Rpc) {
                Ok(method) => method,
                Err(error) => return error.response(None),
            };
        handle_rpc(request, route, method, &self.only()).await
    }

    async fn handle_send(
        &self,
        request: Request<Body>,
        route: &'static Route,
        route_path: &str,
        limits: crate::StreamingLimits,
    ) -> Response<Body> {
        let method = match self.only_method(
            request.uri().path(),
            route_path,
            keylink::ExternalKind::Send,
        ) {
            Ok(method) => method,
            Err(error) => return error.response(None),
        };
        handle_send(request, route, method, limits, &self.only()).await
    }

    async fn handle_recv(
        &self,
        request: Request<Body>,
        route: &'static Route,
        route_path: &str,
        limits: crate::StreamingLimits,
    ) -> Response<Body> {
        let method = match self.only_method(
            request.uri().path(),
            route_path,
            keylink::ExternalKind::Recv,
        ) {
            Ok(method) => method,
            Err(error) => return error.response(None),
        };
        handle_recv(request, route, method, limits, &self.only()).await
    }

    async fn handle_blob_lookup(
        &self,
        request: Request<Body>,
        route: &'static Route,
        route_path: &str,
        tickets: &crate::blob::Tickets<keylink::BlobHandle>,
    ) -> Response<Body> {
        let method = match self.only_method(
            request.uri().path(),
            route_path,
            keylink::ExternalKind::Blob,
        ) {
            Ok(method) => method,
            Err(error) => return error.response(None),
        };
        handle_blob_lookup(request, route, method, tickets, &self.only()).await
    }
}

/// A gateway onto one node's KEYED actors.
///
/// A separate type rather than a mode of [`Gateway`], and not by preference:
/// `Gateway` holds `S::Client` and this holds
/// `<S as keylink::ExternalActorService>::Client`, which is the generated
/// `RegistryClient`. One struct cannot own both, and `new` fixes which one it
/// owns.
///
/// Everything else is shared. The prelude, the path decoders, the response
/// builders and the handler bodies are free functions both surfaces call, so
/// the address space is the only thing that differs — which is what
/// [`Surface`] carries.
pub struct ActorGateway<S, A>
where
    S: keylink::ExternalActorService,
    A: Access,
{
    client: S::Client,
    access: A,
    challenge: Challenge,
    limits: JsonLimits,
    service: PhantomData<fn() -> S>,
}

impl<S, A> ActorGateway<S, A>
where
    S: keylink::ExternalActorService,
    A: Access,
{
    /// Build a keyed gateway with mandatory access and a validated challenge.
    #[must_use]
    pub fn new(client: S::Client, access: A, challenge: Challenge) -> Self {
        Self {
            client,
            access,
            challenge,
            limits: JsonLimits::default(),
            service: PhantomData,
        }
    }

    /// Replace the bounded JSON ceilings.
    #[must_use]
    pub fn limits(mut self, limits: JsonLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Install the actor routes this surface selects.
    ///
    /// # Errors
    ///
    /// [`ConfigError::WrongAddressing`] for a singleton surface, and
    /// [`ConfigError::MissingRuntimeRoute`] when the inventory carries no
    /// actor rpc route.
    ///
    /// An EMPTY generated inventory is also refused. A service declaring only
    /// `send` and `tell` implements the actor seam with nothing in it, and a
    /// surface that mounted it would answer `404` per request instead of
    /// saying at startup that there is nothing to serve.
    pub fn router<State>(self, surface: HttpSurface) -> Result<Router<State>, ConfigError>
    where
        State: Clone + Send + Sync + 'static,
    {
        if surface.addressing() != Addressing::Actors {
            return Err(ConfigError::WrongAddressing);
        }
        install(self, surface)
    }
}

/// How a mounted service turns a request path into the surface that answers it.
///
/// The two gateways disagree about exactly two things: which generated function
/// an invocation goes through, and whether an actor id has to be decoded out of
/// the path before anything else happens. This is where both live, so the route
/// table, the prelude and every handler body exist once.
trait Mount: Send + Sync + 'static {
    /// The policy this mount asks. A named associated type rather than a second
    /// parameter on every function that takes a mount.
    type Access: Access;

    /// The generated inventory, readable before any address is decoded.
    ///
    /// Here so the empty-inventory refusal is made once. It used to live on
    /// `ActorGateway::router` alone, while its own comment stated the rule as
    /// general — and the singleton side can be empty too: `METHODS` carries one
    /// entry per `rpc`, `send`, `recv` or `blob`, so a service declaring only
    /// `tell` produces an empty one.
    fn methods(&self) -> &'static [keylink::ExternalMethod];

    /// `None` when the path addresses nothing: on an actor surface a malformed
    /// id, and on a singleton surface never.
    fn surface<'a>(&'a self, path: &str, route_path: &str) -> Option<Surface<'a, Self::Access>>;
}

impl<S, A> Mount for Gateway<S, A>
where
    S: keylink::ExternalService + Send + Sync + 'static,
    A: Access,
{
    type Access = A;

    fn methods(&self) -> &'static [keylink::ExternalMethod] {
        S::METHODS
    }

    fn surface<'a>(&'a self, _path: &str, _route_path: &str) -> Option<Surface<'a, A>> {
        let client = &self.client;
        Some(Surface {
            access: &self.access,
            challenge: &self.challenge,
            limits: self.limits,
            service: S::SERVICE,
            actor: None,
            invoke: Box::new(move |method, body, call, codec| {
                S::invoke(client, method, body, call, codec)
            }),
        })
    }
}

impl<S, A> Mount for ActorGateway<S, A>
where
    S: keylink::ExternalActorService + Send + Sync + 'static,
    A: Access,
{
    type Access = A;

    fn methods(&self) -> &'static [keylink::ExternalMethod] {
        S::METHODS
    }

    /// The actor is decoded BEFORE anything else, because `Access` is asked
    /// about it and a policy must never be handed a raw path segment. A
    /// malformed id is the same `404` a missing method is: telling them apart
    /// says which ids are well formed.
    fn surface<'a>(&'a self, path: &str, route_path: &str) -> Option<Surface<'a, A>> {
        let actor = decode_actor(path, route_path)?;
        let client = &self.client;
        let addressed = actor.clone();
        Some(Surface {
            access: &self.access,
            challenge: &self.challenge,
            limits: self.limits,
            service: S::SERVICE,
            actor: Some(actor),
            invoke: Box::new(move |method, body, call, codec| {
                S::invoke_at(client, &addressed, method, body, call, codec)
            }),
        })
    }
}

/// The verb, then the method, then the address this path names.
///
/// All three refusals belong to the router rather than the prelude, and the
/// order is the point. The verb is a fact about the ROUTE, knowable with no
/// address and no inventory. The method is a fact about the INVENTORY, which
/// [`Mount::methods`] hands over without an address. The address is decoded
/// last, and it is what builds the [`Surface`] the prelude then runs on.
///
/// Two of the three orderings are observable and one is not. A `GET` at a keyed
/// path whose actor segment is malformed answers `405`, not the `404` the id
/// would produce — decoding the id first would make the verb refusal
/// unreachable for exactly the callers most likely to be probing. Method before
/// address is NOT observable: a malformed id and an unknown method both answer
/// `GatewayError::MethodNotFound` byte for byte. It is written this way anyway,
/// because the property being protected is that the two are indistinguishable,
/// and an order that happens to produce that is weaker than one that cannot do
/// otherwise.
fn addressed<'a, M: Mount>(
    mount: &'a M,
    request: &Request<Body>,
    route: &Route,
    route_path: &str,
    kind: keylink::ExternalKind,
) -> Result<(Surface<'a, M::Access>, &'static str), GatewayError> {
    if request.method() != route.method() {
        return Err(GatewayError::MethodNotAllowed);
    }
    let methods = mount.methods();
    let decoded = decode_method_in(methods, request.uri().path(), route_path, kind)?;
    let Some(method) = methods
        .iter()
        .copied()
        .find(|candidate| candidate.name() == decoded && candidate.kind() == kind)
        .map(keylink::ExternalMethod::name)
    else {
        return Err(GatewayError::MethodNotFound);
    };
    let surface = mount
        .surface(request.uri().path(), route_path)
        .ok_or(GatewayError::MethodNotFound)?;
    Ok((surface, method))
}

/// Every route one surface serves, installed from the generated inventory.
///
/// Written once for both address spaces. A keyed surface serves the same route
/// families a singleton one does, under a template carrying one more segment,
/// and the only thing that differs is what [`Mount::surface`] builds — so a
/// second copy of this table would be a second place for a route to go missing.
///
/// Refuses [`ConfigError::MissingRuntimeRoute`] when the inventory carries no
/// rpc route, and an EMPTY inventory with it. A service that declares nothing
/// this gateway serves would answer `404` per request instead of saying at
/// startup that there is nothing to serve.
fn install<M: Mount, State>(mount: M, contract: HttpSurface) -> Result<Router<State>, ConfigError>
where
    State: Clone + Send + Sync + 'static,
{
    // An EMPTY generated inventory is refused here, for both address spaces. A
    // surface that mounted one would answer per request instead of saying at
    // startup that there is nothing to serve.
    if mount.methods().is_empty() {
        return Err(ConfigError::MissingRuntimeRoute);
    }
    let Some(rpc) = contract
        .routes()
        .find(|route| route.kind() == RouteKind::Rpc)
    else {
        return Err(ConfigError::MissingRuntimeRoute);
    };
    let rpc_path = format!("{}{}", contract.prefix(), rpc.template());
    let handler_path = rpc_path.clone();
    let mount = Arc::new(mount);
    let rpc_mount = Arc::clone(&mount);
    let mut router = Router::new().route(
        &rpc_path,
        any(move |request: Request<Body>| {
            let mount = Arc::clone(&rpc_mount);
            let path = handler_path.clone();
            async move {
                let (surface, method) =
                    match addressed(&*mount, &request, rpc, &path, keylink::ExternalKind::Rpc) {
                        Ok(surface) => surface,
                        Err(error) => return error.response(None),
                    };
                handle_rpc(request, rpc, method, &surface).await
            }
        }),
    );
    if let Some(limits) = contract.streaming() {
        // Sized here rather than on the gateway, because the bound arrives with
        // the surface: a gateway built without streaming serves no byte
        // resources and needs no table at all.
        let tickets = Arc::new(crate::blob::Tickets::new(limits.max_blob_tickets()));
        for route in contract.routes() {
            let path = format!("{}{}", contract.prefix(), route.template());
            let handler_path = path.clone();
            let mount = Arc::clone(&mount);
            let handler = match route.kind() {
                RouteKind::Send => any(move |request: Request<Body>| {
                    let mount = Arc::clone(&mount);
                    let path = handler_path.clone();
                    async move {
                        let (surface, method) = match addressed(
                            &*mount,
                            &request,
                            route,
                            &path,
                            keylink::ExternalKind::Send,
                        ) {
                            Ok(surface) => surface,
                            Err(error) => return error.response(None),
                        };
                        handle_send(request, route, method, limits, &surface).await
                    }
                }),
                RouteKind::Recv => any(move |request: Request<Body>| {
                    let mount = Arc::clone(&mount);
                    let path = handler_path.clone();
                    async move {
                        let (surface, method) = match addressed(
                            &*mount,
                            &request,
                            route,
                            &path,
                            keylink::ExternalKind::Recv,
                        ) {
                            Ok(surface) => surface,
                            Err(error) => return error.response(None),
                        };
                        handle_recv(request, route, method, limits, &surface).await
                    }
                }),
                // One template, two verbs, and therefore ONE registration:
                // `Router::route` panics on a second `any` for a path it already
                // serves. Each verb still carries its own `Route`, so the shared
                // prelude's verb check stays an exact comparison rather than a
                // set membership test.
                RouteKind::BlobHead => continue,
                RouteKind::BlobGet => {
                    let Some(head) = contract
                        .routes()
                        .find(|route| route.kind() == RouteKind::BlobHead)
                    else {
                        return Err(ConfigError::MissingRuntimeRoute);
                    };
                    let head_path = handler_path.clone();
                    let head_tickets = Arc::clone(&tickets);
                    let head_mount = Arc::clone(&mount);
                    let tickets = Arc::clone(&tickets);
                    axum::routing::get(move |request: Request<Body>| {
                        let mount = Arc::clone(&mount);
                        let tickets = Arc::clone(&tickets);
                        let path = handler_path.clone();
                        async move {
                            let (surface, method) = match addressed(
                                &*mount,
                                &request,
                                route,
                                &path,
                                keylink::ExternalKind::Blob,
                            ) {
                                Ok(surface) => surface,
                                // Matched by variant, not swallowed. A wrong
                                // verb cannot reach here today because the byte
                                // routes register through a `MethodRouter` that
                                // filters it first — but every other route in
                                // this table registers through `any`, and an
                                // `Err(_)` would turn that change into a silent
                                // `404`.
                                Err(GatewayError::MethodNotAllowed) => {
                                    return GatewayError::MethodNotAllowed.response(None)
                                }
                                Err(_) => return GatewayError::BlobNotFound.response(None),
                            };
                            handle_blob_bytes(
                                request, route, &path, method, limits, &tickets, &surface,
                            )
                            .await
                        }
                    })
                    .head(move |request: Request<Body>| {
                        let mount = Arc::clone(&head_mount);
                        let tickets = Arc::clone(&head_tickets);
                        let path = head_path.clone();
                        async move {
                            let (surface, method) = match addressed(
                                &*mount,
                                &request,
                                head,
                                &path,
                                keylink::ExternalKind::Blob,
                            ) {
                                Ok(surface) => surface,
                                Err(GatewayError::MethodNotAllowed) => {
                                    return GatewayError::MethodNotAllowed.response(None)
                                }
                                Err(_) => return GatewayError::BlobNotFound.response(None),
                            };
                            handle_blob_bytes(
                                request, head, &path, method, limits, &tickets, &surface,
                            )
                            .await
                        }
                    })
                }
                RouteKind::BlobLookup => {
                    let tickets = Arc::clone(&tickets);
                    any(move |request: Request<Body>| {
                        let mount = Arc::clone(&mount);
                        let tickets = Arc::clone(&tickets);
                        let path = handler_path.clone();
                        async move {
                            let (surface, method) = match addressed(
                                &*mount,
                                &request,
                                route,
                                &path,
                                keylink::ExternalKind::Blob,
                            ) {
                                Ok(surface) => surface,
                                Err(error) => return error.response(None),
                            };
                            handle_blob_lookup(request, route, method, &tickets, &surface).await
                        }
                    })
                }
                // Installed above, or not implemented at this phase. The
                // inventory lists only what a phase actually serves, so a kind
                // arriving here is a route added without a handler.
                _ => continue,
            };
            router = router.route(&path, handler);
        }
    }
    Ok(router.fallback(|| async { GatewayError::MethodNotFound.response(None) }))
}

/// How a handler reaches the generated client, and everything else it needs
/// that is not the request.
///
/// **One handler body, two address spaces.** The singleton and actor surfaces
/// hold DIFFERENT client types — `S::Client` against `RegistryClient` — so one
/// struct cannot own both and a second gateway is unavoidable. What is
/// avoidable is a second copy of every handler, and this is what avoids it: the
/// only thing the two surfaces disagree about is which generated function the
/// invocation goes through, so that is the one thing passed in.
///
/// The codec is not a parameter because the gateway has exactly one: every call
/// site builds a `JsonCodec` from the same response limit.
struct Surface<'a, A: Access> {
    access: &'a A,
    challenge: &'a Challenge,
    limits: JsonLimits,
    service: &'static str,
    /// `None` on a singleton surface. The validated id on an actor one, passed
    /// to `Access` before the call is made.
    ///
    /// Owned rather than borrowed, and the invocation is boxed for the same
    /// reason: a `Surface` holding references to a caller's locals could only
    /// be built inside the handler that uses it, which is what forced every
    /// handler to be written twice. One allocation per request, against a QUIC
    /// round trip, buys one copy of every handler.
    actor: Option<keylink::ActorId>,
    invoke: Box<Invoke<'a>>,
}

/// The one generated call a handler makes, already bound to its client and,
/// on an actor surface, to its actor.
///
/// The codec is an argument because the ceilings differ by route family: a
/// `recv` bounds its declared value by the streaming metadata limit, an `rpc`
/// by the JSON response limit.
type Invoke<'a> = dyn Fn(&'static str, &[u8], keylink::ExternalCall, JsonCodec) -> keylink::ExternalFuture<JsonCodec>
    + Send
    + Sync
    + 'a;

/// `POST /recv/{method}` and `POST /{actor}/recv/{method}`: bounded JSON in,
/// raw bytes out.
async fn handle_recv<A: Access>(
    request: Request<Body>,
    route: &'static Route,
    method: &'static str,
    limits: crate::StreamingLimits,
    surface: &Surface<'_, A>,
) -> Response<Body> {
    let (parts, body) = request.into_parts();
    let admitted = match surface.admit(&parts, method).await {
        Ok(admitted) => admitted,
        Err(response) => return response,
    };
    let Admitted { .. } = admitted;
    // A one-shot download has no reply to replay and no range to serve. Both
    // are refused rather than ignored, and refused before the arguments are
    // read, so a caller that asked for either is never charged for a body.
    if parts.headers.get(IDEMPOTENCY_HEADER).is_some() {
        return GatewayError::IdempotencyNotSupported.response(None);
    }
    if parts.headers.get(header::RANGE).is_some() || parts.headers.get(header::IF_RANGE).is_some() {
        return GatewayError::RangeNotSupported.response(None);
    }
    if !is_media(&parts.headers, route.request_media()) {
        return GatewayError::UnsupportedMediaType.response(None);
    }
    let arguments = match bounded_json(body, surface.limits.request.get()).await {
        Ok(arguments) => arguments,
        Err(error) => return error.response(None),
    };
    let (value, reader) = match (surface.invoke)(
        method,
        &arguments,
        keylink::ExternalCall::Recv,
        JsonCodec::new(limits.metadata_json_bytes()),
    )
    .await
    {
        Ok(keylink::ExternalOutput::Recv { value, body }) => (value, body),
        Ok(_) => return GatewayError::UpstreamError.response(None),
        Err(error) => return GatewayError::from_external(error).response(None),
    };
    // Everything refusable is refused here, while a status line can still say
    // why. Past this point the only way to report a fault is to break the body,
    // so a length the sender already declared is checked now.
    if reader
        .declared_len()
        .is_some_and(|len| len > limits.max_download_bytes())
    {
        return GatewayError::ResponseStreamTooLarge.response(None);
    }
    let result = match crate::stream::result_header(&value, limits.metadata_json_bytes()) {
        Ok(result) => result,
        Err(_) => return GatewayError::ResponseTooLarge.response(None),
    };
    stream_response(reader, result, route, limits)
}

impl<A: Access> Surface<'_, A> {
    /// Everything every route does before it looks at a request body.
    ///
    /// On the [`Surface`], because that is the whole of what it needs: the policy,
    /// the challenge, the inventory, the service name and the address. A prelude
    /// copied into the second gateway is the duplication that has already cost this
    /// branch two anchors, `HJ9` and `HJ3`, and there is one surface type for both
    /// address spaces precisely so there is one of these.
    ///
    /// The order is a guarantee, and a narrower one than it looks. This half of it
    /// runs the principal before `Access`, and takes the method already selected.
    /// What the whole chain buys is that **`Access` is never asked about a route
    /// that does not exist**.
    ///
    /// The verb, the method and the address are decided earlier, in [`addressed`],
    /// in that order — see the note there for why each precedes the next.
    ///
    /// It does NOT hide which methods exist: because the method check precedes the
    /// principal check, an unauthenticated caller separates a real method (`401`)
    /// from a missing one (`404`), and a forbidden caller separates them the same
    /// way. That is the Task 1 shape and it is deliberate — enumerating method
    /// names off a generated inventory is low value — but the reason written here
    /// used to claim the opposite, which is the kind of wrong reason that survives
    /// review and gets defended.
    ///
    /// Takes the request PARTS rather than the request. `axum::body::Body` is
    /// `Send` and not `Sync`, so a `&Request<Body>` held across the `Access` await
    /// would make every handler future non-`Send` and no route would install.
    async fn admit(
        &self,
        parts: &http::request::Parts,
        method: &'static str,
    ) -> Result<Admitted, Response<Body>> {
        let Some(principal) = parts.extensions.get::<Principal>().cloned() else {
            return Err(GatewayError::Unauthenticated.response(Some(self.challenge.header())));
        };
        match self
            .access
            .authorize(Call {
                extensions: &parts.extensions,
                headers: &parts.headers,
                principal: &principal,
                service: self.service,
                method,
                actor: self.actor.as_ref(),
            })
            .await
        {
            Ok(()) => Ok(Admitted { principal }),
            Err(AccessError::Unauthenticated) => {
                Err(GatewayError::Unauthenticated.response(Some(self.challenge.header())))
            }
            // Watched red as row `HK20`: replacing this arm with `Ok(Admitted
            // { method, principal })` lets a denied caller reach the ticket
            // lookup, after which a resource it holds and one that never existed
            // stop answering alike.
            Err(AccessError::Forbidden) => Err(GatewayError::Forbidden.response(None)),
        }
    }
}

/// `POST /send/{method}`: raw bytes in, bounded JSON out.
///
/// Singleton only, and that is a guarantee rather than an omission: a keyed
/// service that declares `send` cannot be served as an actor at all, so no
/// actor surface ever reaches this.
async fn handle_send<A: Access>(
    request: Request<Body>,
    route: &'static Route,
    method: &'static str,
    limits: crate::StreamingLimits,
    surface: &Surface<'_, A>,
) -> Response<Body> {
    let (parts, body) = request.into_parts();
    let admitted = match surface.admit(&parts, method).await {
        Ok(admitted) => admitted,
        Err(response) => return response,
    };
    let Admitted { .. } = admitted;
    if parts.headers.get(IDEMPOTENCY_HEADER).is_some() {
        return GatewayError::IdempotencyNotSupported.response(None);
    }
    if parts.headers.get(header::RANGE).is_some() || parts.headers.get(header::IF_RANGE).is_some() {
        return GatewayError::RangeNotSupported.response(None);
    }
    if !crate::stream::is_octet_stream(&parts.headers) {
        return GatewayError::UnsupportedMediaType.response(None);
    }
    let arguments = match crate::stream::arguments(&parts.headers, limits.metadata_json_bytes()) {
        Ok(arguments) => arguments,
        Err(_) => return GatewayError::InvalidStreamMetadata.response(None),
    };
    let declared_len = parts
        .headers
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    if declared_len.is_some_and(|len| len > limits.max_upload_bytes()) {
        return GatewayError::RequestStreamTooLarge.response(None);
    }
    match (surface.invoke)(
        method,
        &arguments,
        keylink::ExternalCall::Send {
            body: crate::stream::upload_body(body, declared_len, limits.max_upload_bytes()),
        },
        JsonCodec::new(surface.limits.response.get()),
    )
    .await
    {
        Ok(keylink::ExternalOutput::Json(json)) => json_response(json, route),
        Ok(_) => GatewayError::UpstreamError.response(None),
        Err(error) => GatewayError::from_external(error).response(None),
    }
}

/// `POST /blob/{method}`: invoke, record a ticket, and answer with the
/// declared value and where the bytes live.
///
/// **Nothing is fetched.** A blob method hands back an address plus a
/// grant, and the payload can be gigabytes; turning a lookup into a
/// download would make the smallest call the most expensive one. The bytes
/// are a separate resource, and this is the only place its identity is
/// minted.
///
/// **The ticket is recorded last.** Insertion happens after the invocation
/// AND after the declared value encodes, so a call that failed on the way
/// out cannot leave a grant behind that no caller was ever told about.
async fn handle_blob_lookup<A: Access>(
    request: Request<Body>,
    route: &'static Route,
    method: &'static str,
    tickets: &crate::blob::Tickets<keylink::BlobHandle>,
    surface: &Surface<'_, A>,
) -> Response<Body> {
    let (parts, body) = request.into_parts();
    let admitted = match surface.admit(&parts, method).await {
        Ok(admitted) => admitted,
        Err(response) => return response,
    };
    let Admitted { principal } = admitted;
    // `Range` and `If-Range` are IGNORED here, not refused. RFC 9110 §14.2:
    // "A server MUST ignore a Range header field received with a request
    // method ... for which range handling is not defined", and §13.1.5
    // says the same of `If-Range` without a `Range`. A lookup addresses no
    // bytes yet, so there is nothing for either field to name — and a
    // download manager that attaches `Range` to every request would meet a
    // hard refusal on a route the specification guarantees drops it.
    //
    // The plan's route table says to reject both. RFC 9110 says otherwise,
    // and an HTTP gateway follows the RFC.
    // DEVIATION from the plan's route table, which lists idempotency as
    // optional here. `keylink::ExternalCall::Blob` carries no key slot, so
    // Adding the slot is `keylink/src/external.rs`, which Task 6 owns, and
    // the gateway has nowhere to put one until then.
    //
    // Anchored on this comment line as well as the check, because the same
    // check appears in four handlers and nothing else tells them apart.
    // Silently ignoring a caller's replay key is worse than refusing it.
    if parts.headers.get(IDEMPOTENCY_HEADER).is_some() {
        return GatewayError::IdempotencyNotSupported.response(None);
    }
    if !is_media(&parts.headers, route.request_media()) {
        return GatewayError::UnsupportedMediaType.response(None);
    }
    let arguments = match bounded_json(body, surface.limits.request.get()).await {
        Ok(arguments) => arguments,
        Err(error) => return error.response(None),
    };
    let (value, handle) = match (surface.invoke)(
        method,
        &arguments,
        keylink::ExternalCall::Blob,
        JsonCodec::new(surface.limits.response.get()),
    )
    .await
    {
        Ok(keylink::ExternalOutput::Blob { value, handle }) => (value, handle),
        Ok(_) => return GatewayError::UpstreamError.response(None),
        Err(error) => return GatewayError::from_external(error).response(None),
    };

    let address = crate::blob::Address::of(handle.id());
    // Prefix-preserving because it is built from the path the caller
    // actually reached, not from the template. A gateway mounted under
    // `/v1` answers `/v1/blob/Fetch/<address>` without being told it is.
    let location = format!("{}/{}", parts.uri.path(), address.as_str());
    let Ok(location) = http::HeaderValue::from_str(&location) else {
        return GatewayError::UpstreamError.response(None);
    };
    let mut response = json_response(value, route);
    tickets.insert(ticket_for(principal, method, address, surface), handle);
    response.headers_mut().insert(header::LOCATION, location);
    response
}

/// `GET` and `HEAD` on `/blob/{method}/{id}`.
///
/// One function for both verbs, because every check up to the last one is
/// identical and the plan's own rule is that HEAD answers the same
/// metadata. What differs is one thing: HEAD never opens a byte reader,
/// and never applies a range.
///
/// The order is the whole design, and each step is placed where it is for
/// a reason a later reader will otherwise remove:
///
/// 1. `Access` runs before the ticket lookup, so a caller who may not
///    reach this method learns nothing about which resources exist.
/// 2. The path address is a lookup key and never a handle. A perfectly
///    formed address nobody granted finds nothing.
/// 3. Availability is rechecked on EVERY request. A ticket is a record
///    that a grant was issued, not a promise the holder still serves it.
/// 4. Conditions before ranges: a `304` is a complete answer and a range
///    of a representation the caller already has is wasted work.
/// 5. `If-Range` only after a valid range, because it modifies a range and
///    has no meaning without one.
async fn handle_blob_bytes<A: Access>(
    request: Request<Body>,
    route: &'static Route,
    route_path: &str,
    method: &'static str,
    limits: crate::StreamingLimits,
    tickets: &crate::blob::Tickets<keylink::BlobHandle>,
    surface: &Surface<'_, A>,
) -> Response<Body> {
    let (parts, body) = request.into_parts();
    let admitted = match surface.admit(&parts, method).await {
        Ok(admitted) => admitted,
        Err(response) => return response,
    };
    let Admitted { principal } = admitted;

    // Refused before anything is looked up, while a status line can still
    // carry the reason. A byte route addresses existing content: there is
    // nothing for a request body to say and nothing for a replay key to
    // replay.
    if parts.headers.get(IDEMPOTENCY_HEADER).is_some() {
        return GatewayError::IdempotencyNotSupported.response(None);
    }
    if !is_empty_body(&parts.headers) {
        return GatewayError::UnsupportedMediaType.response(None);
    }

    // A malformed address is the same `404` as a well formed one nobody
    // holds. Telling them apart is an oracle for what a valid address even
    // looks like.
    let Some(address) = decode_blob_address(parts.uri.path(), route_path) else {
        return GatewayError::BlobNotFound.response(None);
    };
    let ticket = ticket_for(principal, method, address.clone(), surface);
    let Some((generation, handle)) = tickets.get(&ticket) else {
        return GatewayError::BlobNotFound.response(None);
    };

    // Fresh, every time, and the clone above is what lets it happen with no
    // lock held across the await.
    let total = match handle.verified_len().await {
        Ok(total) => total,
        Err(keylink::Error::BlobRefused { .. }) => return gone(tickets, &ticket, generation),
        // The same mapping `handle_rpc` uses. Collapsing every upstream
        // failure to `502` here would tell a caller to stop retrying a
        // holder that is merely unreachable.
        Err(error) => return GatewayError::from_keylink(error).response(None),
    };

    let tag = crate::blob::etag(&address);
    match crate::blob::none_match(&parts.headers, &tag) {
        Ok(true) => return not_modified(&tag),
        Ok(false) => {}
        Err(()) => return GatewayError::InvalidRequest.response(None),
    }

    let wants_bytes = route.kind() == RouteKind::BlobGet;
    let asked = if wants_bytes {
        match crate::blob::range(&parts.headers, total) {
            Ok(asked) => asked,
            Err(crate::blob::RangeError::Malformed) => {
                return GatewayError::InvalidRequest.response(None)
            }
            Err(crate::blob::RangeError::Unsatisfiable) => return unsatisfiable(&tag, total),
        }
    } else {
        // HEAD ignores `Range` and `If-Range` entirely and answers for the
        // full representation, which is what a client uses it to size.
        crate::blob::Asked::Whole
    };
    let asked = match asked {
        crate::blob::Asked::Part(_) if !crate::blob::if_range_allows(&parts.headers, &tag) => {
            crate::blob::Asked::Whole
        }
        asked => asked,
    };

    drop(body);
    let mut response = Response::builder().header(header::ETAG, tag.clone());
    response = response
        .header(header::ACCEPT_RANGES, "bytes")
        // `private` because a ticket is bound to one principal, and
        // `no-transform` because a proxy that recompresses content-addressed
        // bytes breaks the address they are named by.
        .header(header::CACHE_CONTROL, "private, no-transform");
    if let Some(media) = route.response_media() {
        // From the inventory, not a literal. Task 7 generates the OpenAPI
        // document from the same `Route`, so a literal here would let the
        // document describe a media type the gateway does not send.
        response = response.header(header::CONTENT_TYPE, media);
    }

    let (status, span) = match &asked {
        crate::blob::Asked::Whole => (route.success_status(), 0..total),
        crate::blob::Asked::Part(range) => {
            response = response.header(
                header::CONTENT_RANGE,
                format!("bytes {}-{}/{total}", range.start, range.end - 1),
            );
            (http::StatusCode::PARTIAL_CONTENT, range.clone())
        }
    };
    let length = span.end - span.start;
    response = response
        .status(status)
        .header(header::CONTENT_LENGTH, length.to_string());

    // Checked for BOTH verbs, and above the HEAD return. RFC 9110 §9.3.2
    // asks a server to answer a HEAD with the fields it would send for the
    // GET, and a HEAD that says `200` for content the GET refuses is the
    // one answer a client uses to decide whether to start.
    if length > limits.max_download_bytes() {
        return GatewayError::ResponseStreamTooLarge.response(None);
    }
    if !wants_bytes {
        return response
            .body(Body::empty())
            .unwrap_or_else(|_| GatewayError::UpstreamError.response(None));
    }
    let reader = match if span == (0..total) {
        handle.open().await
    } else {
        handle.open_range(span).await
    } {
        Ok(reader) => reader,
        Err(keylink::Error::BlobRefused { .. }) => return gone(tickets, &ticket, generation),
        Err(error) => return GatewayError::from_keylink(error).response(None),
    };
    response
        // Bounded by the length this response just declared, not by the
        // gateway ceiling. A one-byte range under an 8 MiB ceiling would
        // otherwise run to 8 MiB before the adapter objected, and what
        // stopped it would be hyper's framing — a broken connection rather
        // than this adapter saying which limit was crossed.
        .body(crate::stream::download_body(reader, length))
        .unwrap_or_else(|_| GatewayError::UpstreamError.response(None))
}

/// `POST /rpc/{method}` and `POST /{actor}/rpc/{method}`: bounded JSON in,
/// bounded JSON out.
async fn handle_rpc<A: Access>(
    request: Request<Body>,
    route: &'static Route,
    method: &'static str,
    surface: &Surface<'_, A>,
) -> Response<Body> {
    let (parts, body) = request.into_parts();
    let admitted = match surface.admit(&parts, method).await {
        Ok(admitted) => admitted,
        Err(response) => return response,
    };
    let Admitted { principal } = admitted;
    if !is_media(&parts.headers, route.request_media()) {
        return GatewayError::UnsupportedMediaType.response(None);
    }
    let key = match derived_key(&parts.headers, &principal, method, surface) {
        Ok(key) => key,
        Err(error) => return error.response(None),
    };
    let body = match bounded_json(body, surface.limits.request.get()).await {
        Ok(body) => body,
        Err(error) => return error.response(None),
    };
    match (surface.invoke)(
        method,
        &body,
        keylink::ExternalCall::Rpc { key },
        JsonCodec::new(surface.limits.response.get()),
    )
    .await
    {
        Ok(keylink::ExternalOutput::Json(json)) => json_response(json, route),
        Ok(_) => GatewayError::UpstreamError.response(None),
        Err(error) => GatewayError::from_external(error).response(None),
    }
}

/// What survived the shared prelude: the exact generated method, and who asked.
struct Admitted {
    principal: Principal,
}

fn stream_response(
    reader: keylink::BodyReader,
    result: (http::HeaderName, http::HeaderValue),
    route: &Route,
    limits: crate::StreamingLimits,
) -> Response<Body> {
    let declared = reader.declared_len();
    let mut response = Response::new(crate::stream::download_body(
        reader,
        limits.max_download_bytes(),
    ));
    *response.status_mut() = route.success_status();
    let headers = response.headers_mut();
    if let Some(media) = route.response_media() {
        headers.insert(header::CONTENT_TYPE, http::HeaderValue::from_static(media));
    }
    headers.insert(result.0, result.1);
    // Only when the sender knew. Announcing a length for an open-ended stream
    // would be a promise the adapter cannot keep.
    if let Some(len) = declared {
        if let Ok(value) = http::HeaderValue::from_str(&len.to_string()) {
            headers.insert(header::CONTENT_LENGTH, value);
        }
    }
    response
}

#[cfg(test)]
const SINGLETON_RPC_PATH: &str = "/rpc/{method}";

#[cfg(test)]
fn singleton_rpc_route() -> &'static Route {
    crate::__contract::singleton_rpc_route()
}

/// Collect a bounded JSON request body, or the refusal that replaces it.
///
/// Shared by every route that takes JSON in. It was duplicated per handler
/// once, which is how `HJ9`'s registered anchor stopped being unique the moment
/// a second route was added — a copy is not only harder to change, it can also
/// make the thing that watches it ambiguous.
async fn bounded_json(body: Body, limit: usize) -> Result<axum::body::Bytes, GatewayError> {
    match axum::body::to_bytes(body, limit.saturating_add(1)).await {
        Ok(body) if body.len() <= limit => Ok(body),
        Ok(_) => Err(GatewayError::RequestTooLarge),
        // A collection failure that is not the cap is a broken request, and
        // saying "too large" about a short one would be a lie with a status.
        Err(error) if is_length_limit(&error) => Err(GatewayError::RequestTooLarge),
        Err(_) => Err(GatewayError::InvalidRequest),
    }
}

fn json_response(json: Vec<u8>, route: &Route) -> Response<Body> {
    let mut response = Response::new(Body::from(json));
    *response.status_mut() = route.success_status();
    if let Some(media) = route.response_media() {
        response
            .headers_mut()
            .insert(header::CONTENT_TYPE, http::HeaderValue::from_static(media));
    }
    response
}

/// The raw text of one `{name}` placeholder in a route template.
///
/// **Segment-wise, and the segment count must match.** The three decoders this
/// replaces each stripped a literal prefix, which works only for the template
/// shapes they were written against: `/v1/{actor}/rpc/{method}` fed to a
/// method decoder that expected `{method}` to follow a literal produced a
/// `404` for every keyed call. Measured, on the first end-to-end actor row.
///
/// Requiring equal segment counts is also what refuses `/v1/a/b/rpc/Sum`,
/// where a prefix strip would have yielded the actor `a` and left `/b` behind.
///
/// The text is RAW. Axum's own extractors percent-decode first, which would
/// turn `%2F` into a separator and change what was addressed.
fn raw_segment<'a>(path: &'a str, template: &str, name: &str) -> Option<&'a str> {
    let path: Vec<&str> = path.split('/').collect();
    let template: Vec<&str> = template.split('/').collect();
    if path.len() != template.len() {
        return None;
    }
    let mut found = None;
    for (segment, part) in path.iter().zip(template.iter()) {
        if *part == name {
            found = Some(*segment);
        } else if !(part.starts_with('{') && part.ends_with('}')) && segment != part {
            return None;
        }
    }
    found.filter(|segment| !segment.is_empty())
}

/// Decode the method segment against an inventory the caller names.
///
/// Both surfaces have their own `METHODS`, and the actor one is a strict
/// subset, so the bound on the method segment differs between them — which is
/// the whole reason this takes a list rather than a type.
///
/// **A segment too long to be any method NAMES no method.** It used to answer
/// `400 invalid_request`, and that was an oracle: the flip point between the
/// two statuses is the exact byte length of the longest method name of that
/// kind, readable one byte at a time by a caller who has not authenticated,
/// because this runs before the principal lookup. Worse at the bound: a keyed
/// service declaring no `recv` method sized the bound at ZERO, so EVERY well
/// formed path on that route family answered `400` — which is both a lie about
/// the request and a way to ask whether a service serves any method of a kind.
/// A keyed inventory is a strict subset of a singleton one, so an empty
/// kind-filtered inventory is ordinary rather than a misconfiguration.
///
/// `InvalidRequest` is kept for `InvalidEncoding` alone, which is a statement
/// about the request's syntax and no statement at all about the inventory.
fn decode_method_in(
    methods: &'static [keylink::ExternalMethod],
    path: &str,
    route_path: &str,
    kind: keylink::ExternalKind,
) -> Result<String, GatewayError> {
    let encoded = raw_segment(path, route_path, "{method}").ok_or(GatewayError::MethodNotFound)?;
    let max_bytes = methods
        .iter()
        .filter(|method| method.kind() == kind)
        .map(|method| method.name().len())
        .max()
        .ok_or(GatewayError::MethodNotFound)?;
    crate::raw_path::parse(encoded, crate::raw_path::Shape::Segment { max_bytes }).map_err(
        |error| match error {
            crate::raw_path::ParseError::InvalidEncoding => GatewayError::InvalidRequest,
            _ => GatewayError::MethodNotFound,
        },
    )
}

/// The `{actor}` segment of an actor-route path, decoded once.
///
/// Bounded before it is decoded, so an attacker's megabyte of `%41` is refused
/// without being allocated, and constructed through [`keylink::ActorId::new`],
/// so a policy downstream is handed a validated id rather than a string that
/// looks like one.
///
/// **The decode bound is not watched, and it cannot be from here.** Raising it
/// past `MAX_KEY_LEN` came back GREEN at `exit=0`: `ActorId::new` refuses the
/// oversized id one line later, so a test that reads a return value sees the
/// same answer either way. What the bound buys is refusing before allocating,
/// and that is invisible to an assertion. Recorded rather than claimed.
fn decode_actor(path: &str, route_path: &str) -> Option<keylink::ActorId> {
    let encoded = raw_segment(path, route_path, "{actor}")?;
    let decoded = crate::raw_path::parse(
        encoded,
        crate::raw_path::Shape::Segment {
            max_bytes: keylink::MAX_KEY_LEN,
        },
    )
    .ok()?;
    keylink::ActorId::new(decoded).ok()
}

/// The `{id}` segment of a byte-route path, as raw text.
///
/// A malformed address is the same `404` as a well formed one nobody holds.
fn decode_blob_address(path: &str, route_path: &str) -> Option<crate::blob::Address> {
    crate::blob::Address::parse(raw_segment(path, route_path, "{id}")?)
}

/// What a byte resource is keyed on, in ONE place.
///
/// Both handlers built this literal, and a key written twice is a key that can
/// drift by one field. It is also unbreakable as a pair: forcing the principal
/// on the read side alone makes every lookup MISS, so a test that only asserts
/// absences stays green while the scoping is gone. Measured — the first
/// version of row `HK16` came back `exit=0`. One definition is what makes the
/// break reach both sides at once.
fn ticket_for<A: Access>(
    principal: Principal,
    method: &'static str,
    id: crate::blob::Address,
    surface: &Surface<'_, A>,
) -> crate::blob::Ticket {
    crate::blob::Ticket {
        principal,
        method,
        actor: surface.actor.clone(),
        id,
    }
}

/// One length-delimited field.
///
/// The length goes in FIRST and is fixed width, so no field's bytes can be
/// read as another's. Concatenating `principal || service || method` lets a
/// boundary move: `("ab", "c")` and `("a", "bc")` hash the same, and two
/// callers who should never share an identity would share one.
fn delimited(hasher: &mut blake3::Hasher, field: &[u8]) {
    hasher.update(&(field.len() as u64).to_be_bytes());
    hasher.update(field);
}

/// `Content-Length: 0` is an empty body. A positive length or a transfer
/// encoding is not, and neither is refused later — a byte route that read a
/// body it has no use for would let a caller push bytes at it for free.
fn is_empty_body(headers: &HeaderMap) -> bool {
    if headers.get(header::TRANSFER_ENCODING).is_some() {
        return false;
    }
    match headers.get(header::CONTENT_LENGTH) {
        None => true,
        Some(value) => value.to_str().is_ok_and(|value| value.trim() == "0"),
    }
}

/// `304`, carrying only what a cache validator needs.
fn not_modified(tag: &str) -> Response<Body> {
    Response::builder()
        .status(http::StatusCode::NOT_MODIFIED)
        .header(header::ETAG, header_value(tag))
        .header(header::CACHE_CONTROL, "private, no-transform")
        .body(Body::empty())
        .unwrap_or_else(|_| GatewayError::UpstreamError.response(None))
}

/// `416`, carrying the length that makes the next ask satisfiable.
///
/// Both headers are built from bytes that cannot be invalid — a decimal length
/// and an entity tag that is 64 hex characters in quotes — so the fallible
/// constructors are not used at all. A best-effort `if let` here would let a
/// `416` go out without the `Content-Range` RFC 9110 §15.5.17 requires, and
/// silently: the caller would be told its range failed and not what would
/// succeed.
fn unsatisfiable(tag: &str, total: u64) -> Response<Body> {
    let mut response = GatewayError::RangeNotSatisfiable.response(None);
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_RANGE,
        header_value(&format!("bytes */{total}")),
    );
    headers.insert(header::ETAG, header_value(tag));
    response
}

/// The holder refused: forget this grant and say so.
///
/// **Compare-and-remove, not remove.** The refusal arrived after an await, and
/// another request can have refreshed this ticket in between. Deleting by key
/// alone would destroy a grant that works on the strength of a stale answer,
/// and the caller who had just refreshed it would get `404` for content it
/// holds.
///
/// One function and not two arms, because the availability check and the reader
/// open both reach it, and a rule written twice is a rule that can drift by one
/// line — which is also what makes it breakable as a single anchor.
fn gone(
    tickets: &crate::blob::Tickets<keylink::BlobHandle>,
    ticket: &crate::blob::Ticket,
    generation: crate::blob::Generation,
) -> Response<Body> {
    tickets.remove(ticket, generation);
    GatewayError::BlobGone.response(None)
}

/// A header value from text this gateway generated itself.
///
/// Every caller passes a decimal number, a quoted hex address, or a fixed
/// token, none of which can fail the visible-ASCII check. The fallback is the
/// empty value rather than a panic, because an `expect` on an unreachable
/// branch costs the process and this costs one header.
fn header_value(text: &str) -> http::HeaderValue {
    http::HeaderValue::from_str(text).unwrap_or_else(|_| http::HeaderValue::from_static(""))
}

fn is_media(headers: &HeaderMap, expected: Option<&str>) -> bool {
    let mut values = headers.get_all(header::CONTENT_TYPE).iter();
    let Some(value) = values.next() else {
        return false;
    };
    if values.next().is_some() {
        return false;
    }
    let Ok(value) = value.to_str() else {
        return false;
    };
    let Ok(media) = value.parse::<mime::Mime>() else {
        return false;
    };
    match expected {
        Some("application/json") => {
            media.type_() == mime::APPLICATION
                && (media.subtype() == mime::JSON || media.suffix() == Some(mime::JSON))
        }
        Some("application/octet-stream") => {
            media.type_() == mime::APPLICATION
                && media.subtype() == mime::OCTET_STREAM
                && media.params().next().is_none()
        }
        _ => false,
    }
}

fn is_length_limit(error: &axum::Error) -> bool {
    let mut source = error.source();
    while let Some(error) = source {
        if error.is::<http_body_util::LengthLimitError>() {
            return true;
        }
        source = error.source();
    }
    false
}

fn derived_key<A: Access>(
    headers: &HeaderMap,
    principal: &Principal,
    method: &'static str,
    surface: &Surface<'_, A>,
) -> Result<Option<keylink::IdempotencyKey>, GatewayError> {
    let mut values = headers.get_all(IDEMPOTENCY_HEADER).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(GatewayError::InvalidIdempotencyKey);
    }
    let value = value
        .to_str()
        .map_err(|_| GatewayError::InvalidIdempotencyKey)?;
    if value.is_empty()
        || value.len() > keylink::MAX_KEY_LEN
        || !value.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
    {
        return Err(GatewayError::InvalidIdempotencyKey);
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(KEY_DOMAIN);
    delimited(&mut hasher, principal.as_bytes());
    delimited(&mut hasher, surface.service.as_bytes());
    delimited(&mut hasher, method.as_bytes());
    // The addressing mode, not just the id. A singleton and an actor whose id
    // is empty cannot both exist, but the tag is what makes that a fact about
    // the encoding rather than a fact about `ActorId`'s validation.
    hasher.update(&[u8::from(surface.actor.is_some())]);
    delimited(
        &mut hasher,
        surface
            .actor
            .as_ref()
            .map_or(b"", |id| id.as_str().as_bytes()),
    );
    delimited(&mut hasher, value.as_bytes());
    let encoded = hasher.finalize().to_hex().to_string();
    keylink::IdempotencyKey::new(encoded)
        .map(Some)
        .map_err(|_| GatewayError::InvalidIdempotencyKey)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use std::sync::atomic::AtomicU64;
    use std::{
        convert::Infallible,
        future::{pending, Future},
        pin::Pin,
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc, Mutex,
        },
        task::{Context, Poll, Waker},
    };

    use axum::{body::Body, Router};
    use bytes::Bytes;
    use http::{HeaderValue, Request};
    use http_body::{Body as HttpBody, Frame, SizeHint};
    use iroh::{endpoint::presets, Endpoint};
    use keylink::{
        Allowlist, Context as KeylinkContext, ExternalError, ExternalFuture, ExternalService, Node,
    };
    use tokio::io::AsyncWriteExt;

    use super::Gateway;
    use crate::{Access, AccessError, Call, Challenge, Principal};

    #[derive(Clone)]
    struct TestClient {
        invoked: Arc<AtomicUsize>,
        dropped: Arc<AtomicBool>,
        pending: bool,
    }

    struct TestService;

    impl ExternalService for TestService {
        const SERVICE: &'static str = "CapturedService";
        const METHODS: &'static [keylink::ExternalMethod] = &[keylink::ExternalMethod::new(
            "Run",
            keylink::ExternalKind::Rpc,
        )];
        type Client = TestClient;

        fn invoke<C: keylink::ServiceCodec>(
            client: &Self::Client,
            _method: &str,
            _body: &[u8],
            _call: keylink::ExternalCall,
            codec: C,
        ) -> ExternalFuture<C> {
            client.invoked.fetch_add(1, Ordering::SeqCst);
            let sentinel = DropSentinel(Arc::clone(&client.dropped));
            if client.pending {
                Box::pin(async move {
                    let _sentinel = sentinel;
                    pending::<Result<keylink::ExternalOutput<C::Output>, ExternalError<C::Error>>>()
                        .await
                })
            } else {
                drop(sentinel);
                Box::pin(async move {
                    codec
                        .encode(&())
                        .map(keylink::ExternalOutput::Json)
                        .map_err(ExternalError::Codec)
                })
            }
        }
    }

    struct DropSentinel(Arc<AtomicBool>);

    impl Drop for DropSentinel {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    struct RecordingBody {
        polls: Arc<AtomicUsize>,
        bytes: Option<Bytes>,
    }
    struct ErrorBody;

    impl HttpBody for ErrorBody {
        type Data = Bytes;
        type Error = std::io::Error;

        fn poll_frame(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            Poll::Ready(Some(Err(std::io::Error::other("short body failed"))))
        }
    }

    impl RecordingBody {
        fn new(polls: Arc<AtomicUsize>) -> Self {
            Self {
                polls,
                bytes: Some(Bytes::from_static(b"{")),
            }
        }
    }

    impl HttpBody for RecordingBody {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(self.bytes.take().map(Frame::data).map(Ok))
        }

        fn size_hint(&self) -> SizeHint {
            SizeHint::with_exact(self.bytes.as_ref().map_or(0, Bytes::len) as u64)
        }
    }

    #[derive(Clone)]
    struct PanicAccess;

    impl Access for PanicAccess {
        async fn authorize(&self, _call: Call<'_>) -> Result<(), AccessError> {
            panic!("unknown methods must bypass access")
        }
    }

    #[derive(Clone)]
    struct Deny;

    impl Access for Deny {
        async fn authorize(&self, _call: Call<'_>) -> Result<(), AccessError> {
            Err(AccessError::Forbidden)
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    struct Captured {
        extension: Option<u64>,
        header: Option<HeaderValue>,
        principal: Principal,
        service: &'static str,
        method: &'static str,
    }

    #[derive(Clone)]
    struct Capture(Arc<Mutex<Option<Captured>>>);

    impl Access for Capture {
        async fn authorize(&self, call: Call<'_>) -> Result<(), AccessError> {
            *self.0.lock().expect("capture mutex") = Some(Captured {
                extension: call.extensions.get::<u64>().copied(),
                header: call.headers.get("x-capture").cloned(),
                principal: call.principal.clone(),
                service: call.service,
                method: call.method,
            });
            Err(AccessError::Forbidden)
        }
    }

    fn client() -> TestClient {
        TestClient {
            invoked: Arc::new(AtomicUsize::new(0)),
            dropped: Arc::new(AtomicBool::new(false)),
            pending: true,
        }
    }

    fn ready_client() -> TestClient {
        TestClient {
            invoked: Arc::new(AtomicUsize::new(0)),
            dropped: Arc::new(AtomicBool::new(false)),
            pending: false,
        }
    }

    fn gateway<A: Access>(client: TestClient, access: A) -> Gateway<TestService, A> {
        Gateway::new(
            client,
            access,
            Challenge::new(HeaderValue::from_static("Bearer realm=keylink"))
                .expect("valid challenge"),
        )
    }

    fn request(path: &str, body: Body) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri(path)
            .header("content-type", "application/json")
            .extension(Principal::new(b"tenant-a").expect("valid principal"))
            .body(body)
            .expect("request")
    }

    #[test]
    fn singleton_gateway_rejects_an_actor_surface() {
        let surface =
            crate::HttpSurface::new("/v1", crate::Addressing::Actors).expect("valid actor surface");
        let error = gateway(ready_client(), Denyless)
            .router::<()>(surface)
            .expect_err("singleton client cannot serve actors");
        assert_eq!(error, crate::ConfigError::WrongAddressing);
    }

    #[test]
    fn malformed_percent_encoding_is_an_invalid_adapter_request() {
        assert!(matches!(
            super::decode_method_in(
                <TestService as keylink::ExternalService>::METHODS,
                "/rpc/%ZZ",
                super::SINGLETON_RPC_PATH,
                keylink::ExternalKind::Rpc,
            ),
            Err(crate::error::GatewayError::InvalidRequest)
        ));
        assert!(matches!(
            super::decode_method_in(
                <TestService as keylink::ExternalService>::METHODS,
                "/rpc/%FF",
                super::SINGLETON_RPC_PATH,
                keylink::ExternalKind::Rpc,
            ),
            Err(crate::error::GatewayError::InvalidRequest)
        ));
    }

    #[derive(Clone)]
    struct SendClient {
        invoked: Arc<Mutex<Vec<keylink::ExternalKind>>>,
    }

    struct SendService;

    impl ExternalService for SendService {
        const SERVICE: &'static str = "SendService";
        const METHODS: &'static [keylink::ExternalMethod] = &[
            keylink::ExternalMethod::new("Run", keylink::ExternalKind::Rpc),
            keylink::ExternalMethod::new("Upload", keylink::ExternalKind::Send),
            keylink::ExternalMethod::new("Download", keylink::ExternalKind::Recv),
            // Present so a blob-lookup row can reach the handler at all.
            // Without it `decode_method` bounds the method segment at the
            // longest BLOB method name, which is zero, and refuses every name
            // with a `400` before the body is even in scope — which is how the
            // first version of `blob_lookup_refuses_on_headers_before_any_body_poll`
            // passed with its guard removed.
            keylink::ExternalMethod::new("Fetch", keylink::ExternalKind::Blob),
        ];
        type Client = SendClient;

        fn invoke<C: keylink::ServiceCodec>(
            client: &Self::Client,
            _method: &str,
            _arguments: &[u8],
            call: keylink::ExternalCall,
            codec: C,
        ) -> ExternalFuture<C> {
            client.invoked.lock().expect("test mutex").push(call.kind());
            Box::pin(async move {
                codec
                    .encode(&7_u64)
                    .map(keylink::ExternalOutput::Json)
                    .map_err(ExternalError::Codec)
            })
        }
    }

    fn send_gateway(
        invoked: Arc<Mutex<Vec<keylink::ExternalKind>>>,
    ) -> Gateway<SendService, Denyless> {
        Gateway::<SendService, _>::new(
            SendClient { invoked },
            Denyless,
            Challenge::new(HeaderValue::from_static("Bearer realm=keylink"))
                .expect("valid challenge"),
        )
        .limits(crate::JsonLimits::new(32, 32).expect("positive JSON limits"))
    }

    /// One refusal case: content type, arguments header, one extra header,
    /// declared `Content-Length`, and the status all four must produce.
    type Refusal = (
        Option<&'static str>,
        Option<&'static str>,
        Option<(&'static str, &'static str)>,
        Option<&'static str>,
        http::StatusCode,
    );

    /// Everything a `send` refuses on its headers alone, and the one property
    /// they share: the upload never starts. Refusing after the body has been
    /// read costs the bytes it was refusing.
    async fn refused_before_any_poll(cases: &[Refusal]) {
        for &(media, arguments, extra, length, status) in cases {
            let invoked = Arc::new(Mutex::new(Vec::new()));
            let polls = Arc::new(AtomicUsize::new(0));
            let limits = crate::StreamingLimits::new(2, 4, 4, 1).expect("positive limits");
            let route = crate::HttpSurface::new("", crate::Addressing::Singleton)
                .expect("valid surface")
                .with_streaming(limits)
                .routes()
                .find(|route| route.kind() == crate::__contract::RouteKind::Send)
                .expect("send route");
            let mut request = Request::builder()
                .method("POST")
                .uri("/send/Upload")
                .extension(Principal::new(b"tenant-a").expect("valid principal"));
            if let Some(media) = media {
                request = request.header("content-type", media);
            }
            if let Some(arguments) = arguments {
                request = request.header("keylink-arguments", arguments);
            }
            if let Some((name, value)) = extra {
                request = request.header(name, value);
            }
            if let Some(length) = length {
                request = request.header("content-length", length);
            }
            let response = send_gateway(Arc::clone(&invoked))
                .handle_send(
                    request
                        .body(Body::new(RecordingBody::new(Arc::clone(&polls))))
                        .expect("request"),
                    route,
                    "/send/{method}",
                    limits,
                )
                .await;
            assert_eq!(
                response.status(),
                status,
                "{media:?} {arguments:?} {extra:?} {length:?}"
            );
            assert_eq!(polls.load(Ordering::SeqCst), 0);
            assert!(invoked.lock().expect("test mutex").is_empty());
        }
    }

    /// A lookup that is refused on its headers must not read the body first.
    ///
    /// The plan's terminator for `blob_lookup_..._before_body_poll` is
    /// "recording body reports zero polls", and the end-to-end row cannot see
    /// a poll — it asserts a status, which holds whether the refusal came
    /// before or after `bounded_json`. This is the measurement, and it is the
    /// same `RecordingBody` the `send` rows use.
    ///
    /// Watched red with `scripts/break-once.sh` (row `HK25`) by replacing
    /// `// Silently ignoring a caller's replay key is worse than refusing it. if parts.headers.get(IDEMPOTENCY_HEADER).is_some() {`
    /// with `if false {`. The literal carries its comment line because the
    /// same check appears in four handlers and nothing else tells them apart.
    ///
    /// The FIRST version of this row passed with the guard removed. The test
    /// service had no blob method, so `decode_method` bounded the method
    /// segment at the longest blob name — zero — and refused every name with a
    /// `400` before the body was in scope. The status assertion matched that
    /// unrelated `400`, and `polls == 0` held because nothing reached the
    /// body. It now asserts the error CODE, and the service has a `Fetch`
    /// blob method so the request gets that far.
    /// The actor segment is read off the RAW path, so `%2F` stays one id.
    ///
    /// Axum's own path extractor percent-decodes before keylink sees a value.
    /// An id containing an encoded slash would arrive already split, and the
    /// gateway would address `a` on a route it thinks is `/a/b/rpc/...` —
    /// wrong actor, and a path that no longer matches its own template.
    ///
    /// Bounded before it is decoded, and decoded once. `MAX_KEY_LEN` is 128, so
    /// an encoded form is refused past 384 bytes without allocating for it.
    ///
    /// **That bound is not watched, and it cannot be from here.** Raising it
    /// past `MAX_KEY_LEN` came back GREEN at `exit=0`: `ActorId::new` refuses
    /// the oversized id one line later, so the length assertions below hold
    /// either way. What the bound buys is that an attacker's megabyte of `%41`
    /// is refused before it is allocated, and this row cannot see an
    /// allocation. Recorded rather than claimed.
    ///
    /// Watched red with `scripts/break-once.sh` (row `HK32`) by replacing
    /// `} else if !(part.starts_with('{') && part.ends_with('}')) && segment != part {`
    /// with `} else if false {`, which yields an actor for a path whose
    /// literals do not match its own route.
    ///
    /// The segment-count check beside it came back GREEN and is recorded
    /// rather than registered: for every input here the literal comparison
    /// already refuses first. It earns its place on a path SHORTER than the
    /// template, which this router cannot receive and a future one might.
    #[test]
    fn the_actor_segment_is_decoded_once_from_the_raw_path() {
        let route = "/v1/{actor}/rpc/{method}";

        assert_eq!(
            super::decode_actor("/v1/tenant-a/rpc/Sum", route).map(|id| id.as_str().to_owned()),
            Some("tenant-a".to_owned())
        );
        assert_eq!(
            super::decode_actor("/v1/obra%2F4471/rpc/Sum", route).map(|id| id.as_str().to_owned()),
            Some("obra/4471".to_owned()),
            "an encoded slash decodes to one id, not to two segments"
        );
        assert_eq!(
            super::decode_actor("/v1/a%20b/rpc/Sum", route).map(|id| id.as_str().to_owned()),
            Some("a b".to_owned())
        );

        for wrong in [
            "/v1//rpc/Sum",
            "/v1/a/b/rpc/Sum",
            "/v1/a%2/rpc/Sum",
            "/v1/a%zz/rpc/Sum",
            "/v2/a/rpc/Sum",
        ] {
            assert_eq!(super::decode_actor(wrong, route), None, "{wrong}");
        }

        let long = "x".repeat(keylink::MAX_KEY_LEN);
        assert!(
            super::decode_actor(&format!("/v1/{long}/rpc/Sum"), route).is_some(),
            "the longest valid id is still an id"
        );
        assert_eq!(
            super::decode_actor(&format!("/v1/{long}x/rpc/Sum"), route),
            None,
            "and one byte past the cap is not"
        );
    }

    #[tokio::test]
    async fn blob_lookup_refuses_on_headers_before_any_body_poll() {
        let invoked = Arc::new(Mutex::new(Vec::new()));
        let polls = Arc::new(AtomicUsize::new(0));
        let limits = crate::StreamingLimits::new(2, 4, 4, 1).expect("positive limits");
        let route = crate::HttpSurface::new("", crate::Addressing::Singleton)
            .expect("valid surface")
            .with_streaming(limits)
            .routes()
            .find(|route| route.kind() == crate::__contract::RouteKind::BlobLookup)
            .expect("blob lookup route");
        let tickets = crate::blob::Tickets::new(1);

        let response = send_gateway(Arc::clone(&invoked))
            .handle_blob_lookup(
                Request::builder()
                    .method("POST")
                    .uri("/blob/Fetch")
                    .header("content-type", "application/json")
                    .header(super::IDEMPOTENCY_HEADER, "k")
                    .extension(Principal::new(b"tenant-a").expect("valid principal"))
                    .body(Body::new(RecordingBody::new(Arc::clone(&polls))))
                    .expect("request"),
                route,
                "/blob/{method}",
                &tickets,
            )
            .await;

        assert_eq!(
            response.status(),
            http::StatusCode::BAD_REQUEST,
            "the idempotency refusal, not a path-decode refusal"
        );
        assert_eq!(
            String::from_utf8(
                axum::body::to_bytes(response.into_body(), 4096)
                    .await
                    .expect("a body")
                    .to_vec()
            )
            .expect("UTF-8"),
            r#"{"error":{"code":"idempotency_not_supported","message":"this method does not accept idempotency keys"}}"#,
            "and it is that refusal by code, not any other 400"
        );
        assert_eq!(
            polls.load(Ordering::SeqCst),
            0,
            "refusing after the body has been read costs the bytes it refused"
        );
        assert!(invoked.lock().expect("test mutex").is_empty());
    }

    #[tokio::test]
    async fn send_rejects_malformed_metadata_before_body_poll() {
        refused_before_any_poll(&[
            (
                None,
                None,
                None,
                None,
                http::StatusCode::UNSUPPORTED_MEDIA_TYPE,
            ),
            (
                Some("application/json"),
                Some("e30"),
                None,
                None,
                http::StatusCode::UNSUPPORTED_MEDIA_TYPE,
            ),
            (
                Some("application/octet-stream"),
                None,
                None,
                None,
                http::StatusCode::BAD_REQUEST,
            ),
            (
                Some("application/octet-stream"),
                Some("e30="),
                None,
                None,
                http::StatusCode::BAD_REQUEST,
            ),
        ])
        .await;
    }

    /// A `send` has no reply to replay and no range to serve, so both headers
    /// are refused rather than quietly ignored — and refused before the upload
    /// they would otherwise have consumed.
    #[tokio::test]
    async fn send_rejects_idempotency_and_range_before_body_poll() {
        refused_before_any_poll(&[
            (
                Some("application/octet-stream"),
                Some("e30"),
                Some(("idempotency-key", "x")),
                None,
                http::StatusCode::BAD_REQUEST,
            ),
            (
                Some("application/octet-stream"),
                Some("e30"),
                Some(("range", "bytes=0-1")),
                None,
                http::StatusCode::BAD_REQUEST,
            ),
            (
                Some("application/octet-stream"),
                Some("e30"),
                Some(("if-range", "\"etag\"")),
                None,
                http::StatusCode::BAD_REQUEST,
            ),
        ])
        .await;
    }

    /// A sender that announces more than the ceiling is refused on the
    /// announcement. Discovering it byte by byte would mean accepting the bytes.
    #[tokio::test]
    async fn send_rejects_declared_oversize_before_body_poll() {
        refused_before_any_poll(&[(
            Some("application/octet-stream"),
            Some("e30"),
            None,
            Some("5"),
            http::StatusCode::PAYLOAD_TOO_LARGE,
        )])
        .await;
    }

    #[tokio::test]
    async fn send_route_selects_only_enabled_send_methods() {
        let invoked = Arc::new(Mutex::new(Vec::new()));
        let gateway = Gateway::<SendService, _>::new(
            SendClient {
                invoked: Arc::clone(&invoked),
            },
            Denyless,
            Challenge::new(HeaderValue::from_static("Bearer realm=keylink"))
                .expect("valid challenge"),
        );
        let limits = crate::StreamingLimits::new(32, 32, 32, 1).expect("positive limits");
        let router = gateway
            .router::<()>(
                crate::HttpSurface::new("", crate::Addressing::Singleton)
                    .expect("valid surface")
                    .with_streaming(limits),
            )
            .expect("surface builds");
        let app = router.layer(axum::middleware::from_fn(
            |mut request: Request<Body>, next: axum::middleware::Next| async move {
                request
                    .extensions_mut()
                    .insert(Principal::new(b"tenant-a").expect("valid principal"));
                next.run(request).await
            },
        ));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener binds");
        let address = listener.local_addr().expect("listener address");
        let server = tokio::spawn(async move { axum::serve(listener, app).await });
        let client = reqwest::Client::new();

        let upload = client
            .post(format!("http://{address}/send/Upload"))
            .header("content-type", "application/octet-stream")
            .header("keylink-arguments", "e30")
            .body("bytes")
            .send()
            .await
            .expect("HTTP response");
        assert_eq!(upload.status(), http::StatusCode::OK);
        assert_eq!(
            *invoked.lock().expect("test mutex"),
            [keylink::ExternalKind::Send]
        );

        let wrong_kind = client
            .post(format!("http://{address}/send/Run"))
            .header("content-type", "application/octet-stream")
            .header("keylink-arguments", "e30")
            .body("bytes")
            .send()
            .await
            .expect("HTTP response");
        assert_eq!(wrong_kind.status(), http::StatusCode::NOT_FOUND);
        assert_eq!(invoked.lock().expect("test mutex").len(), 1);
        server.abort();
    }

    #[derive(Clone)]
    struct WrongOutputClient(Arc<Mutex<Option<keylink::BlobHandle>>>);

    struct WrongOutputService;

    impl ExternalService for WrongOutputService {
        const SERVICE: &'static str = "WrongOutput";
        const METHODS: &'static [keylink::ExternalMethod] = &[keylink::ExternalMethod::new(
            "Run",
            keylink::ExternalKind::Rpc,
        )];
        type Client = WrongOutputClient;

        fn invoke<C: keylink::ServiceCodec>(
            client: &Self::Client,
            _method: &str,
            _body: &[u8],
            _call: keylink::ExternalCall,
            codec: C,
        ) -> ExternalFuture<C> {
            let handle = client
                .0
                .lock()
                .expect("test mutex")
                .take()
                .expect("one call");
            Box::pin(async move {
                let value = codec.encode(&()).map_err(ExternalError::Codec)?;
                Ok(keylink::ExternalOutput::Blob { value, handle })
            })
        }
    }

    #[tokio::test]
    async fn unary_gateway_rejects_the_wrong_generated_output_variant() {
        let node = local_node().await;
        let blobs = keylink::Blobs::memory();
        let id = blobs
            .add_bytes(b"wrong-output".as_slice())
            .await
            .expect("blob stores");
        let client = WrongOutputClient(Arc::new(Mutex::new(Some(keylink::BlobHandle::new(
            &node,
            node.endpoint().addr(),
            id,
        )))));
        let gateway = Gateway::<WrongOutputService, _>::new(
            client,
            Denyless,
            Challenge::new(HeaderValue::from_static("Bearer realm=keylink"))
                .expect("valid challenge"),
        );
        let response = gateway
            .handle_rpc(
                request("/rpc/Run", Body::from("{}")),
                super::singleton_rpc_route(),
                super::SINGLETON_RPC_PATH,
            )
            .await;

        assert_eq!(response.status(), http::StatusCode::BAD_GATEWAY);
    }

    #[tokio::test]
    async fn singleton_routes_use_bounded_raw_parser() {
        let client = client();
        let polls = Arc::new(AtomicUsize::new(0));
        let response = gateway(client.clone(), PanicAccess)
            .handle_rpc(
                request(
                    "/rpc/%61%61%61%61",
                    Body::new(RecordingBody::new(Arc::clone(&polls))),
                ),
                super::singleton_rpc_route(),
                super::SINGLETON_RPC_PATH,
            )
            .await;

        // `%61%61%61%61` decodes to `aaaa`, longer than any served method
        // name. The refusal is `404` rather than `400` because a segment too
        // long to be any method names no method — and a `400` here reported
        // the length cap to a caller who had not authenticated.
        assert_eq!(response.status(), http::StatusCode::NOT_FOUND);
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        assert_eq!(client.invoked.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn unknown_method_never_polls_the_body_or_dispatches() {
        let client = client();
        let polls = Arc::new(AtomicUsize::new(0));
        let response = gateway(client.clone(), PanicAccess)
            .handle_rpc(
                request(
                    "/rpc/Bad",
                    Body::new(RecordingBody::new(Arc::clone(&polls))),
                ),
                super::singleton_rpc_route(),
                super::SINGLETON_RPC_PATH,
            )
            .await;

        assert_eq!(response.status(), http::StatusCode::NOT_FOUND);
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        assert_eq!(client.invoked.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn denied_method_never_polls_the_body_or_dispatches() {
        let client = client();
        let polls = Arc::new(AtomicUsize::new(0));
        let response = gateway(client.clone(), Deny)
            .handle_rpc(
                request(
                    "/rpc/Run",
                    Body::new(RecordingBody::new(Arc::clone(&polls))),
                ),
                super::singleton_rpc_route(),
                super::SINGLETON_RPC_PATH,
            )
            .await;

        assert_eq!(response.status(), http::StatusCode::FORBIDDEN);
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        assert_eq!(client.invoked.load(Ordering::SeqCst), 0);
    }

    /// Watched red with `scripts/break-once.sh` by replacing
    /// `    if values.next().is_some() {
    ///         return false;
    ///     }` with an always-false branch.
    /// The exact test terminated with HTTP 200 instead of 415.
    #[tokio::test]
    async fn duplicate_content_types_are_rejected() {
        let client = ready_client();
        let mut request = request("/rpc/Run", Body::from("{}"));
        request.headers_mut().remove(http::header::CONTENT_TYPE);
        request.headers_mut().append(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        request.headers_mut().append(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("text/plain"),
        );

        let response = gateway(client.clone(), Denyless)
            .handle_rpc(
                request,
                super::singleton_rpc_route(),
                super::SINGLETON_RPC_PATH,
            )
            .await;
        assert_eq!(response.status(), http::StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert_eq!(client.invoked.load(Ordering::SeqCst), 0);
    }

    /// Watched red with `scripts/break-once.sh` by replacing
    /// `    let Ok(media) = value.parse::<mime::Mime>() else {
    ///         return false;
    ///     };` with parsing of only the value before the first semicolon.
    /// The exact test terminated with HTTP 200 instead of 415.
    #[tokio::test]
    async fn malformed_content_types_are_rejected() {
        for value in ["application/json; charset", "application/json; =utf-8"] {
            let client = ready_client();
            let mut request = request("/rpc/Run", Body::from("{}"));
            request
                .headers_mut()
                .insert(http::header::CONTENT_TYPE, HeaderValue::from_static(value));

            let response = gateway(client.clone(), Denyless)
                .handle_rpc(
                    request,
                    super::singleton_rpc_route(),
                    super::SINGLETON_RPC_PATH,
                )
                .await;
            assert_eq!(
                response.status(),
                http::StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "{value}"
            );
            assert_eq!(client.invoked.load(Ordering::SeqCst), 0, "{value}");
        }
    }

    /// Watched red with `scripts/break-once.sh` by replacing
    /// `Err(_) => Err(GatewayError::InvalidRequest),`
    /// with `RequestTooLarge`. The exact test terminated with HTTP 413 instead
    /// of 400.
    #[tokio::test]
    async fn short_body_collection_failure_is_not_reported_as_too_large() {
        let client = client();
        let response = gateway(client.clone(), Denyless)
            .handle_rpc(
                request("/rpc/Run", Body::new(ErrorBody)),
                super::singleton_rpc_route(),
                super::SINGLETON_RPC_PATH,
            )
            .await;
        assert_eq!(response.status(), http::StatusCode::BAD_REQUEST);
        assert_eq!(client.invoked.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn opaque_idempotency_header_is_rejected_before_body_poll_or_dispatch() {
        let client = client();
        let polls = Arc::new(AtomicUsize::new(0));
        let mut request = request(
            "/rpc/Run",
            Body::new(RecordingBody::new(Arc::clone(&polls))),
        );
        request.headers_mut().insert(
            "idempotency-key",
            HeaderValue::from_bytes(&[0xff]).expect("opaque header value"),
        );

        let response = gateway(client.clone(), Denyless)
            .handle_rpc(
                request,
                super::singleton_rpc_route(),
                super::SINGLETON_RPC_PATH,
            )
            .await;

        assert_eq!(response.status(), http::StatusCode::BAD_REQUEST);
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        assert_eq!(client.invoked.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn gateway_forwards_the_complete_verified_call_to_access() {
        let client = client();
        let captured = Arc::new(Mutex::new(None));
        let polls = Arc::new(AtomicUsize::new(0));
        let mut request = request(
            "/rpc/Run",
            Body::new(RecordingBody::new(Arc::clone(&polls))),
        );
        request
            .headers_mut()
            .insert("x-capture", HeaderValue::from_static("header-value"));
        request.extensions_mut().insert(7_u64);

        let response = gateway(client.clone(), Capture(Arc::clone(&captured)))
            .handle_rpc(
                request,
                super::singleton_rpc_route(),
                super::SINGLETON_RPC_PATH,
            )
            .await;

        assert_eq!(response.status(), http::StatusCode::FORBIDDEN);
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        assert_eq!(client.invoked.load(Ordering::SeqCst), 0);
        assert_eq!(
            captured.lock().expect("capture mutex").take(),
            Some(Captured {
                extension: Some(7),
                header: Some(HeaderValue::from_static("header-value")),
                principal: Principal::new(b"tenant-a").expect("valid principal"),
                service: "CapturedService",
                method: "Run",
            })
        );
    }

    #[test]
    fn concurrent_handlers_poll_independent_invocations_without_waiting() {
        let client = client();
        let gateway = gateway(client.clone(), Denyless);
        let mut first = Box::pin(gateway.handle_rpc(
            request("/rpc/Run", Body::from("{}")),
            super::singleton_rpc_route(),
            super::SINGLETON_RPC_PATH,
        ));
        let mut second = Box::pin(gateway.handle_rpc(
            request("/rpc/Run", Body::from("{}")),
            super::singleton_rpc_route(),
            super::SINGLETON_RPC_PATH,
        ));
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);

        assert!(Future::poll(first.as_mut(), &mut context).is_pending());
        assert!(Future::poll(second.as_mut(), &mut context).is_pending());
        assert_eq!(client.invoked.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn dropping_handler_drops_the_adapter_owned_invocation_future() {
        let client = client();
        let gateway = gateway(client.clone(), Denyless);
        let mut handler = Box::pin(gateway.handle_rpc(
            request("/rpc/Run", Body::from("{}")),
            super::singleton_rpc_route(),
            super::SINGLETON_RPC_PATH,
        ));
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);

        assert!(Future::poll(handler.as_mut(), &mut context).is_pending());
        assert_eq!(client.invoked.load(Ordering::SeqCst), 1);
        drop(handler);
        assert!(client.dropped.load(Ordering::SeqCst));
    }

    keylink::service! {
        service Cancellation;
        rpc Wait(token: u64) -> u64;
        send Upload(prefix: u64) -> u64;
        recv Stream(polls: u64) -> u64;
    }

    /// What a `recv` source tells a test: how much it has handed over, and
    /// when it was dropped.
    ///
    /// Both live on the *serving* side on purpose. A gateway's `BodyReader` has
    /// no public constructor and cannot be wrapped, so the honest place to watch
    /// the chain is the far end of it — which is also where a regression would
    /// keep producing bytes nobody asked for.
    #[derive(Clone)]
    struct DownloadProbe {
        produced: Arc<AtomicU64>,
        dropped: tokio::sync::watch::Sender<bool>,
        /// When present, the source fails instead of ending — but only once the
        /// test releases it. The wait is what keeps the failure from racing the
        /// response header a stream reset would otherwise discard.
        fail: Arc<Mutex<Option<tokio::sync::oneshot::Receiver<()>>>>,
    }

    /// A source that counts what it hands over and reports its own drop.
    struct CountingSource {
        polls: u64,
        chunk_len: usize,
        probe: DownloadProbe,
        fail: Option<tokio::sync::oneshot::Receiver<()>>,
    }

    impl tokio::io::AsyncRead for CountingSource {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            if self.polls == 0 {
                let Some(fail) = self.fail.as_mut() else {
                    return Poll::Ready(Ok(()));
                };
                return match Pin::new(fail).poll(_cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(_) => Poll::Ready(Err(std::io::Error::other("source failed"))),
                };
            }
            let len = self.chunk_len.min(buf.remaining());
            buf.put_slice(&vec![b'd'; len]);
            self.polls -= 1;
            self.probe.produced.fetch_add(len as u64, Ordering::SeqCst);
            Poll::Ready(Ok(()))
        }
    }

    impl Drop for CountingSource {
        fn drop(&mut self) {
            let _ = self.probe.dropped.send(true);
        }
    }

    /// The serving endpoint's per-stream receive window for the backpressure
    /// test. Pinned rather than inherited, so the bound below is measured
    /// against a number this file owns instead of a dependency's default.
    const RECEIVE_WINDOW: u32 = 16 * 1024;

    /// An upload the gateway polls directly, counting what it hands over.
    ///
    /// Directly is the point. Behind a real socket this same measurement reads
    /// the kernel's buffers rather than the adapter's behaviour, and a megabyte
    /// disappears into them before Keylink is involved at all.
    struct CountingBody {
        chunks: u64,
        chunk_len: usize,
        produced: Arc<AtomicU64>,
        first: Option<tokio::sync::oneshot::Sender<()>>,
    }

    impl HttpBody for CountingBody {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            if self.chunks == 0 {
                return Poll::Ready(None);
            }
            self.chunks -= 1;
            let chunk_len = self.chunk_len;
            self.produced.fetch_add(chunk_len as u64, Ordering::SeqCst);
            if let Some(first) = self.first.take() {
                let _ = first.send(());
            }
            Poll::Ready(Some(Ok(Frame::data(Bytes::from(vec![b'k'; chunk_len])))))
        }
    }

    /// The HTTP body is pulled by the Keylink writer, not drained into it. While
    /// the receiver reads nothing its flow-control window closes, the gateway's
    /// write parks, and the body stops being polled — so peak memory is a
    /// bounded amount of transport buffer however long the upload is.
    ///
    /// # Why this counts bytes rather than polls
    ///
    /// The first shape of this test asserted "no second poll after the first"
    /// and was wrong: keylink's frame reader pulls a whole transport read to
    /// find the request header, so a receiver legitimately absorbs more than one
    /// chunk before the handler asks for anything. The window is not the only
    /// slack, and pinning the assertion to one chunk pinned it to an accident.
    ///
    /// So the claim is measured as what it is: an upper bound on what a
    /// *non-reading* receiver can pull through. Half the offered payload is
    /// eight times the slack expected here and half of what an adapter that
    /// drained the body would show, so nothing in between is a near miss. Load
    /// can only make the body produce less, so the bound cannot redden under
    /// load — only a real regression moves it.
    ///
    /// Watched red with `scripts/break-once.sh` (row `HJ11`) by replacing
    /// `tokio::io::copy_buf(&mut reader, stream)
    /// .await
    /// .map(drop)
    /// .map_err(Error::from)`
    /// in `keylink/src/body.rs` with one that drains the reader into a `Vec`
    /// and writes that afterwards, which is the regression this bound exists
    /// for. The exact test terminated with `pulled 1048576 of 1048576 offered
    /// bytes` — every byte pulled through, with the receiver reading none.
    #[tokio::test]
    async fn send_backpressure_stops_http_body_polling() {
        let chunks = 64_u64;
        let offered = u64::from(RECEIVE_WINDOW) * chunks;
        let limits = crate::StreamingLimits::new(32, offered, 64, 1).expect("positive limits");
        let route = crate::HttpSurface::new("", crate::Addressing::Singleton)
            .expect("valid surface")
            .with_streaming(limits)
            .routes()
            .find(|route| route.kind() == crate::__contract::RouteKind::Send)
            .expect("send route");

        let server_node = Node::from_endpoint(
            Endpoint::builder(presets::Minimal)
                .transport_config(
                    iroh::endpoint::QuicTransportConfig::builder()
                        .stream_receive_window(RECEIVE_WINDOW.into())
                        .build(),
                )
                .bind()
                .await
                .expect("a local endpoint binds"),
        );
        let client_node = local_node().await;
        let (entered_tx, mut entered_rx) = tokio::sync::mpsc::unbounded_channel();
        let release = Arc::new(tokio::sync::Notify::new());
        let _keylink = cancellation::serve(
            &server_node,
            CancellationHandler {
                entered: entered_tx,
                release: Arc::clone(&release),
                download: None,
            },
            Allowlist::anyone(),
        )
        .expect("service starts");
        let client = cancellation::Client::connect(&client_node, server_node.endpoint().addr())
            .await
            .expect("client connects");
        let gateway = Gateway::<cancellation::External, _>::new(
            client,
            Denyless,
            Challenge::new(HeaderValue::from_static("Bearer realm=keylink"))
                .expect("valid challenge"),
        );

        let produced = Arc::new(AtomicU64::new(0));
        let (first_tx, first) = tokio::sync::oneshot::channel();
        let request = Request::builder()
            .method("POST")
            .uri("/send/Upload")
            .header("content-type", "application/octet-stream")
            .header("keylink-arguments", {
                use base64::Engine as _;

                base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(r#"{"prefix":3}"#)
            })
            .extension(Principal::new(b"tenant-a").expect("valid principal"))
            .body(Body::new(CountingBody {
                chunks,
                chunk_len: RECEIVE_WINDOW as usize,
                produced: Arc::clone(&produced),
                first: Some(first_tx),
            }))
            .expect("request");

        let responding = gateway.handle_send(request, route, "/send/{method}", limits);
        let watching = async {
            entered_rx.recv().await.expect("the handler is dispatched");
            first.await.expect("the body is polled at least once");
            tokio::task::yield_now().await;
            let stalled = produced.load(Ordering::SeqCst);
            assert!(
                stalled <= offered / 2,
                "a receiver that reads nothing pulled {stalled} of {offered} offered bytes"
            );
            release.notify_waiters();
            stalled
        };
        let (response, stalled) = tokio::join!(responding, watching);

        assert_eq!(response.status(), http::StatusCode::OK);
        assert_eq!(
            produced.load(Ordering::SeqCst),
            offered,
            "and once the receiver reads, the whole upload still arrives"
        );
        assert!(
            stalled < offered,
            "a reader that resumes is what let the rest through"
        );
    }

    /// A download the test drives itself, so a `recv` gateway can be built
    /// without a listener between it and the assertions.
    async fn recv_gateway(
        probe: DownloadProbe,
    ) -> (
        Gateway<cancellation::External, Denyless>,
        keylink::Server,
        Vec<Node>,
        // Held by the caller only so the handler's reporting channel outlives
        // the gateway. Dropping it here would turn an unrelated handler call
        // into a panic on a closed receiver.
        tokio::sync::mpsc::UnboundedReceiver<()>,
    ) {
        let server_node = local_node().await;
        // The gateway RECEIVES here, so the window that throttles the sender is
        // the one this side advertises.
        let client_node = Node::from_endpoint(
            Endpoint::builder(presets::Minimal)
                .transport_config(
                    iroh::endpoint::QuicTransportConfig::builder()
                        .stream_receive_window(RECEIVE_WINDOW.into())
                        .build(),
                )
                .bind()
                .await
                .expect("a local endpoint binds"),
        );
        let (entered_tx, entered_rx) = tokio::sync::mpsc::unbounded_channel();
        let server = cancellation::serve(
            &server_node,
            CancellationHandler {
                entered: entered_tx,
                release: Arc::new(tokio::sync::Notify::new()),
                download: Some(probe),
            },
            Allowlist::anyone(),
        )
        .expect("service starts");
        let client = cancellation::Client::connect(&client_node, server_node.endpoint().addr())
            .await
            .expect("client connects");
        let gateway = Gateway::<cancellation::External, _>::new(
            client,
            Denyless,
            Challenge::new(HeaderValue::from_static("Bearer realm=keylink"))
                .expect("valid challenge"),
        )
        .limits(crate::JsonLimits::new(64, 64).expect("positive JSON limits"));
        (gateway, server, vec![server_node, client_node], entered_rx)
    }

    fn stream_request(polls: u64) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri("/recv/Stream")
            .header("content-type", "application/json")
            .extension(Principal::new(b"tenant-a").expect("valid principal"))
            .body(Body::from(format!(r#"{{"polls":{polls}}}"#)))
            .expect("request")
    }

    fn recv_route(limits: crate::StreamingLimits) -> &'static crate::__contract::Route {
        crate::HttpSurface::new("", crate::Addressing::Singleton)
            .expect("valid surface")
            .with_streaming(limits)
            .routes()
            .find(|route| route.kind() == crate::__contract::RouteKind::Recv)
            .expect("recv route")
    }

    /// The response body pulls the Keylink reader, and nothing pulls it while
    /// the HTTP side is not reading. A client that stops consuming closes the
    /// flow-control window and the far end stops producing.
    ///
    /// Measured as bytes rather than polls, and against a body this test polls
    /// itself rather than one behind a socket, for the reasons written up on
    /// `send_backpressure_stops_http_body_polling`. Both traps apply here in
    /// mirror image.
    #[tokio::test]
    async fn recv_backpressure_stops_keylink_reader_polling() {
        let polls = 64_u64;
        let offered = u64::from(RECEIVE_WINDOW) * polls;
        let limits = crate::StreamingLimits::new(64, 64, offered, 1).expect("positive limits");
        let produced = Arc::new(AtomicU64::new(0));
        let (dropped, _dropped_rx) = tokio::sync::watch::channel(false);
        let probe = DownloadProbe {
            produced: Arc::clone(&produced),
            dropped,
            fail: Arc::new(Mutex::new(None)),
        };
        let (gateway, _server, _nodes, _entered) = recv_gateway(probe).await;

        let response = gateway
            .handle_recv(
                stream_request(polls),
                recv_route(limits),
                "/recv/{method}",
                limits,
            )
            .await;
        assert_eq!(response.status(), http::StatusCode::OK);

        let mut body = response.into_body();
        let first = http_body_util::BodyExt::frame(&mut body)
            .await
            .expect("a first frame")
            .expect("a readable frame");
        assert!(first.into_data().is_ok(), "the first frame carries bytes");
        tokio::task::yield_now().await;
        let stalled = produced.load(Ordering::SeqCst);
        assert!(
            stalled <= offered / 2,
            "an unread response pulled {stalled} of {offered} offered bytes"
        );

        let rest = http_body_util::BodyExt::collect(body)
            .await
            .expect("the rest of the body")
            .to_bytes();
        assert_eq!(
            produced.load(Ordering::SeqCst),
            offered,
            "and once the client reads, the whole download still arrives"
        );
        assert!(!rest.is_empty());
    }

    /// A reply that fails mid-stream with no declared length must break the
    /// HTTP body. Nothing else can catch this one.
    ///
    /// This is the guard `recv_propagates_truncation_as_body_error` cannot be.
    /// That case declares a length, so hyper's own `Content-Length` check
    /// rejects the short body whether or not the adapter noticed — measured:
    /// swallowing the reader error there still leaves that test green. Without
    /// a declared length there is no second check, and a swallowed error
    /// becomes a clean, short download that every layer calls a success.
    ///
    /// The failure is released only after the first frame, because a stream
    /// reset discards what the peer has not read and would otherwise destroy
    /// the response header instead of the body.
    ///
    /// Watched red with `scripts/break-once.sh` (row `HJ12`) by replacing
    /// `Err(_) => Some((
    /// Err(std::io::Error::other("the response stream failed")),
    /// Download::Ended,
    /// )),`
    /// in `keylink-http/src/stream.rs` with `Err(_) => None`, which ends the
    /// body cleanly instead of breaking it. The same break leaves
    /// `recv_propagates_truncation_as_body_error` GREEN, which is why this row
    /// exists.
    #[tokio::test]
    async fn recv_late_source_error_breaks_the_undeclared_body() {
        let polls = 1_u64;
        let limits = crate::StreamingLimits::new(64, 64, u64::from(RECEIVE_WINDOW) * 8, 1)
            .expect("positive limits");
        let (dropped, _dropped_rx) = tokio::sync::watch::channel(false);
        let (release, released) = tokio::sync::oneshot::channel();
        let probe = DownloadProbe {
            produced: Arc::new(AtomicU64::new(0)),
            dropped,
            fail: Arc::new(Mutex::new(Some(released))),
        };
        let (gateway, _server, _nodes, _entered) = recv_gateway(probe).await;

        let response = gateway
            .handle_recv(
                stream_request(polls),
                recv_route(limits),
                "/recv/{method}",
                limits,
            )
            .await;
        assert_eq!(response.status(), http::StatusCode::OK);
        assert!(
            response
                .headers()
                .get(http::header::CONTENT_LENGTH)
                .is_none(),
            "nothing declared a length, so nothing else can check one"
        );

        let mut body = response.into_body();
        http_body_util::BodyExt::frame(&mut body)
            .await
            .expect("a first frame")
            .expect("a readable frame");
        release.send(()).expect("the source is still waiting");

        assert!(
            http_body_util::BodyExt::collect(body).await.is_err(),
            "a reply whose source failed must break, not end early and cleanly"
        );
    }

    /// Dropping the response drops the Keylink reader it owns, and that reaches
    /// the far end: the serving source is dropped rather than left producing
    /// bytes for a client that has gone.
    #[tokio::test]
    async fn recv_disconnect_drops_body_reader() {
        let polls = 64_u64;
        let limits = crate::StreamingLimits::new(64, 64, u64::from(RECEIVE_WINDOW) * polls, 1)
            .expect("positive limits");
        let (dropped, mut dropped_rx) = tokio::sync::watch::channel(false);
        let probe = DownloadProbe {
            produced: Arc::new(AtomicU64::new(0)),
            dropped,
            fail: Arc::new(Mutex::new(None)),
        };
        let (gateway, _server, _nodes, _entered) = recv_gateway(probe).await;

        let response = gateway
            .handle_recv(
                stream_request(polls),
                recv_route(limits),
                "/recv/{method}",
                limits,
            )
            .await;
        assert_eq!(response.status(), http::StatusCode::OK);

        let mut body = response.into_body();
        http_body_util::BodyExt::frame(&mut body)
            .await
            .expect("a first frame")
            .expect("a readable frame");
        drop(body);

        dropped_rx
            .changed()
            .await
            .expect("the serving source reports its drop");
        assert!(*dropped_rx.borrow());
    }

    struct CancellationHandler {
        entered: tokio::sync::mpsc::UnboundedSender<()>,
        release: Arc<tokio::sync::Notify>,
        download: Option<DownloadProbe>,
    }

    impl cancellation::Handler for CancellationHandler {
        async fn wait(&self, _cx: &KeylinkContext, token: u64) -> u64 {
            self.entered.send(()).expect("test receiver stays open");
            self.release.notified().await;
            token
        }

        /// Reports that it holds the request, then reads nothing until released.
        /// That gap is the window the backpressure test measures across.
        async fn upload(
            &self,
            _cx: &KeylinkContext,
            prefix: u64,
            body: keylink::BodyReader,
        ) -> u64 {
            self.entered.send(()).expect("test receiver stays open");
            self.release.notified().await;
            prefix + body.drain().await.expect("the upload arrives whole")
        }

        async fn stream(&self, _cx: &KeylinkContext, polls: u64) -> (u64, keylink::Body) {
            let probe = self.download.clone().expect("this test set a probe");
            let fail = probe.fail.lock().expect("test mutex").take();
            (
                polls,
                keylink::Body::from_reader(
                    CountingSource {
                        polls,
                        chunk_len: RECEIVE_WINDOW as usize,
                        probe,
                        fail,
                    },
                    None,
                ),
            )
        }
    }

    #[derive(Clone)]
    struct TrackedClient {
        inner: cancellation::Client,
        dropped: tokio::sync::watch::Sender<bool>,
    }

    struct Tracked;

    impl ExternalService for Tracked {
        const SERVICE: &'static str = <cancellation::External as ExternalService>::SERVICE;
        const METHODS: &'static [keylink::ExternalMethod] =
            <cancellation::External as ExternalService>::METHODS;
        type Client = TrackedClient;

        fn invoke<C: keylink::ServiceCodec>(
            client: &Self::Client,
            method: &str,
            body: &[u8],
            call: keylink::ExternalCall,
            codec: C,
        ) -> ExternalFuture<C> {
            let future = cancellation::External::invoke(&client.inner, method, body, call, codec);
            let dropped = client.dropped.clone();
            Box::pin(async move {
                struct SignalDrop {
                    signal: tokio::sync::watch::Sender<bool>,
                }
                impl Drop for SignalDrop {
                    fn drop(&mut self) {
                        let _ = self.signal.send(true);
                    }
                }
                let _signal = SignalDrop { signal: dropped };
                future.await
            })
        }
    }

    async fn local_node() -> Node {
        Node::from_endpoint(
            Endpoint::bind(presets::Minimal)
                .await
                .expect("a local endpoint binds"),
        )
    }

    /// A client that hangs up drops the invocation the adapter owns.
    ///
    /// Two guards, and they answer different questions. The runtime half below
    /// hangs up a raw socket mid-call and watches the generated client's future
    /// get dropped. The source half above is what keeps that true: it reads
    /// this file's own text and refuses the two shapes that would detach the
    /// future from the request — storing it in a binding, and spawning it.
    ///
    /// A TEXT guard, and knowingly so: it can be defeated by a spelling it does
    /// not enumerate. What it buys is that the two ways this has actually been
    /// written wrong both fail here rather than in production.
    ///
    /// Watched red with `scripts/break-once.sh` (row `HK34`) by replacing
    /// `keylink::ExternalCall::Blob,`
    /// with `{ let call = keylink::ExternalCall::Blob; call },`, which is
    /// behaviour-identical and puts a `;` between the invocation and its
    /// `.await`. The await-shape assertion fired.
    ///
    /// And (row `HK35`) by replacing
    /// `// One template, two verbs, and therefore ONE registration:`
    /// with a comment naming `tokio::spawn(`. The spawn ban fired.
    #[tokio::test]
    async fn tcp_disconnect_drops_the_adapter_owned_invoke_future() {
        let source = include_str!("gateway.rs");
        // Split on the test MODULE, not on the attribute: several test-only
        // items sit above it, and splitting on `#[cfg(test)]` alone would cut
        // production off at the first one and leave nothing to inspect.
        let production = source
            .split("#[cfg(test)]\nmod tests")
            .next()
            .expect("production precedes the test module");
        let calls = production.split("(surface.invoke)(").skip(1).count();
        assert!(
            calls > 0,
            "the handlers reach the client through the surface"
        );
        for tail in production.split("(surface.invoke)(").skip(1) {
            // Awaited in the same expression it is made in. A future that is
            // bound to a name can be detached later; one that is awaited where
            // it is built is owned by the request, and a client that hangs up
            // drops it.
            let (arguments, _) = tail
                .split_once(".await")
                .expect("an invocation is awaited, not stored");
            assert!(
                !arguments.contains(';'),
                "an invocation was made without being awaited in the same expression"
            );
        }
        assert!(!production.contains("tokio::spawn("));

        let server_node = local_node().await;
        let client_node = local_node().await;
        let (entered_tx, mut entered_rx) = tokio::sync::mpsc::unbounded_channel();
        let release = Arc::new(tokio::sync::Notify::new());
        let _keylink = cancellation::serve(
            &server_node,
            CancellationHandler {
                entered: entered_tx,
                release: Arc::clone(&release),
                download: None,
            },
            Allowlist::anyone(),
        )
        .expect("service starts");
        let client = cancellation::Client::connect(&client_node, server_node.endpoint().addr())
            .await
            .expect("client connects");
        let (dropped_tx, mut dropped_rx) = tokio::sync::watch::channel(false);
        let gateway = Gateway::<Tracked, _>::new(
            TrackedClient {
                inner: client,
                dropped: dropped_tx,
            },
            Denyless,
            Challenge::new(HeaderValue::from_static("Bearer realm=keylink"))
                .expect("valid challenge"),
        )
        .router::<()>(
            crate::HttpSurface::new("/v1", crate::Addressing::Singleton)
                .expect("valid singleton surface"),
        )
        .expect("surface matches singleton gateway");
        let app: Router<()> = Router::new()
            .merge(gateway)
            .layer(axum::middleware::from_fn(
                |mut request: Request<Body>, next: axum::middleware::Next| async move {
                    request
                        .extensions_mut()
                        .insert(Principal::new(b"tenant-a").expect("valid principal"));
                    next.run(request).await
                },
            ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener binds");
        let address = listener.local_addr().expect("listener address");
        let server = tokio::spawn(async move { axum::serve(listener, app).await });

        let mut socket = tokio::net::TcpStream::connect(address)
            .await
            .expect("raw client connects");
        socket
            .write_all(
                b"POST /v1/rpc/Wait HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 11\r\n\r\n{\"token\":7}",
            )
            .await
            .expect("request writes");
        entered_rx.recv().await.expect("backend call starts");
        drop(socket);

        dropped_rx
            .changed()
            .await
            .expect("invoke future drop is reported");
        assert!(*dropped_rx.borrow());
        release.notify_waiters();
        server.abort();
    }

    #[derive(Clone)]
    struct Denyless;

    impl Access for Denyless {
        async fn authorize(&self, _call: Call<'_>) -> Result<(), AccessError> {
            Ok(())
        }
    }
}
