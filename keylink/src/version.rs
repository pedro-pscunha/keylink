//! Contract windows and the conversions between their frozen wire shapes.

use std::{borrow::Cow, fmt, marker::PhantomData, sync::Arc};

use bytes::Bytes;
use iroh::{
    endpoint::Connection,
    protocol::{AcceptError, ProtocolHandler},
};
use postcard_schema::Schema;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    wire::{self, FrameReader},
    Addressed, Admission, Envelope, Error, Idempotency, Outcome, RemoteError, Result, WireRequest,
    WireResponse,
};

/// One declared label and the hash of the wire shape behind it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Schema)]
pub struct Version {
    label: u32,
    hash: u64,
}
impl Version {
    #[doc(hidden)]
    pub const fn __new(label: u32, hash: u64) -> Self {
        Self { label, hash }
    }
    /// The service's declared version label.
    pub const fn label(&self) -> u32 {
        self.label
    }
    /// The hash of this version's wire contract.
    pub const fn hash(&self) -> u64 {
        self.hash
    }
    fn alpn(self, protocol: &str) -> Vec<u8> {
        format!("{protocol}/{}/{:016x}", self.label, self.hash).into_bytes()
    }
}
impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}#{:016x}", self.label, self.hash)
    }
}

/// The versions one side speaks for a service, newest first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Window {
    service: String,
    versions: Vec<Version>,
}
impl Window {
    /// The protocol name, including `/keyed` for an addressed service.
    pub fn service(&self) -> &str {
        &self.service
    }
    /// The supported versions, newest first.
    pub fn versions(&self) -> &[Version] {
        &self.versions
    }
}
impl fmt::Display for Window {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/", self.service)?;
        match self.versions.as_slice() {
            [one] => write!(f, "{}", one.label),
            [first, .., last]
                if self
                    .versions
                    .windows(2)
                    .all(|v| v[0].label.checked_sub(1) == Some(v[1].label)) =>
            {
                write!(f, "{}..={}", last.label, first.label)
            }
            _ => {
                write!(f, "{{")?;
                for (i, v) in self.versions.iter().rev().enumerate() {
                    if i != 0 {
                        write!(f, ",")?;
                    }
                    write!(f, "{}", v.label)?;
                }
                write!(f, "}}")
            }
        }
    }
}

/// What a peer disclosed when no contract matched.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum PeerWindow {
    /// An admitted peer answered the service's version probe.
    Speaks(Window),
    /// The peer did not serve or did not admit this service's probe.
    Unknown,
}

/// Both sides of a refused contract negotiation.
#[derive(Debug, Clone)]
pub struct Mismatch {
    ours: Window,
    theirs: PeerWindow,
}
impl Mismatch {
    pub(crate) fn new(ours: Window, theirs: PeerWindow) -> Self {
        Self { ours, theirs }
    }
    /// The versions this node offered.
    pub fn ours(&self) -> &Window {
        &self.ours
    }
    /// What this node learned about the peer's versions.
    pub fn theirs(&self) -> &PeerWindow {
        &self.theirs
    }
}
impl fmt::Display for Mismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "no common contract version: this node speaks {}, ",
            self.ours
        )?;
        match &self.theirs {
            PeerWindow::Unknown => write!(f, "the peer named no version of {} (it does not serve it, or did not admit this node)", self.ours.service),
            PeerWindow::Speaks(theirs) => {
                write!(f, "the peer speaks {theirs}")?;
                for ours in &self.ours.versions {
                    for theirs in &theirs.versions {
                        if ours.label == theirs.label && ours.hash != theirs.hash {
                            write!(f, "; both claim {} with different shapes (ours {:016x}, theirs {:016x})", ours.label, ours.hash, theirs.hash)?;
                        }
                    }
                }
                Ok(())
            }
        }
    }
}

/// A value one contract version cannot express.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{method}: {reason}")]
pub struct Unsupported {
    method: &'static str,
    reason: Cow<'static, str>,
}
impl Unsupported {
    /// Explain why a method cannot cross this version step.
    pub fn new(method: &'static str, reason: impl Into<Cow<'static, str>>) -> Self {
        Self {
            method,
            reason: reason.into(),
        }
    }
    /// The method that could not be carried.
    pub fn method(&self) -> &'static str {
        self.method
    }
    /// Why the conversion refused it.
    pub fn reason(&self) -> &str {
        &self.reason
    }
}

#[doc(hidden)]
pub trait Hop: 'static {
    type Request: Serialize + DeserializeOwned + Send + Sync + 'static;
    type Response: Serialize + DeserializeOwned + Send + Sync + 'static;
    type NewerRequest;
    type NewerResponse;
    fn upgrade_request(old: Self::Request) -> std::result::Result<Self::NewerRequest, Unsupported>;
    fn downgrade_request(
        new: Self::NewerRequest,
    ) -> std::result::Result<Self::Request, Unsupported>;
    fn upgrade_response(
        old: Self::Response,
    ) -> std::result::Result<Self::NewerResponse, Unsupported>;
    fn downgrade_response(
        new: Self::NewerResponse,
    ) -> std::result::Result<Self::Response, Unsupported>;
}
#[doc(hidden)]
pub trait Toward<Req, Resp>: 'static {
    type Request: Serialize + DeserializeOwned + Send + Sync + 'static;
    type Response: Serialize + DeserializeOwned + Send + Sync + 'static;
    const CURRENT: bool = false;
    fn up_request(old: Self::Request) -> std::result::Result<Req, Unsupported>;
    fn down_request(new: Req) -> std::result::Result<Self::Request, Unsupported>;
    fn up_response(old: Self::Response) -> std::result::Result<Resp, Unsupported>;
    fn down_response(new: Resp) -> std::result::Result<Self::Response, Unsupported>;
}
#[doc(hidden)]
pub struct Here<Req, Resp>(PhantomData<fn(Req) -> Resp>);
impl<Req, Resp> Toward<Req, Resp> for Here<Req, Resp>
where
    Req: Serialize + DeserializeOwned + Send + Sync + 'static,
    Resp: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    type Request = Req;
    type Response = Resp;
    const CURRENT: bool = true;
    fn up_request(r: Req) -> std::result::Result<Req, Unsupported> {
        Ok(r)
    }
    fn down_request(r: Req) -> std::result::Result<Req, Unsupported> {
        Ok(r)
    }
    fn up_response(r: Resp) -> std::result::Result<Resp, Unsupported> {
        Ok(r)
    }
    fn down_response(r: Resp) -> std::result::Result<Resp, Unsupported> {
        Ok(r)
    }
}
impl<Req: 'static, Resp: 'static, H: Hop, N> Toward<Req, Resp> for (H, N)
where
    N: Toward<Req, Resp, Request = H::NewerRequest, Response = H::NewerResponse>,
{
    type Request = H::Request;
    type Response = H::Response;
    fn up_request(r: Self::Request) -> std::result::Result<Req, Unsupported> {
        N::up_request(H::upgrade_request(r)?)
    }
    fn down_request(r: Req) -> std::result::Result<Self::Request, Unsupported> {
        H::downgrade_request(N::down_request(r)?)
    }
    fn up_response(r: Self::Response) -> std::result::Result<Resp, Unsupported> {
        N::up_response(H::upgrade_response(r)?)
    }
    fn down_response(r: Resp) -> std::result::Result<Self::Response, Unsupported> {
        H::downgrade_response(N::down_response(r)?)
    }
}
#[doc(hidden)]
pub struct Keyed<P>(PhantomData<P>);
impl<Req: 'static, Resp: 'static, P: Toward<Req, Resp>> Toward<Addressed<Req>, Resp> for Keyed<P> {
    type Request = Addressed<P::Request>;
    type Response = P::Response;
    const CURRENT: bool = P::CURRENT;
    fn up_request(r: Self::Request) -> std::result::Result<Addressed<Req>, Unsupported> {
        let (id, r) = r.split();
        Ok(Addressed::new(id, P::up_request(r)?))
    }
    fn down_request(r: Addressed<Req>) -> std::result::Result<Self::Request, Unsupported> {
        let (id, r) = r.split();
        Ok(Addressed::new(id, P::down_request(r)?))
    }
    fn up_response(r: Self::Response) -> std::result::Result<Resp, Unsupported> {
        P::up_response(r)
    }
    fn down_response(r: Resp) -> std::result::Result<Self::Response, Unsupported> {
        P::down_response(r)
    }
}

pub(crate) struct EncodedReply {
    pub(crate) frame: Bytes,
    pub(crate) carry_body: bool,
    pub(crate) grant: Option<crate::BlobId>,
}

pub(crate) trait Codec<Req, Resp>: Send + Sync + fmt::Debug {
    fn encode_request(
        &self,
        body_len: Option<u64>,
        idem: Option<&Idempotency>,
        r: WireRequest<Req>,
    ) -> Result<Bytes>;
    fn decode_response(&self, bytes: &[u8]) -> Result<Envelope<WireResponse<Resp>>>;
    fn decode_request(
        &self,
        bytes: &[u8],
    ) -> Result<std::result::Result<Envelope<WireRequest<Req>>, RemoteError>>;
    /// The encoded header and whether the handler's body can follow it.
    fn reencode_response(
        &self,
        frame: Bytes,
        current_grant: Option<crate::BlobId>,
    ) -> Result<EncodedReply>;
    #[cfg(feature = "persist")]
    fn upgrade_recorded(&self, frame: &[u8]) -> Option<Bytes>;
}
struct At<P> {
    version: Version,
    marker: PhantomData<fn() -> P>,
}
impl<P> fmt::Debug for At<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("At").field(&self.version).finish()
    }
}
fn request_map<A, B>(
    r: WireRequest<A>,
    f: impl FnOnce(A) -> std::result::Result<B, Unsupported>,
) -> std::result::Result<WireRequest<B>, Unsupported> {
    Ok(match r {
        WireRequest::Call(r) => WireRequest::Call(f(r)?),
        WireRequest::KeyState(k) => WireRequest::KeyState(k),
    })
}
fn response_map<A, B>(
    r: WireResponse<A>,
    f: impl FnOnce(A) -> std::result::Result<B, Unsupported>,
) -> std::result::Result<WireResponse<B>, Unsupported> {
    Ok(match r {
        WireResponse::Call(Outcome::Ok(r)) => WireResponse::Call(Outcome::Ok(f(r)?)),
        WireResponse::Call(Outcome::Err(e)) => WireResponse::Call(Outcome::Err(e)),
        WireResponse::KeyState(k) => WireResponse::KeyState(k),
    })
}
fn response_grant<T: crate::Granting>(response: &WireResponse<T>) -> Option<crate::BlobId> {
    match response {
        WireResponse::Call(Outcome::Ok(value)) => value.granted(),
        _ => None,
    }
}

impl<Req, Resp, P> Codec<Req, Resp> for At<P>
where
    Req: Serialize + DeserializeOwned + 'static,
    Resp: Serialize + DeserializeOwned + crate::Granting + 'static,
    P: Toward<Req, Resp>,
    P::Response: crate::Granting,
{
    fn encode_request(
        &self,
        body_len: Option<u64>,
        idem: Option<&Idempotency>,
        r: WireRequest<Req>,
    ) -> Result<Bytes> {
        let message =
            request_map(r, P::down_request).map_err(|source| Error::UnsupportedRequest {
                version: self.version,
                source,
            })?;
        Ok(wire::encode_header(body_len, idem, &message)?)
    }
    fn decode_response(&self, bytes: &[u8]) -> Result<Envelope<WireResponse<Resp>>> {
        let r: Envelope<WireResponse<P::Response>> = postcard::from_bytes(bytes)?;
        let message = response_map(r.message, P::up_response).map_err(|source| {
            Error::UnsupportedResponse {
                version: self.version,
                source,
            }
        })?;
        Ok(Envelope {
            message,
            body_len: r.body_len,
            idempotency: r.idempotency,
        })
    }
    fn decode_request(
        &self,
        bytes: &[u8],
    ) -> Result<std::result::Result<Envelope<WireRequest<Req>>, RemoteError>> {
        let r: Envelope<WireRequest<P::Request>> = postcard::from_bytes(bytes)?;
        Ok(request_map(r.message, P::up_request)
            .map(|message| Envelope {
                message,
                body_len: r.body_len,
                idempotency: r.idempotency,
            })
            .map_err(|e| RemoteError::UnsupportedRequest {
                version: self.version.label,
                detail: e.to_string(),
            }))
    }
    fn reencode_response(
        &self,
        frame: Bytes,
        current_grant: Option<crate::BlobId>,
    ) -> Result<EncodedReply> {
        if P::CURRENT {
            return Ok(EncodedReply {
                frame,
                carry_body: true,
                grant: current_grant,
            });
        }
        let r: Envelope<WireResponse<Resp>> = postcard::from_bytes(&frame[4..])?;
        let original_grant = response_grant(&r.message);
        let (message, len, carry_body) = match response_map(r.message, P::down_response) {
            Ok(m) if response_grant(&m) == original_grant => (m, r.body_len, true),
            Ok(_) => (
                WireResponse::Call(Outcome::Err(RemoteError::UnsupportedResponse {
                    version: self.version.label,
                    detail: "a contract conversion cannot change the blob address".into(),
                })),
                Some(0),
                false,
            ),
            Err(e) => (
                WireResponse::Call(Outcome::Err(RemoteError::UnsupportedResponse {
                    version: self.version.label,
                    detail: e.to_string(),
                })),
                Some(0),
                false,
            ),
        };
        let grant = response_grant(&message);
        Ok(EncodedReply {
            frame: wire::encode_header(len, r.idempotency.as_ref(), &message)?,
            carry_body,
            grant,
        })
    }
    #[cfg(feature = "persist")]
    fn upgrade_recorded(&self, frame: &[u8]) -> Option<Bytes> {
        let r = self.decode_response(frame.get(4..)?).ok()?;
        wire::encode_header(r.body_len, r.idempotency.as_ref(), &r.message).ok()
    }
}

pub(crate) struct Entry<Req, Resp> {
    pub(crate) version: Version,
    pub(crate) alpn: Vec<u8>,
    pub(crate) codec: Arc<dyn Codec<Req, Resp>>,
}
#[doc(hidden)]
pub struct Lineage<Req, Resp> {
    pub(crate) entries: Vec<Entry<Req, Resp>>,
    pub(crate) probe: Vec<u8>,
    window: Window,
}
impl<Req, Resp> Lineage<Req, Resp>
where
    Req: Serialize + DeserializeOwned + 'static,
    Resp: Serialize + DeserializeOwned + crate::Granting + 'static,
{
    pub fn new<P: Toward<Req, Resp>>(protocol: &'static str, current: Version) -> Self
    where
        P::Response: crate::Granting,
    {
        let mut this = Self {
            entries: Vec::new(),
            probe: crate::derive_alpn::<ProbeReply>(&format!("{protocol}/versions")),
            window: Window {
                service: protocol.into(),
                versions: Vec::new(),
            },
        };
        this.push::<P>(current);
        this
    }
    #[must_use]
    pub fn previous<P: Toward<Req, Resp>>(mut self, version: Version) -> Self
    where
        P::Response: crate::Granting,
    {
        self.push::<P>(version);
        self
    }
    fn push<P: Toward<Req, Resp>>(&mut self, version: Version)
    where
        P::Response: crate::Granting,
    {
        __check_window_len(self.entries.len() + 1);
        self.entries.push(Entry {
            version,
            alpn: version.alpn(&self.window.service),
            codec: Arc::new(At::<P> {
                version,
                marker: PhantomData,
            }),
        });
        self.window.versions.push(version);
    }
}
impl<Req, Resp> Lineage<Req, Resp> {
    pub fn window(&self) -> &Window {
        &self.window
    }
    pub fn alpn(&self) -> &[u8] {
        &self.entries[0].alpn
    }
    pub(crate) fn mismatch(&self, theirs: PeerWindow) -> Error {
        Error::SchemaMismatch(Box::new(Mismatch::new(self.window.clone(), theirs)))
    }
}

/// The one recovery conversion shared by all mounts of a service.
#[cfg(feature = "persist")]
pub(crate) type UpgradeRecord = dyn Fn(Version, &[u8]) -> Option<Bytes> + Send + Sync;
pub(crate) struct RecordCodec {
    pub(crate) current: Version,
    #[cfg(feature = "persist")]
    pub(crate) upgrade: Arc<UpgradeRecord>,
}
impl fmt::Debug for RecordCodec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("RecordCodec").field(&self.current).finish()
    }
}
impl<Req: 'static, Resp: 'static> Lineage<Req, Resp> {
    pub(crate) fn records(&'static self) -> RecordCodec {
        let current = self.entries[0].version;
        RecordCodec {
            current,
            #[cfg(feature = "persist")]
            upgrade: Arc::new(move |version, frame| {
                if version == current {
                    return Some(Bytes::copy_from_slice(frame));
                }
                self.entries
                    .iter()
                    .find(|entry| entry.version == version)?
                    .codec
                    .upgrade_recorded(frame)
            }),
        }
    }
}
#[cfg(test)]
impl RecordCodec {
    pub(crate) fn testing() -> Self {
        Self {
            current: Version::__new(0, 0),
            #[cfg(feature = "persist")]
            upgrade: Arc::new(|version, frame| {
                (version == Version::__new(0, 0)).then(|| Bytes::copy_from_slice(frame))
            }),
        }
    }
}

#[derive(Serialize, Deserialize, Schema)]
struct ProbeReply {
    versions: Vec<Version>,
}
const MAX_WINDOW: usize = 64;
/// Check a generated window before any ALPN or probe can publish it.
#[doc(hidden)]
pub const fn __check_window_len(count: usize) {
    ::core::assert!(
        count <= MAX_WINDOW,
        "a contract window holds at most 64 versions"
    );
}
pub(crate) struct Probe {
    pub(crate) window: Window,
    pub(crate) admission: Arc<dyn Admission>,
    pub(crate) required: Vec<Arc<dyn Admission>>,
}
impl fmt::Debug for Probe {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Probe").field(&self.window).finish()
    }
}
impl ProtocolHandler for Probe {
    async fn accept(&self, connection: Connection) -> std::result::Result<(), AcceptError> {
        let Some((_peer, _live)) =
            crate::admission::settle(self.admission.as_ref(), &self.required, &connection)
        else {
            return Ok(());
        };
        let result: Result<()> = async {
            let mut send = connection.open_uni().await?;
            let payload = postcard::to_allocvec(&ProbeReply {
                versions: self.window.versions.clone(),
            })?;
            let mut frame = Vec::with_capacity(4 + payload.len());
            frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            frame.extend_from_slice(&payload);
            wire::write_frame(&mut send, frame.into()).await?;
            send.finish()?;
            connection.closed().await;
            Ok(())
        }
        .await;
        result.map_err(AcceptError::from_err)
    }
}
// A diagnostic must not turn a completed TLS handshake into an unbounded dial.
const PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
async fn bounded_probe(
    exchange: impl std::future::Future<Output = Result<ProbeReply>>,
) -> Option<ProbeReply> {
    n0_future::time::timeout(PROBE_TIMEOUT, exchange)
        .await
        .ok()
        .and_then(std::result::Result::ok)
}

pub(crate) async fn read_probe(connection: &Connection, service: &str) -> PeerWindow {
    let result = bounded_probe(async {
        let recv = connection.accept_uni().await?;
        let bytes = FrameReader::new(recv)
            .read_frame()
            .await?
            .ok_or(Error::Closed)?;
        Ok(postcard::from_bytes(&bytes)?)
    })
    .await;
    match result {
        Some(reply)
            if !reply.versions.is_empty()
                && reply.versions.len() <= MAX_WINDOW
                && reply.versions.windows(2).all(|v| v[0].label > v[1].label) =>
        {
            PeerWindow::Speaks(Window {
                service: service.into(),
                versions: reply.versions,
            })
        }
        _ => PeerWindow::Unknown,
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]
    use super::*;
    impl crate::Granting for u8 {
        fn granted(&self) -> Option<crate::BlobId> {
            None
        }
    }
    impl crate::Granting for u64 {
        fn granted(&self) -> Option<crate::BlobId> {
            None
        }
    }
    fn window(labels: &[u32]) -> Window {
        Window {
            service: "p/S".into(),
            versions: labels.iter().map(|&l| Version::__new(l, 0)).collect(),
        }
    }
    #[test]
    fn windows_name_single_contiguous_and_gapped_labels() {
        assert_eq!(window(&[3]).to_string(), "p/S/3");
        assert_eq!(window(&[3, 2]).to_string(), "p/S/2..=3");
        assert_eq!(window(&[3, 1]).to_string(), "p/S/{1,3}");
    }
    #[test]
    fn version_alpn_carries_label_and_hash() {
        assert_eq!(Version::__new(3, 7).alpn("p"), b"p/3/0000000000000007");
    }
    #[test]
    fn mismatch_names_shared_labels_with_different_shapes() {
        let mut theirs = window(&[3]);
        theirs.versions[0].hash = 1;
        assert!(Mismatch::new(window(&[3]), PeerWindow::Speaks(theirs)).to_string().contains("both claim 3 with different shapes (ours 0000000000000000, theirs 0000000000000001)"));
    }
    struct RefuseReply;
    impl Hop for RefuseReply {
        type Request = u8;
        type Response = u8;
        type NewerRequest = u8;
        type NewerResponse = u8;
        fn upgrade_request(r: u8) -> std::result::Result<u8, Unsupported> {
            Ok(r)
        }
        fn downgrade_request(r: u8) -> std::result::Result<u8, Unsupported> {
            Ok(r)
        }
        fn upgrade_response(r: u8) -> std::result::Result<u8, Unsupported> {
            Ok(r)
        }
        fn downgrade_response(_: u8) -> std::result::Result<u8, Unsupported> {
            Err(Unsupported::new("Fetch", "no older reply"))
        }
    }
    /// A downgrade refusal is a header without the handler's body.
    ///
    /// Watched red with `scripts/break-once.sh`: changed the body flag on the
    /// unsupported-response branch to `true`. The isolated unit test failed at
    /// "a conversion refusal must not stream the current body's bytes";
    /// 0 passed, 1 failed. Restored. The edit is described because this record
    /// lives in the file the harness changes.
    #[test]
    fn an_unrepresentable_reply_does_not_carry_its_current_body() {
        let codec = At::<(RefuseReply, Here<u8, u8>)> {
            version: Version::__new(2, 0),
            marker: PhantomData,
        };
        let current = wire::encode_header(Some(17), None, &WireResponse::Call(Outcome::Ok(7_u8)))
            .expect("encodes");
        let encoded = codec
            .reencode_response(current, None)
            .expect("refusal encodes");
        let frame = encoded.frame;
        let carry_body = encoded.carry_body;
        assert!(
            !carry_body,
            "a conversion refusal must not stream the current body's bytes"
        );
        let reply: Envelope<WireResponse<u8>> = postcard::from_bytes(&frame[4..]).expect("decodes");
        assert_eq!(reply.body_len, Some(0));
        assert!(matches!(
            reply.message,
            WireResponse::Call(Outcome::Err(RemoteError::UnsupportedResponse {
                version: 2,
                ..
            }))
        ));
    }
    /// Watched red: compare only hashes in the current-version recovery fast
    /// path. The isolated test read 7 instead of the required converted 700;
    /// 0 passed, 1 failed. Restored the full identity comparison.
    #[cfg(feature = "persist")]
    #[test]
    fn recovery_selects_the_label_even_when_shapes_have_equal_hashes() {
        struct Units;
        impl Hop for Units {
            type Request = u64;
            type Response = u64;
            type NewerRequest = u64;
            type NewerResponse = u64;
            fn upgrade_request(r: u64) -> std::result::Result<u64, Unsupported> {
                Ok(r)
            }
            fn downgrade_request(r: u64) -> std::result::Result<u64, Unsupported> {
                Ok(r)
            }
            fn upgrade_response(r: u64) -> std::result::Result<u64, Unsupported> {
                Ok(r * 100)
            }
            fn downgrade_response(r: u64) -> std::result::Result<u64, Unsupported> {
                Ok(r / 100)
            }
        }
        let hash = crate::__schema_hash::<u64>("units");
        let old = Version::__new(1, hash);
        let current = Version::__new(2, hash);
        let lineage = Box::leak(Box::new(
            Lineage::new::<Here<u64, u64>>("units", current)
                .previous::<(Units, Here<u64, u64>)>(old),
        ));
        let encoded = wire::encode_header(Some(0), None, &WireResponse::Call(Outcome::Ok(7_u64)))
            .expect("encodes");
        let recorded = lineage.records();
        let upgraded = (recorded.upgrade)(old, &encoded).expect("known older version");
        let reply = lineage.entries[0]
            .codec
            .decode_response(&upgraded[4..])
            .expect("decodes current");
        assert!(matches!(
            reply.message,
            WireResponse::Call(Outcome::Ok(700))
        ));
        assert_eq!((recorded.upgrade)(current, &encoded), Some(encoded.clone()));
        assert!((recorded.upgrade)(Version::__new(3, hash), &encoded).is_none());
    }
    /// Watched red: double the exchange timeout without moving the outer test
    /// budget. The paused-clock test failed, 0 passed/1 failed, with no wall-clock
    /// upper bound. Restored the service probe deadline.
    #[tokio::test(start_paused = true)]
    async fn the_probe_deadline_bounds_application_progress() {
        let outer = tokio::time::timeout(
            PROBE_TIMEOUT + std::time::Duration::from_secs(1),
            bounded_probe(std::future::pending()),
        )
        .await;
        assert!(
            matches!(outer, Ok(None)),
            "the probe's own deadline must finish before the outer test budget"
        );
    }
    #[tokio::test]
    async fn a_peer_that_selects_the_probe_but_withholds_a_reply_is_unknown() {
        #[derive(Debug)]
        struct Withhold(Arc<tokio::sync::Notify>);
        impl ProtocolHandler for Withhold {
            async fn accept(&self, connection: Connection) -> std::result::Result<(), AcceptError> {
                connection.closed().await;
                self.0.notify_one();
                Ok(())
            }
        }
        let server = crate::Node::bind_local().await.expect("binds");
        let caller = crate::Node::bind_local().await.expect("binds");
        let lineage = Box::leak(Box::new(Lineage::new::<Here<u8, u8>>(
            "withheld/probe",
            Version::__new(1, crate::__schema_hash::<u8>("withheld/probe")),
        )));
        let closed = Arc::new(tokio::sync::Notify::new());
        let _router = iroh::protocol::Router::builder(server.endpoint().clone())
            .accept(lineage.probe.clone(), Withhold(closed.clone()))
            .spawn();
        let error = crate::RpcClient::connect(&caller, server.endpoint().addr(), lineage)
            .await
            .expect_err("a probe is not an agreed contract");
        assert!(
            matches!(error, Error::SchemaMismatch(m) if matches!(m.theirs(), PeerWindow::Unknown))
        );
        closed.notified().await;
    }
    #[test]
    #[should_panic(expected = "a contract window holds at most 64 versions")]
    fn construction_refuses_a_window_its_probe_reader_would_reject() {
        let hash = crate::__schema_hash::<u8>("wide");
        let mut lineage =
            Lineage::new::<Here<u8, u8>>("wide", Version::__new(MAX_WINDOW as u32, hash));
        for label in (1..MAX_WINDOW as u32).rev() {
            lineage = lineage.previous::<Here<u8, u8>>(Version::__new(label, hash));
        }
        assert_eq!(lineage.window().versions().len(), MAX_WINDOW);
        let _ = lineage.previous::<Here<u8, u8>>(Version::__new(0, hash));
    }
}
