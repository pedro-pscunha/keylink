//! Framing. QUIC gives a byte stream, not a message stream.
//!
//! One call occupies one bidirectional stream, and each direction of that
//! stream carries exactly one length-prefixed header frame followed by an
//! optional raw body:
//!
//! ```text
//! [u32 LE header_len][postcard(Envelope<Msg>)][body bytes ...]FIN
//! ```
//!
//! The body needs no framing of its own. Its extent is the rest of the stream,
//! its size is declared in the envelope, and its flow control is QUIC's. That
//! is why a 4 GiB file costs zero framing bytes and a bounded amount of memory.
//!
//! Nothing outside this module and [`crate::body`] touches bytes.

use bytes::{Buf, Bytes, BytesMut};
use iroh::endpoint::{RecvStream, SendStream};
use postcard_schema::Schema;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{Error, Result};

/// Refuse absurd header frames *before* buffering for them, so a peer cannot
/// make us reserve gigabytes by lying about a length.
///
/// Bodies are deliberately not subject to this: they are streamed, never held,
/// and their size is a declared value the receiver can refuse before a byte
/// moves. See [`crate::BodyReader::declared_len`].
pub const MAX_HEADER: u32 = 1 << 20;

/// Bytes of length prefix ahead of every header frame.
const PREFIX: usize = size_of::<u32>();

/// Upper bound on a single transport read. Not a frame cap — the transport
/// hands back whatever it has, and this only stops one read from returning an
/// unbounded slab.
const READ_CHUNK: usize = 64 * 1024;

/// keylink's own framing metadata, wrapped around the caller's message.
///
/// This is part of the hashed contract, so a change to keylink's wire format
/// moves the ALPN exactly like a change to the caller's types does. There is no
/// separate protocol version to remember to bump.
/// # Freezing
///
/// Every field of this type is inside the hashed contract, so ANY change here
/// moves the ALPN for every service in the fleet at once. That is the intended
/// safety property and it is also the reason to change it rarely: prefer to put
/// variation in the message, which is per service.
/// The wire shapes this build of keylink can accept, hashed into every contract.
///
/// # The defect this closes
///
/// Before `tell`, the ALPN hashed the USER's types and nothing about keylink's
/// own wire capabilities. So two peers with byte-identical service definitions,
/// one built against a keylink that accepts unidirectional streams and one
/// built against a keylink that does not, derived the SAME ALPN. The handshake
/// agreed, the sender opened a `uni` stream, the older peer never accepted one,
/// and **every `tell` was silently lost** — no error on either side.
///
/// Naming keylink's wire surface inside the hashed contract makes that a
/// refused handshake instead. The `bool` values do not matter to the hash —
/// `postcard_schema` hashes types, not values — so this is not a per-service
/// discriminator. It is a version of keylink's wire protocol expressed as a
/// type, and it moves whenever a FIELD is added.
///
/// # Changing this
///
/// Adding a field moves the ALPN of every service in the fleet at once. That is
/// the point, and it is why a new field belongs here only when keylink learns a
/// genuinely new way to carry a message. Per-service variation belongs in the
/// message.
#[derive(Debug, Serialize, Deserialize, Schema)]
pub struct WireKinds {
    /// This keylink accepts unidirectional streams, which is how `tell` is
    /// delivered.
    pub(crate) uni: bool,
}

#[derive(Debug, Serialize, Deserialize, Schema)]
/// Fields are `pub(crate)`: every reader and writer is in this crate, and a
/// downstream crate that could build one could put an `IdempotencyKey` on the
/// wire that never met a constructor. The TYPE stays public because each
/// generated `Contract` names it.
pub struct Envelope<T> {
    /// How many body bytes follow the header, when the sender knows. `None` is
    /// an open-ended stream that ends when the sender finishes it.
    pub(crate) body_len: Option<u64>,
    /// Caller-supplied key so that a retried call runs at most once.
    ///
    /// Lives here rather than in an actor request because retry safety is not
    /// an actor concern: an ordinary `rpc` that reserves stock needs it just as
    /// much as an actor message does.
    pub(crate) idempotency: Option<crate::IdempotencyKey>,
    /// The caller's request or response.
    pub(crate) message: T,
}

/// Serialise a header frame and hand it to the transport as one owned chunk.
///
/// The length prefix is reserved at the front of the same buffer postcard
/// serialises into, so the frame is one allocation and one write with no
/// concatenation and no vectored fixup.
pub(crate) async fn write_header<T: Serialize>(
    stream: &mut SendStream,
    body_len: Option<u64>,
    idempotency: Option<&crate::IdempotencyKey>,
    message: &T,
) -> Result<()> {
    write_frame(stream, encode_header(body_len, idempotency, message)?).await
}

/// Hand an already-encoded frame to the transport.
///
/// Split from [`write_header`] so a frame can be kept as well as sent. The
/// idempotency ledger stores exactly these bytes, which is what makes a replayed
/// reply byte-identical to the one the handler produced rather than a
/// re-encoding of a value that has since been decoded and rebuilt.
pub(crate) async fn write_frame(stream: &mut SendStream, frame: Bytes) -> Result<()> {
    stream.write_chunk(frame).await?;
    Ok(())
}

/// Build one header frame, length prefix included.
pub(crate) fn encode_header<T: Serialize>(
    body_len: Option<u64>,
    idempotency: Option<&crate::IdempotencyKey>,
    message: &T,
) -> Result<Bytes> {
    let mut buf = BytesMut::with_capacity(256);
    buf.extend_from_slice(&[0u8; PREFIX]);
    // The clone is bounded at MAX_KEY_LEN and happens once per call. Borrowing
    // into the envelope instead would make `Envelope` generic over a lifetime
    // that only the write path has, and the read path would pay for it.
    let mut buf = postcard::to_extend(
        &Envelope {
            body_len,
            idempotency: idempotency.cloned(),
            message,
        },
        buf,
    )?;

    let encoded = buf.len() - PREFIX;
    let Ok(len) = u32::try_from(encoded) else {
        return Err(Error::FrameTooLarge {
            len: encoded as u64,
            cap: MAX_HEADER,
        });
    };
    if len > MAX_HEADER {
        return Err(Error::FrameTooLarge {
            len: u64::from(len),
            cap: MAX_HEADER,
        });
    }
    buf[..PREFIX].copy_from_slice(&len.to_le_bytes());

    Ok(buf.freeze())
}

/// The read half of one stream: a header frame, then the body.
///
/// Holds whatever the transport handed over past the end of the header, so the
/// body path starts from bytes already in hand instead of from a second read.
#[derive(Debug)]
pub(crate) struct FrameReader {
    stream: RecvStream,
    staged: BytesMut,
}

impl FrameReader {
    pub(crate) fn new(stream: RecvStream) -> Self {
        Self {
            stream,
            staged: BytesMut::new(),
        }
    }

    /// Read the header frame.
    ///
    /// `Ok(None)` means the peer finished the stream cleanly *before* the frame
    /// started — a normal end of conversation. A frame that starts and does not
    /// finish is [`Error::FrameTruncated`]. The two are never confused.
    pub(crate) async fn read_header<T: DeserializeOwned>(&mut self) -> Result<Option<Envelope<T>>> {
        if !self.stage_at_least(PREFIX).await? {
            return if self.staged.is_empty() {
                Ok(None)
            } else {
                Err(Error::FrameTruncated {
                    got: self.staged.len(),
                    want: PREFIX,
                })
            };
        }

        let len = self.staged.get_u32_le();

        // Before buffering anything. A peer that announces 4 GiB is refused
        // here, having cost us one comparison.
        if len > MAX_HEADER {
            return Err(Error::FrameTooLarge {
                len: u64::from(len),
                cap: MAX_HEADER,
            });
        }

        let want = len as usize;
        if !self.stage_at_least(want).await? {
            return Err(Error::FrameTruncated {
                got: self.staged.len(),
                want,
            });
        }

        let frame = self.staged.split_to(want).freeze();
        Ok(Some(postcard::from_bytes(&frame)?))
    }

    /// The next run of body bytes, or `None` once the peer finishes the stream.
    ///
    /// Zero-copy: past the staged remainder of the header read, these are the
    /// transport's own buffers handed straight through.
    pub(crate) async fn next_chunk(&mut self) -> Result<Option<Bytes>> {
        if !self.staged.is_empty() {
            return Ok(Some(self.staged.split().freeze()));
        }
        Ok(self.stream.read_chunk(READ_CHUNK).await?)
    }

    /// Pull until `n` bytes are staged. `Ok(false)` means the peer finished
    /// first, leaving whatever is staged as a partial frame.
    async fn stage_at_least(&mut self, n: usize) -> Result<bool> {
        while self.staged.len() < n {
            let Some(chunk) = self.stream.read_chunk(READ_CHUNK).await? else {
                return Ok(false);
            };
            self.staged.extend_from_slice(&chunk);
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;
    use crate::testing;

    #[tokio::test]
    async fn round_trips_a_header_and_the_body_behind_it() {
        let link = testing::link().await;
        let (mut send, _recv) = link.client.open_bi().await.unwrap();

        write_header(&mut send, Some(3), None, &"hello".to_string())
            .await
            .unwrap();
        send.write_all(b"abc").await.unwrap();
        send.finish().unwrap();

        let (_send, recv) = link.server.accept_bi().await.unwrap();
        let mut reader = FrameReader::new(recv);

        let header = reader.read_header::<String>().await.unwrap().unwrap();
        assert_eq!(header.message, "hello");
        assert_eq!(header.body_len, Some(3));
        assert_eq!(&reader.next_chunk().await.unwrap().unwrap()[..], b"abc");
    }

    /// Invariant: the cap is checked against the *announced* length, so a peer
    /// that lies pays for it and we do not. This resolves immediately; if the
    /// check happened after buffering it would block until the connection
    /// timed out, because no bytes follow the prefix.
    #[tokio::test]
    async fn an_announced_frame_over_the_cap_is_refused_before_anything_is_buffered() {
        let link = testing::link().await;
        let (mut send, _recv) = link.client.open_bi().await.unwrap();
        send.write_all(&u32::MAX.to_le_bytes()).await.unwrap();

        let (_send, recv) = link.server.accept_bi().await.unwrap();
        let error = FrameReader::new(recv)
            .read_header::<String>()
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                Error::FrameTooLarge {
                    len: 4_294_967_295,
                    cap: MAX_HEADER
                }
            ),
            "got {error:?}"
        );
    }

    /// Invariant: a peer that finishes cleanly at a frame boundary has ended the
    /// conversation, not failed.
    #[tokio::test]
    async fn a_clean_close_at_a_frame_boundary_is_not_an_error() {
        let link = testing::link().await;
        let (mut send, _recv) = link.client.open_bi().await.unwrap();
        write_header(&mut send, None, None, &"only one".to_string())
            .await
            .unwrap();
        send.finish().unwrap();

        let (_send, recv) = link.server.accept_bi().await.unwrap();
        let mut reader = FrameReader::new(recv);

        assert!(reader.read_header::<String>().await.unwrap().is_some());
        assert!(reader.read_header::<String>().await.unwrap().is_none());
    }

    /// The other half of the same invariant: stopping mid-frame is a failure,
    /// and must never be reported as the end of the conversation.
    #[tokio::test]
    async fn a_stream_that_stops_mid_frame_is_an_error() {
        let link = testing::link().await;
        let (mut send, _recv) = link.client.open_bi().await.unwrap();
        send.write_all(&64u32.to_le_bytes()).await.unwrap();
        send.write_all(&[7u8; 10]).await.unwrap();
        send.finish().unwrap();

        let (_send, recv) = link.server.accept_bi().await.unwrap();
        let error = FrameReader::new(recv)
            .read_header::<String>()
            .await
            .unwrap_err();

        assert!(
            matches!(error, Error::FrameTruncated { got: 10, want: 64 }),
            "got {error:?}"
        );
    }

    /// Chunk boundaries are the transport's business and do not track writes,
    /// so a frame split across several reads must still assemble.
    #[tokio::test]
    async fn a_frame_split_across_many_writes_still_assembles() {
        let link = testing::link().await;
        let (mut send, _recv) = link.client.open_bi().await.unwrap();

        let long = "k".repeat(200_000);
        let mut framed = BytesMut::with_capacity(256);
        framed.extend_from_slice(&[0u8; PREFIX]);
        let mut framed = postcard::to_extend(
            &Envelope {
                body_len: None,
                idempotency: None,
                message: &long,
            },
            framed,
        )
        .unwrap();
        let len = u32::try_from(framed.len() - PREFIX).unwrap();
        framed[..PREFIX].copy_from_slice(&len.to_le_bytes());

        for piece in framed.freeze().chunks(1000) {
            send.write_all(piece).await.unwrap();
        }
        send.finish().unwrap();

        let (_send, recv) = link.server.accept_bi().await.unwrap();
        let header = FrameReader::new(recv)
            .read_header::<String>()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(header.message, long);
    }
}
