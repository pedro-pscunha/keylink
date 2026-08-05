//! Large payloads: files, blobs, and open-ended byte streams.
//!
//! A body is not a message. Messages are typed, small, and decoded whole; a
//! body is opaque bytes that neither side ever holds in full. The two travel on
//! the same QUIC stream, so a body needs no request id to be correlated with
//! its call: *the stream is the correlation*.
//!
//! Backpressure is QUIC's flow-control window, not a queue we manage. When the
//! receiver stops reading, the sender's write stops resolving, and the source
//! stops being polled. Peak memory is one chunk, whatever the payload size.

use std::fmt;
use std::path::Path;
use std::pin::Pin;

use bytes::{Bytes, BytesMut};
use iroh::endpoint::SendStream;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};

use crate::wire::FrameReader;
use crate::{Error, Result};

/// Bytes buffered between the source and the transport on the sending side.
const COPY_BUFFER: usize = 64 * 1024;

/// A payload to send alongside a request or a response.
///
/// This is a *value*, not a stream handle: a handler returns one and keylink
/// drains it. That is what keeps the call site free of transport concepts —
/// nobody writes to a socket, they hand over a file.
pub struct Body {
    declared_len: Option<u64>,
    source: Source,
}

enum Source {
    /// Already in memory. Sent without a copy: `Bytes` is handed to the
    /// transport by reference count.
    Bytes(Bytes),
    /// Pulled on demand. Never fully resident.
    Reader(Pin<Box<dyn AsyncRead + Send>>),
}

impl Body {
    /// No payload. What every unary rpc sends.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            declared_len: Some(0),
            source: Source::Bytes(Bytes::new()),
        }
    }

    /// Send bytes already in memory. Zero-copy: nothing is duplicated on the
    /// way to the transport.
    #[must_use]
    pub fn from_bytes(bytes: impl Into<Bytes>) -> Self {
        let bytes = bytes.into();
        Self {
            declared_len: Some(bytes.len() as u64),
            source: Source::Bytes(bytes),
        }
    }

    /// Send a file. The length is read from its metadata, so the receiver knows
    /// the size before the first byte arrives and can refuse it.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] when the file cannot be opened or stat'd.
    pub async fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let file = tokio::fs::File::open(path).await?;
        let declared_len = file.metadata().await?.len();
        Ok(Self {
            declared_len: Some(declared_len),
            source: Source::Reader(Box::pin(file)),
        })
    }

    /// Send whatever an [`AsyncRead`] produces.
    ///
    /// Pass `len` when it is known; the receiver uses it to size its
    /// destination and to tell a short transfer from a complete one. `None` is
    /// an open-ended stream, ended by finishing it.
    #[must_use]
    pub fn from_reader<R: AsyncRead + Send + 'static>(reader: R, len: Option<u64>) -> Self {
        Self {
            declared_len: len,
            source: Source::Reader(Box::pin(reader)),
        }
    }

    /// How many bytes this body will produce, when that is known up front.
    #[must_use]
    pub fn declared_len(&self) -> Option<u64> {
        self.declared_len
    }

    /// Drain into the stream. Consumes the body, because a body is sendable
    /// exactly once and the type system should say so.
    pub(crate) async fn write_to(self, stream: &mut SendStream) -> Result<()> {
        match self.source {
            Source::Bytes(bytes) if bytes.is_empty() => Ok(()),
            Source::Bytes(bytes) => Ok(stream.write_chunk(bytes).await?),
            Source::Reader(reader) => {
                let mut reader = BufReader::with_capacity(COPY_BUFFER, reader);
                tokio::io::copy_buf(&mut reader, stream).await?;
                Ok(())
            }
        }
    }
}

impl fmt::Debug for Body {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let source = match self.source {
            Source::Bytes(_) => "bytes",
            Source::Reader(_) => "reader",
        };
        f.debug_struct("Body")
            .field("declared_len", &self.declared_len)
            .field("source", &source)
            .finish()
    }
}

/// The receiving half of a [`Body`].
///
/// Terminal operations take `self` by value: once a body is drained there is
/// nothing left to read, and consuming the reader makes that a compile error
/// rather than a silent empty result.
#[derive(Debug)]
pub struct BodyReader {
    frames: FrameReader,
    declared_len: Option<u64>,
    received: u64,
}

impl BodyReader {
    pub(crate) fn new(frames: FrameReader, declared_len: Option<u64>) -> Self {
        Self {
            frames,
            declared_len,
            received: 0,
        }
    }

    /// How many bytes the sender declared, when it knew.
    ///
    /// Check this *before* reading to refuse a payload that is too big for
    /// wherever it is going. Refusing here costs one round trip; discovering it
    /// after 40 GiB costs 40 GiB.
    #[must_use]
    pub fn declared_len(&self) -> Option<u64> {
        self.declared_len
    }

    /// Bytes read so far.
    #[must_use]
    pub fn received(&self) -> u64 {
        self.received
    }

    /// The next run of bytes, or `None` at the end of the body.
    ///
    /// Zero-copy — these are the transport's buffers, reference-counted, not
    /// copied into one of ours.
    ///
    /// # Errors
    ///
    /// [`Error::BodyTruncated`] when the sender finished short of what it
    /// declared, [`Error::BodyOverrun`] when it sent more.
    pub async fn next_chunk(&mut self) -> Result<Option<Bytes>> {
        let Some(chunk) = self.frames.next_chunk().await? else {
            return match self.declared_len {
                Some(declared) if self.received < declared => Err(Error::BodyTruncated {
                    declared,
                    received: self.received,
                }),
                _ => Ok(None),
            };
        };

        self.received += chunk.len() as u64;
        if let Some(declared) = self.declared_len {
            if self.received > declared {
                return Err(Error::BodyOverrun { declared });
            }
        }
        Ok(Some(chunk))
    }

    /// Stream the whole body into a sink, returning how many bytes moved.
    ///
    /// Peak memory is one chunk regardless of the payload size, so this is the
    /// right way to land a multi-gigabyte file on disk.
    ///
    /// # Errors
    ///
    /// Propagates the truncation and overrun errors of [`Self::next_chunk`],
    /// plus [`Error::Io`] from the sink.
    pub async fn copy_to<W: AsyncWrite + Unpin>(mut self, sink: &mut W) -> Result<u64> {
        while let Some(chunk) = self.next_chunk().await? {
            sink.write_all(&chunk).await?;
        }
        sink.flush().await?;
        Ok(self.received)
    }

    /// Collect the whole body into memory, refusing to exceed `cap`.
    ///
    /// The cap is a parameter and not a constant because only the caller knows
    /// what it can afford to hold.
    ///
    /// # Errors
    ///
    /// [`Error::BodyTooLarge`] when the body exceeds `cap`, plus the errors of
    /// [`Self::next_chunk`].
    pub async fn read_to_bytes(mut self, cap: usize) -> Result<Bytes> {
        if self.declared_len.is_some_and(|len| len > cap as u64) {
            return Err(Error::BodyTooLarge { cap });
        }
        let mut out = BytesMut::new();
        while let Some(chunk) = self.next_chunk().await? {
            if out.len() + chunk.len() > cap {
                return Err(Error::BodyTooLarge { cap });
            }
            out.extend_from_slice(&chunk);
        }
        Ok(out.freeze())
    }

    /// Read and discard the body.
    ///
    /// # Errors
    ///
    /// Propagates the errors of [`Self::next_chunk`].
    pub async fn drain(mut self) -> Result<u64> {
        while self.next_chunk().await?.is_some() {}
        Ok(self.received)
    }
}
