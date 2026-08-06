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
#[cfg(feature = "fs")]
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
    #[cfg(feature = "fs")]
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

// ───────────────────────────────────────────────────────── the browser's bodies

/// A reader that is only ever touched from the thread that made it.
///
/// **This is not a level-2 newtype and must not be documented as one.** It
/// deletes no check. It asserts a platform fact: `wasm32-unknown-unknown`
/// without the `atomics` target feature is single threaded, so a value that is
/// `!Send` for want of thread safety cannot in fact cross a thread, because
/// there is no second thread to cross to.
///
/// The assertion is needed because [`Source::Reader`] requires `Send` — iroh's
/// `ProtocolHandler::accept` returns `impl Future + Send` on **every** target,
/// with no `cfg`, so the bound cannot be relaxed for the browser — while
/// `wasm_streams`' reader holds a `JsFuture`, which holds an `Rc<RefCell<_>>`.
///
/// What makes the assertion enforceable rather than hopeful is the
/// `compile_error!` in `lib.rs` that refuses `web` together with
/// `target_feature = "atomics"`. Without that guard this `unsafe impl` would be
/// unsound under threaded wasm, and nothing would say so.
#[cfg(all(feature = "web", target_family = "wasm", target_os = "unknown"))]
struct OnOneThread<R>(R);

#[cfg(all(feature = "web", target_family = "wasm", target_os = "unknown"))]
// SAFETY: see the type's doc comment. Sound only while keylink refuses to
// compile the `web` feature for threaded wasm, which lib.rs enforces.
#[expect(
    unsafe_code,
    reason = "the one unsafe impl in the crate: `Send` for a browser stream \
              reader that is `!Send` only for want of a second thread, on a \
              target where lib.rs refuses to compile if a second thread exists"
)]
unsafe impl<R> Send for OnOneThread<R> {}

#[cfg(all(feature = "web", target_family = "wasm", target_os = "unknown"))]
impl<R: AsyncRead + Unpin> AsyncRead for OnOneThread<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

#[cfg(all(feature = "web", target_family = "wasm", target_os = "unknown"))]
impl Body {
    /// Stream a browser `File` — from an `<input type="file">` or a drop — with
    /// its length declared.
    ///
    /// The browser's answer to [`Body::from_file`], and the reason the `web`
    /// feature exists. `File::size()` is known before a byte is read, so the
    /// receiver can tell a truncated transfer from a clean close, which is the
    /// property `from_reader(_, None)` gives up.
    ///
    /// # Errors
    ///
    /// [`Error::NotAByteStream`] when the file's stream cannot produce a BYOB
    /// reader. See [`Self::from_web_stream`].
    pub fn from_web_file(file: &web_sys::File) -> Result<Self> {
        // f64, and exact below 2^53 bytes — nine petabytes, so the cast is
        // lossless for anything a browser can hold.
        #[expect(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let len = file.size() as u64;
        Self::from_web_stream(file.stream(), Some(len))
    }

    /// Stream any browser `ReadableStream`.
    ///
    /// # Why this returns a `Result` when the obvious signature does not
    ///
    /// `wasm_streams`' `into_async_read` is documented to **panic** unless the
    /// stream is a readable *byte* stream, because it needs a BYOB reader —
    /// and it panics through `expect_throw`, which raises a JS exception rather
    /// than unwinding. Under a `panic = "abort"` release profile that ends the
    /// module.
    ///
    /// An infallible constructor over a fallible operation is worse than a
    /// guard: the caller cannot see the failure, let alone handle it. So this
    /// takes the fallible route and reports [`Error::NotAByteStream`].
    ///
    /// `File::stream()` is a byte stream per the File API, so
    /// [`Self::from_web_file`] is expected to succeed. A stream built by hand
    /// with `new ReadableStream({start(c) {..}})` is not, and will be refused.
    ///
    /// # Errors
    ///
    /// [`Error::NotAByteStream`] when the stream is already locked to a reader,
    /// or cannot produce a BYOB reader.
    pub fn from_web_stream(stream: web_sys::ReadableStream, len: Option<u64>) -> Result<Self> {
        use tokio_util::compat::FuturesAsyncReadCompatExt;

        let reader = wasm_streams::ReadableStream::from_raw(stream)
            .try_into_async_read()
            .map_err(|_| Error::NotAByteStream)?;

        Ok(Self {
            declared_len: len,
            source: Source::Reader(Box::pin(OnOneThread(reader.compat()))),
        })
    }
}
