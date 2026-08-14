//! Finite inclusive JSON byte ceilings.

use std::num::{NonZeroU64, NonZeroUsize};

use crate::ConfigError;

const DEFAULT_JSON_BYTES: usize = 64 * 1024;

/// Positive inclusive request and successful-response JSON byte ceilings.
#[derive(Clone, Copy, Debug)]
pub struct JsonLimits {
    pub(crate) request: NonZeroUsize,
    pub(crate) response: NonZeroUsize,
}

impl JsonLimits {
    /// Build positive inclusive JSON ceilings.
    ///
    /// # Errors
    ///
    /// Returns the corresponding zero-limit [`ConfigError`].
    pub fn new(request_bytes: usize, response_bytes: usize) -> Result<Self, ConfigError> {
        let request = NonZeroUsize::new(request_bytes).ok_or(ConfigError::ZeroRequestLimit)?;
        let response = NonZeroUsize::new(response_bytes).ok_or(ConfigError::ZeroResponseLimit)?;
        Ok(Self { request, response })
    }
}

impl Default for JsonLimits {
    fn default() -> Self {
        Self {
            request: NonZeroUsize::new(DEFAULT_JSON_BYTES).unwrap_or(NonZeroUsize::MIN),
            response: NonZeroUsize::new(DEFAULT_JSON_BYTES).unwrap_or(NonZeroUsize::MIN),
        }
    }
}

/// Explicit finite limits that enable streaming and blob HTTP routes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamingLimits {
    pub(crate) metadata_json: NonZeroUsize,
    pub(crate) upload: NonZeroU64,
    pub(crate) download: NonZeroU64,
    pub(crate) blob_tickets: NonZeroUsize,
}

impl StreamingLimits {
    /// Build positive streaming metadata, transfer, and ticket limits.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::ZeroStreamingLimit`] for the first zero field.
    pub fn new(
        metadata_json_bytes: usize,
        max_upload_bytes: u64,
        max_download_bytes: u64,
        max_blob_tickets: usize,
    ) -> Result<Self, ConfigError> {
        let metadata_json =
            NonZeroUsize::new(metadata_json_bytes).ok_or(ConfigError::ZeroStreamingLimit {
                field: "metadata_json_bytes",
            })?;
        let upload = NonZeroU64::new(max_upload_bytes).ok_or(ConfigError::ZeroStreamingLimit {
            field: "max_upload_bytes",
        })?;
        let download =
            NonZeroU64::new(max_download_bytes).ok_or(ConfigError::ZeroStreamingLimit {
                field: "max_download_bytes",
            })?;
        let blob_tickets =
            NonZeroUsize::new(max_blob_tickets).ok_or(ConfigError::ZeroStreamingLimit {
                field: "max_blob_tickets",
            })?;
        Ok(Self {
            metadata_json,
            upload,
            download,
            blob_tickets,
        })
    }

    /// Maximum decoded JSON metadata bytes.
    #[must_use]
    pub const fn metadata_json_bytes(self) -> usize {
        self.metadata_json.get()
    }

    /// Maximum upload payload bytes.
    #[must_use]
    pub const fn max_upload_bytes(self) -> u64 {
        self.upload.get()
    }

    /// Maximum one-shot download payload bytes.
    #[must_use]
    pub const fn max_download_bytes(self) -> u64 {
        self.download.get()
    }

    /// Maximum process-local blob tickets.
    #[must_use]
    pub const fn max_blob_tickets(self) -> usize {
        self.blob_tickets.get()
    }
}

pub(crate) struct BoundedWriter {
    bytes: Vec<u8>,
    limit: usize,
    overflowed: bool,
}

impl BoundedWriter {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::new(),
            limit,
            overflowed: false,
        }
    }

    pub(crate) fn into_inner(self) -> Vec<u8> {
        self.bytes
    }

    pub(crate) fn overflowed(&self) -> bool {
        self.overflowed
    }
}

impl std::io::Write for BoundedWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            self.overflowed = true;
            return Err(std::io::Error::other(
                "successful response exceeds its JSON limit",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use std::io::Write;

    use super::BoundedWriter;

    /// Watched red with `scripts/break-once.sh` by replacing
    /// `        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
    ///             self.overflowed = true;
    ///             return Err(std::io::Error::other(
    ///                 "successful response exceeds its JSON limit",
    ///             ));
    ///         }`
    /// with an always-false branch. The exact test failed at its error assertion.
    /// Watched red with `scripts/break-once.sh` by replacing
    /// `            bytes: Vec::new(),` with `Vec::with_capacity(limit)`.
    /// The exact test terminated with `capacity overflow`.
    #[test]
    fn bounded_writer_does_not_reserve_the_configured_ceiling() {
        let writer = BoundedWriter::new(usize::MAX);
        assert!(writer.bytes.capacity() < 1024);
    }

    #[test]
    fn bounded_writer_accepts_limit_and_refuses_limit_plus_one() {
        let mut writer = BoundedWriter::new(3);
        writer.write_all(b"ab").expect("below limit");
        writer.write_all(b"c").expect("at limit");
        assert!(writer.write_all(b"d").is_err());
        assert_eq!(writer.into_inner(), b"abc");
    }
}
