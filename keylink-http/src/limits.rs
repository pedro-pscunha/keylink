//! Finite inclusive JSON byte ceilings.

use std::num::NonZeroUsize;

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

pub(crate) struct BoundedWriter {
    bytes: Vec<u8>,
    limit: usize,
    overflowed: bool,
}

impl BoundedWriter {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(limit),
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
    #[test]
    fn bounded_writer_accepts_limit_and_refuses_limit_plus_one() {
        let mut writer = BoundedWriter::new(3);
        writer.write_all(b"ab").expect("below limit");
        writer.write_all(b"c").expect("at limit");
        assert!(writer.write_all(b"d").is_err());
        assert_eq!(writer.into_inner(), b"abc");
    }
}
