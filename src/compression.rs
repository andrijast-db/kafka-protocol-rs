//! Provides compression utilities for encoding records.
//!
//! This module has implementations of gzip, Snappy, as well as a noop compression format that
//! allows encoding and decoding records into a [`Record`](crate::records::Record).

use crate::protocol::buf::{ByteBuf, ByteBufMut};
use anyhow::Result;
use bytes::Buf;
use std::fmt::{Display, Formatter};

#[cfg(feature = "gzip")]
mod gzip;
#[cfg(feature = "lz4")]
mod lz4;
mod none;
#[cfg(feature = "snappy")]
mod snappy;
#[cfg(feature = "zstd")]
mod zstd;

#[cfg(feature = "gzip")]
pub use gzip::Gzip;
#[cfg(feature = "lz4")]
pub use lz4::Lz4;
pub use none::None;
#[cfg(feature = "snappy")]
pub use snappy::Snappy;
#[cfg(feature = "zstd")]
pub use zstd::Zstd;

/// A trait for record compression algorithms.
pub trait Compressor<B: ByteBufMut> {
    /// Target buffer type for compression.
    type BufMut: ByteBufMut;
    /// Compresses into provided [`ByteBufMut`], with records encoded by `F` into `R`.
    fn compress<R, F>(buf: &mut B, f: F) -> Result<R>
    where
        F: FnOnce(&mut Self::BufMut) -> Result<R>;
}

/// A trait for record decompression algorithms.
pub trait Decompressor<B: ByteBuf> {
    /// Target buffer type for decompression.
    type Buf: ByteBuf;
    /// Decompress records from `B` mapped using `F` into `R`.
    fn decompress<R, F>(buf: &mut B, f: F) -> Result<R>
    where
        F: FnOnce(&mut Self::Buf) -> Result<R>;

    /// Decompress records from `B` mapped using `F` into `R`, failing if the decompressed size
    /// exceeds `max_size`.
    fn decompress_with_limit<R, F>(buf: &mut B, max_size: usize, f: F) -> Result<R>
    where
        F: FnOnce(&mut Self::Buf) -> Result<R>,
    {
        Self::decompress(buf, |buf| {
            if buf.remaining() > max_size {
                return Err(DecompressedSizeLimitExceeded { limit: max_size }.into());
            }
            f(buf)
        })
    }
}

/// Error indicating the decompressed size of a record batch exceeded the limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecompressedSizeLimitExceeded {
    /// The limit in bytes.
    pub limit: usize,
}

impl Display for DecompressedSizeLimitExceeded {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Decompressed size exceeds limit of {} bytes", self.limit)
    }
}

impl std::error::Error for DecompressedSizeLimitExceeded {}

/// Writer that fails once more than `limit` bytes are written.
#[cfg(any(feature = "gzip", feature = "lz4", feature = "zstd"))]
struct LimitedWriter {
    buf: bytes::BytesMut,
    limit: usize,
    exceeded: bool,
}

#[cfg(any(feature = "gzip", feature = "lz4", feature = "zstd"))]
impl LimitedWriter {
    fn new(limit: usize) -> Self {
        Self {
            buf: bytes::BytesMut::new(),
            limit,
            exceeded: false,
        }
    }

    /// Decoders may wrap the error returned by `write`, so check the flag instead.
    fn map_err(&self, error: std::io::Error, context: &'static str) -> anyhow::Error {
        if self.exceeded {
            DecompressedSizeLimitExceeded { limit: self.limit }.into()
        } else {
            anyhow::Error::new(error).context(context)
        }
    }
}

#[cfg(any(feature = "gzip", feature = "lz4", feature = "zstd"))]
impl std::io::Write for LimitedWriter {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        if self.buf.len().saturating_add(data.len()) > self.limit {
            self.exceeded = true;
            return Err(std::io::Error::other(DecompressedSizeLimitExceeded {
                limit: self.limit,
            }));
        }
        self.buf.extend_from_slice(data);
        Ok(data.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
