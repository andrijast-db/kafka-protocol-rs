use std::io::Write;

use anyhow::{Context, Result};
use bytes::buf::BufMut;
use bytes::{Bytes, BytesMut};
use flate2::write::{GzDecoder, GzEncoder};
use flate2::Compression;

use crate::protocol::buf::{ByteBuf, ByteBufMut};

use super::{Compressor, Decompressor, LimitedWriter};

/// Gzip compression algorithm. See [Kafka's broker configuration](https://kafka.apache.org/documentation/#brokerconfigs_compression.type)
/// for more information.
pub struct Gzip;

impl<B: ByteBufMut> Compressor<B> for Gzip {
    type BufMut = BytesMut;
    fn compress<R, F>(buf: &mut B, f: F) -> Result<R>
    where
        F: FnOnce(&mut Self::BufMut) -> Result<R>,
    {
        // Write uncompressed bytes into a temporary buffer
        let mut tmp = BytesMut::new();
        let res = f(&mut tmp)?;

        // Compress directly into the target buffer
        let mut e = GzEncoder::new(buf.writer(), Compression::default());
        e.write_all(&tmp).context("Failed to compress gzip")?;
        e.finish().context("Failed to compress gzip")?;

        Ok(res)
    }
}

impl<B: ByteBuf> Decompressor<B> for Gzip {
    type Buf = Bytes;
    fn decompress<R, F>(buf: &mut B, f: F) -> Result<R>
    where
        F: FnOnce(&mut Self::Buf) -> Result<R>,
    {
        Self::decompress_with_limit(buf, usize::MAX, f)
    }

    fn decompress_with_limit<R, F>(buf: &mut B, max_size: usize, f: F) -> Result<R>
    where
        F: FnOnce(&mut Self::Buf) -> Result<R>,
    {
        // Decompress directly from the input buffer
        let mut d = GzDecoder::new(LimitedWriter::new(max_size));
        d.write_all(&buf.copy_to_bytes(buf.remaining()))
            .and_then(|_| d.try_finish())
            .map_err(|e| d.get_ref().map_err(e, "Failed to decompress gzip"))?;
        let tmp = d.finish().context("Failed to decompress gzip")?.buf;

        f(&mut tmp.freeze())
    }
}
