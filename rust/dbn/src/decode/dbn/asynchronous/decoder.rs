use async_compression::tokio::bufread::ZstdDecoder;
use tokio::io::{self, BufReader};

use super::{MetadataDecoder, RecordDecoder, zstd_decoder};

use crate::{
    record::HasRType,
    record_ref::RecordRef,
    Metadata, Result,
};

/// An async decoder for Databento Binary Encoding (DBN), both metadata and records.
pub struct Decoder<R>
where
    R: io::AsyncReadExt + Unpin,
{
    metadata: Metadata,
    decoder: RecordDecoder<R>,
}

impl<R> Decoder<R>
where
    R: io::AsyncReadExt + Unpin,
{
    /// Creates a new async DBN [`Decoder`] from `reader`.
    ///
    /// # Errors
    /// This function will return an error if it is unable to parse the metadata in `reader`.
    pub async fn new(mut reader: R) -> crate::Result<Self> {
        let metadata = MetadataDecoder::new(&mut reader).decode().await?;
        Ok(Self {
            metadata,
            decoder: RecordDecoder::new(reader),
        })
    }

    /// Returns a mutable reference to the inner reader.
    pub fn get_mut(&mut self) -> &mut R {
        self.decoder.get_mut()
    }

    /// Consumes the decoder and returns the inner reader.
    pub fn into_inner(self) -> R {
        self.decoder.into_inner()
    }

    /// Returns a reference to the decoded metadata.
    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    /// Tries to decode a single record and returns a reference to the record that
    /// lasts until the next method call. Returns `None` if `reader` has been
    /// exhausted.
    ///
    /// # Errors
    /// This function returns an error if the underlying reader returns an
    /// error of a kind other than `io::ErrorKind::UnexpectedEof` upon reading.
    ///
    /// If the next record is of a different type than `T`,
    /// this function returns an error of kind `io::ErrorKind::InvalidData`.
    pub async fn decode_record<'a, T: HasRType + 'a>(&'a mut self) -> Result<Option<&T>> {
        self.decoder.decode().await
    }

    /// Tries to decode a single record and returns a reference to the record that
    /// lasts until the next method call. Returns `None` if `reader` has been
    /// exhausted.
    ///
    /// # Errors
    /// This function returns an error if the underlying reader returns an
    /// error of a kind other than `io::ErrorKind::UnexpectedEof` upon reading.
    /// It will also return an error if it encounters an invalid record.
    pub async fn decode_record_ref(&mut self) -> Result<Option<RecordRef>> {
        self.decoder.decode_ref().await
    }
}

impl<'a, R> Decoder<ZstdDecoder<BufReader<R>>>
where
    R: io::AsyncReadExt + Unpin,
{
    /// Creates a new async DBN [`Decoder`] from Zstandard-compressed `reader`.
    ///
    /// # Errors
    /// This function will return an error if it is unable to parse the metadata in `reader`.
    pub async fn with_zstd(reader: R) -> crate::Result<Self> {
        Decoder::new(zstd_decoder(BufReader::new(reader))).await
    }
}

impl<'a, R> Decoder<ZstdDecoder<R>>
where
    R: io::AsyncBufReadExt + Unpin,
{
    /// Creates a new async DBN [`Decoder`] from Zstandard-compressed buffered `reader`.
    ///
    /// # Errors
    /// This function will return an error if it is unable to parse the metadata in `reader`.
    pub async fn with_zstd_buffer(reader: R) -> crate::Result<Self> {
        Decoder::new(zstd_decoder(reader)).await
    }
}
