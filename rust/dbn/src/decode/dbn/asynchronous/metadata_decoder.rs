use async_compression::tokio::bufread::ZstdDecoder;
use tokio::io::{self, BufReader};

use super::{zstd_decoder, RecordDecoder};

use crate::{
    decode::{
        dbn::{synchronous::MetadataDecoder as SyncMetadataDecoder, DBN_PREFIX, DBN_PREFIX_LEN},
        FromLittleEndianSlice,
    },
    Metadata, Result, DBN_VERSION, METADATA_FIXED_LEN,
};

impl<R> From<MetadataDecoder<R>> for RecordDecoder<R>
where
    R: io::AsyncReadExt + Unpin,
{
    fn from(meta_decoder: MetadataDecoder<R>) -> Self {
        RecordDecoder::new(meta_decoder.into_inner())
    }
}

/// An async decoder for the metadata in files and streams in Databento Binary Encoding (DBN).
pub struct MetadataDecoder<R>
where
    R: io::AsyncReadExt + Unpin,
{
    reader: R,
}

impl<R> MetadataDecoder<R>
where
    R: io::AsyncReadExt + Unpin,
{
    /// Creates a new async DBN [`MetadataDecoder`] from `reader`.
    pub fn new(reader: R) -> Self {
        Self { reader }
    }

    /// Decodes and returns a DBN [`Metadata`].
    ///
    /// # Errors
    /// This function will return an error if it is unable to parse the metadata.
    pub async fn decode(&mut self) -> Result<Metadata> {
        let mut prelude_buffer = [0u8; 8];
        self.reader
            .read_exact(&mut prelude_buffer)
            .await
            .map_err(|e| crate::Error::io(e, "reading metadata prelude"))?;
        if &prelude_buffer[..DBN_PREFIX_LEN] != DBN_PREFIX {
            return Err(crate::Error::decode("Invalid DBN header"));
        }
        let version = prelude_buffer[DBN_PREFIX_LEN];
        if version > DBN_VERSION {
            return Err(crate::Error::decode(format!("Can't decode newer version of DBN. Decoder version is {DBN_VERSION}, input version is {version}")));
        }
        let length = u32::from_le_slice(&prelude_buffer[4..]);
        if (length as usize) < METADATA_FIXED_LEN {
            return Err(crate::Error::decode(
                "Invalid DBN metadata. Metadata length shorter than fixed length.",
            ));
        }

        let mut metadata_buffer = vec![0u8; length as usize];
        self.reader
            .read_exact(&mut metadata_buffer)
            .await
            .map_err(|e| crate::Error::io(e, "reading fixed metadata"))?;
        SyncMetadataDecoder::<std::fs::File>::decode_metadata_fields(version, metadata_buffer)
    }

    /// Returns a mutable reference to the inner reader.
    pub fn get_mut(&mut self) -> &mut R {
        &mut self.reader
    }

    /// Consumes the decoder and returns the inner reader.
    pub fn into_inner(self) -> R {
        self.reader
    }
}

impl<R> MetadataDecoder<ZstdDecoder<BufReader<R>>>
where
    R: io::AsyncReadExt + Unpin,
{
    /// Creates a new async DBN [`MetadataDecoder`] from a Zstandard-compressed `reader`.
    pub fn with_zstd(reader: R) -> Self {
        MetadataDecoder::new(zstd_decoder(BufReader::new(reader)))
    }
}

impl<R> MetadataDecoder<ZstdDecoder<R>>
where
    R: io::AsyncBufReadExt + Unpin,
{
    /// Creates a new async DBN [`MetadataDecoder`] from a Zstandard-compressed buffered `reader`.
    pub fn with_zstd_buffer(reader: R) -> Self {
        MetadataDecoder::new(zstd_decoder(reader))
    }
}
