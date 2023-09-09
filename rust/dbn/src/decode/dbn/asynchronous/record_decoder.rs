use async_compression::tokio::bufread::ZstdDecoder;
use tokio::io::{self, BufReader};

use super::zstd_decoder;

use crate::{
    error::silence_eof_error,
    record::{HasRType, RecordHeader},
    record_ref::RecordRef, Result,
};

/// An async decoder for files and streams of Databento Binary Encoding (DBN) records.
pub struct RecordDecoder<R>
where
    R: io::AsyncReadExt + Unpin,
{
    reader: R,
    buffer: Vec<u8>,
}

impl<R> RecordDecoder<R>
where
    R: io::AsyncReadExt + Unpin,
{
    /// Creates a new DBN [`RecordDecoder`] from `reader`.
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            // `buffer` should have capacity for reading `length`
            buffer: vec![0],
        }
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
    pub async fn decode<'a, T: HasRType + 'a>(&'a mut self) -> Result<Option<&T>> {
        let rec_ref = self.decode_ref().await?;
        if let Some(rec_ref) = rec_ref {
            rec_ref
                .get::<T>()
                .ok_or_else(|| {
                    crate::Error::conversion::<T>(format!(
                        "record with rtype {}",
                        rec_ref.header().rtype
                    ))
                })
                .map(Some)
        } else {
            Ok(None)
        }
    }

    /// Tries to decode a single record and returns a reference to the record that
    /// lasts until the next method call. Returns `None` if `reader` has been
    /// exhausted.
    ///
    /// # Errors
    /// This function returns an error if the underlying reader returns an
    /// error of a kind other than `io::ErrorKind::UnexpectedEof` upon reading.
    /// It will also return an error if it encounters an invalid record.
    pub async fn decode_ref(&mut self) -> Result<Option<RecordRef>> {
        let io_err = |e| crate::Error::io(e, "decoding record reference");
        if let Err(err) = self.reader.read_exact(&mut self.buffer[..1]).await {
            return silence_eof_error(err).map_err(io_err);
        }
        let length = self.buffer[0] as usize * RecordHeader::LENGTH_MULTIPLIER;
        if length > self.buffer.len() {
            self.buffer.resize(length, 0);
        }
        if length < std::mem::size_of::<RecordHeader>() {
            return Err(crate::Error::decode(format!(
                "Invalid record with length {length} shorter than header"
            )));
        }
        if let Err(err) = self.reader.read_exact(&mut self.buffer[1..length]).await {
            return silence_eof_error(err).map_err(io_err);
        }
        // Safety: `buffer` is resized to contain at least `length` bytes.
        Ok(Some(unsafe { RecordRef::new(self.buffer.as_mut_slice()) }))
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

impl<R> RecordDecoder<ZstdDecoder<BufReader<R>>>
where
    R: io::AsyncReadExt + Unpin,
{
    /// Creates a new async DBN [`RecordDecoder`] from a Zstandard-compressed `reader`.
    pub fn with_zstd(reader: R) -> Self {
        RecordDecoder::new(zstd_decoder(BufReader::new(reader)))
    }
}

impl<R> RecordDecoder<ZstdDecoder<R>>
where
    R: io::AsyncBufReadExt + Unpin,
{
    /// Creates a new async DBN [`RecordDecoder`] from a Zstandard-compressed buffered `reader`.
    pub fn with_zstd_buffer(reader: R) -> Self {
        RecordDecoder::new(zstd_decoder(reader))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    use crate::{
        decode::tests::TEST_DATA_PATH,
        encode::dbn::AsyncRecordEncoder,
        enums::rtype,
        record::{
            ErrorMsg, InstrumentDefMsg, OhlcvMsg,
            RecordHeader, WithTsOut,
        },
        Error,
    };

    #[tokio::test]
    async fn test_dbn_identity_with_ts_out() {
        let rec1 = WithTsOut {
            rec: OhlcvMsg {
                hd: RecordHeader::new::<WithTsOut<OhlcvMsg>>(rtype::OHLCV_1D, 1, 446, 1678284110),
                open: 160270000000,
                high: 161870000000,
                low: 157510000000,
                close: 158180000000,
                volume: 3170000,
            },
            ts_out: 1678486110,
        };
        let mut rec2 = rec1;
        rec2.rec.hd.instrument_id += 1;
        rec2.ts_out = 1678486827;
        let mut buffer = Vec::new();
        let mut encoder = AsyncRecordEncoder::new(&mut buffer);
        encoder.encode(&rec1).await.unwrap();
        encoder.encode(&rec2).await.unwrap();
        let mut decoder_with = RecordDecoder::new(buffer.as_slice());
        let res1_with = *decoder_with
            .decode::<WithTsOut<OhlcvMsg>>()
            .await
            .unwrap()
            .unwrap();
        let res2_with = *decoder_with
            .decode::<WithTsOut<OhlcvMsg>>()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(rec1, res1_with);
        assert_eq!(rec2, res2_with);
        let mut decoder_without = RecordDecoder::new(buffer.as_slice());
        let res1_without = *decoder_without.decode::<OhlcvMsg>().await.unwrap().unwrap();
        let res2_without = *decoder_without.decode::<OhlcvMsg>().await.unwrap().unwrap();
        assert_eq!(rec1.rec, res1_without);
        assert_eq!(rec2.rec, res2_without);
    }

    #[tokio::test]
    async fn test_decode_record_0_length() {
        let buf = vec![0];
        let mut target = RecordDecoder::new(buf.as_slice());
        assert!(
            matches!(target.decode_ref().await, Err(Error::Decode(msg)) if msg.starts_with("Invalid record with length"))
        );
    }

    #[tokio::test]
    async fn test_decode_record_length_less_than_header() {
        let buf = vec![3u8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];
        assert_eq!(buf[0] as usize * RecordHeader::LENGTH_MULTIPLIER, buf.len());

        let mut target = RecordDecoder::new(buf.as_slice());
        assert!(
            matches!(target.decode_ref().await, Err(Error::Decode(msg)) if msg.starts_with("Invalid record with length"))
        );
    }

    #[tokio::test]
    async fn test_decode_record_length_longer_than_buffer() {
        let rec = ErrorMsg::new(1680703198000000000, "Test");
        let mut target = RecordDecoder::new(&rec.as_ref()[..rec.record_size() - 1]);
        assert!(matches!(target.decode_ref().await, Ok(None)));
    }

    #[tokio::test]
    async fn test_decode_multiframe_zst() {
        let mut decoder = RecordDecoder::with_zstd(
            tokio::fs::File::open(format!(
                "{TEST_DATA_PATH}/multi-frame.definition.dbn.frag.zst"
            ))
            .await
            .unwrap(),
        );
        let mut count = 0;
        while let Some(_rec) = decoder.decode::<InstrumentDefMsg>().await.unwrap() {
            count += 1;
        }
        assert_eq!(count, 8);
    }
}
