use std::{
    fs::File,
    io::{self, BufReader},
    path::Path,
};

use super::{MetadataDecoder, RecordDecoder};

use crate::{
    decode::{private::BufferSlice, DecodeDbn, DecodeRecordRef, StreamIterDecoder},
    record::HasRType,
    record_ref::RecordRef,
    Metadata,
};

/// Type for decoding files and streams in Databento Binary Encoding (DBN), both metadata and records.
pub struct Decoder<R>
where
    R: io::Read,
{
    metadata: Metadata,
    decoder: RecordDecoder<R>,
}

impl<R> Decoder<R>
where
    R: io::Read,
{
    /// Creates a new DBN [`Decoder`] from `reader`.
    ///
    /// # Errors
    /// This function will return an error if it is unable to parse the metadata in `reader`.
    pub fn new(mut reader: R) -> crate::Result<Self> {
        let metadata = MetadataDecoder::new(&mut reader).decode()?;
        Ok(Self {
            metadata,
            decoder: RecordDecoder::new(reader),
        })
    }

    /// Returns a mutable reference to the inner reader.
    pub fn get_mut(&mut self) -> &mut R {
        self.decoder.get_mut()
    }

    /// Returns a reference to the inner reader.
    pub fn get_ref(&self) -> &R {
        self.decoder.get_ref()
    }

    /// Consumes the decoder and returns the inner reader.
    pub fn into_inner(self) -> R {
        self.decoder.into_inner()
    }
}

impl<'a, R> Decoder<zstd::stream::Decoder<'a, BufReader<R>>>
where
    R: io::Read,
{
    /// Creates a new DBN [`Decoder`] from Zstandard-compressed `reader`.
    ///
    /// # Errors
    /// This function will return an error if it is unable to parse the metadata in `reader`.
    pub fn with_zstd(reader: R) -> crate::Result<Self> {
        Decoder::new(
            zstd::stream::Decoder::new(reader)
                .map_err(|e| crate::Error::io(e, "creating zstd decoder"))?,
        )
    }
}

impl<'a, R> Decoder<zstd::stream::Decoder<'a, R>>
where
    R: io::BufRead,
{
    /// Creates a new DBN [`Decoder`] from Zstandard-compressed buffered `reader`.
    ///
    /// # Errors
    /// This function will return an error if it is unable to parse the metadata in `reader`.
    pub fn with_zstd_buffer(reader: R) -> crate::Result<Self> {
        Decoder::new(
            zstd::stream::Decoder::with_buffer(reader)
                .map_err(|e| crate::Error::io(e, "creating zstd decoder"))?,
        )
    }
}

impl Decoder<BufReader<File>> {
    /// Creates a DBN [`Decoder`] from the file at `path`.
    ///
    /// # Errors
    /// This function will return an error if it is unable to read the file at `path` or
    /// if it is unable to parse the metadata in the file.
    pub fn from_file(path: impl AsRef<Path>) -> crate::Result<Self> {
        let file = File::open(path.as_ref()).map_err(|e| {
            crate::Error::io(
                e,
                format!(
                    "Error opening DBN file at path '{}'",
                    path.as_ref().display()
                ),
            )
        })?;
        Self::new(BufReader::new(file))
    }
}

impl<'a> Decoder<zstd::stream::Decoder<'a, BufReader<File>>> {
    /// Creates a DBN [`Decoder`] from the Zstandard-compressed file at `path`.
    ///
    /// # Errors
    /// This function will return an error if it is unable to read the file at `path` or
    /// if it is unable to parse the metadata in the file.
    pub fn from_zstd_file(path: impl AsRef<Path>) -> crate::Result<Self> {
        let file = File::open(path.as_ref()).map_err(|e| {
            crate::Error::io(
                e,
                format!(
                    "Error opening Zstandard-compressed DBN file at path '{}'",
                    path.as_ref().display()
                ),
            )
        })?;
        Self::with_zstd(file)
    }
}

impl<R> DecodeRecordRef for Decoder<R>
where
    R: io::Read,
{
    fn decode_record_ref(&mut self) -> crate::Result<Option<RecordRef>> {
        self.decoder.decode_record_ref()
    }
}

impl<R> DecodeDbn for Decoder<R>
where
    R: io::Read,
{
    fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    fn decode_record<T: HasRType>(&mut self) -> crate::Result<Option<&T>> {
        self.decoder.decode()
    }

    fn decode_stream<T: HasRType>(self) -> StreamIterDecoder<Self, T> {
        StreamIterDecoder::new(self)
    }
}

impl<R> BufferSlice for Decoder<R>
where
    R: io::Read,
{
    fn buffer_slice(&self) -> &[u8] {
        self.decoder.buffer_slice()
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::{
        decode::tests::TEST_DATA_PATH,
        encode::{dbn::Encoder, EncodeDbn},
        enums::Schema,
        record::{
            ImbalanceMsg, InstrumentDefMsg, MboMsg, Mbp10Msg, Mbp1Msg, OhlcvMsg, StatMsg, TbboMsg,
            TradeMsg,
        },
    };

    macro_rules! test_dbn_identity {
        ($test_name:ident, $record_type:ident, $schema:expr) => {
            #[test]
            fn $test_name() {
                let file_decoder = Decoder::from_file(format!(
                    "{TEST_DATA_PATH}/test_data.{}.dbn",
                    $schema.as_str()
                ))
                .unwrap();
                let file_metadata = file_decoder.metadata().clone();
                let decoded_records = file_decoder.decode_records::<$record_type>().unwrap();
                let mut buffer = Vec::new();
                Encoder::new(&mut buffer, &file_metadata)
                    .unwrap()
                    .encode_records(decoded_records.as_slice())
                    .unwrap();
                let buf_decoder = Decoder::new(buffer.as_slice()).unwrap();
                assert_eq!(buf_decoder.metadata(), &file_metadata);
                assert_eq!(decoded_records, buf_decoder.decode_records().unwrap());
            }
        };
    }
    macro_rules! test_dbn_zstd_identity {
        ($test_name:ident, $record_type:ident, $schema:expr) => {
            #[test]
            fn $test_name() {
                let file_decoder = Decoder::from_zstd_file(format!(
                    "{TEST_DATA_PATH}/test_data.{}.dbn.zst",
                    $schema.as_str()
                ))
                .unwrap();
                let file_metadata = file_decoder.metadata().clone();
                let decoded_records = file_decoder.decode_records::<$record_type>().unwrap();
                let mut buffer = Vec::new();
                Encoder::with_zstd(&mut buffer, &file_metadata)
                    .unwrap()
                    .encode_records(decoded_records.as_slice())
                    .unwrap();
                let buf_decoder = Decoder::with_zstd(buffer.as_slice()).unwrap();
                assert_eq!(buf_decoder.metadata(), &file_metadata);
                assert_eq!(decoded_records, buf_decoder.decode_records().unwrap());
            }
        };
    }

    test_dbn_identity!(test_dbn_identity_mbo, MboMsg, Schema::Mbo);
    test_dbn_zstd_identity!(test_dbn_zstd_identity_mbo, MboMsg, Schema::Mbo);
    test_dbn_identity!(test_dbn_identity_mbp1, Mbp1Msg, Schema::Mbp1);
    test_dbn_zstd_identity!(test_dbn_zstd_identity_mbp1, Mbp1Msg, Schema::Mbp1);
    test_dbn_identity!(test_dbn_identity_mbp10, Mbp10Msg, Schema::Mbp10);
    test_dbn_zstd_identity!(test_dbn_zstd_identity_mbp10, Mbp10Msg, Schema::Mbp10);
    test_dbn_identity!(test_dbn_identity_ohlcv1d, OhlcvMsg, Schema::Ohlcv1D);
    test_dbn_zstd_identity!(test_dbn_zstd_identity_ohlcv1d, OhlcvMsg, Schema::Ohlcv1D);
    test_dbn_identity!(test_dbn_identity_ohlcv1h, OhlcvMsg, Schema::Ohlcv1H);
    test_dbn_zstd_identity!(test_dbn_zstd_identity_ohlcv1h, OhlcvMsg, Schema::Ohlcv1H);
    test_dbn_identity!(test_dbn_identity_ohlcv1m, OhlcvMsg, Schema::Ohlcv1M);
    test_dbn_zstd_identity!(test_dbn_zstd_identity_ohlcv1m, OhlcvMsg, Schema::Ohlcv1M);
    test_dbn_identity!(test_dbn_identity_ohlcv1s, OhlcvMsg, Schema::Ohlcv1S);
    test_dbn_zstd_identity!(test_dbn_zstd_identity_ohlcv1s, OhlcvMsg, Schema::Ohlcv1S);
    test_dbn_identity!(test_dbn_identity_tbbo, TbboMsg, Schema::Tbbo);
    test_dbn_zstd_identity!(test_dbn_zstd_identity_tbbo, TbboMsg, Schema::Tbbo);
    test_dbn_identity!(test_dbn_identity_trades, TradeMsg, Schema::Trades);
    test_dbn_zstd_identity!(test_dbn_zstd_identity_trades, TradeMsg, Schema::Trades);
    test_dbn_identity!(
        test_dbn_identity_instrument_def,
        InstrumentDefMsg,
        Schema::Definition
    );
    test_dbn_zstd_identity!(
        test_dbn_zstd_identity_instrument_def,
        InstrumentDefMsg,
        Schema::Definition
    );
    test_dbn_identity!(test_dbn_identity_imbalance, ImbalanceMsg, Schema::Imbalance);
    test_dbn_zstd_identity!(
        test_dbn_zstd_identity_imbalance,
        ImbalanceMsg,
        Schema::Imbalance
    );
    test_dbn_identity!(test_dbn_identity_statistics, StatMsg, Schema::Statistics);
    test_dbn_zstd_identity!(
        test_dbn_zstd_identity_statistics,
        StatMsg,
        Schema::Statistics
    );
}
