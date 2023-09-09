//! Decoding of DBN files.
use std::{
    fs::File,
    io::{self, BufReader},
    mem,
    num::NonZeroU64,
    path::Path,
    str::Utf8Error,
};

use super::{
    private::BufferSlice, DecodeDbn, DecodeRecordRef, FromLittleEndianSlice, StreamIterDecoder,
};
use crate::{
    enums::{SType, Schema},
    error::silence_eof_error,
    record::{HasRType, RecordHeader},
    record_ref::RecordRef,
    MappingInterval, Metadata, SymbolMapping, DBN_VERSION, METADATA_FIXED_LEN, NULL_SCHEMA,
    NULL_STYPE, UNDEF_TIMESTAMP,
};

const DBN_PREFIX: &[u8] = b"DBN";
const DBN_PREFIX_LEN: usize = DBN_PREFIX.len();

/// Returns `true` if `bytes` starts with valid uncompressed DBN.
pub fn starts_with_prefix(bytes: &[u8]) -> bool {
    bytes.len() > DBN_PREFIX_LEN
        && &bytes[..DBN_PREFIX_LEN] == DBN_PREFIX
        && bytes[DBN_PREFIX_LEN] <= crate::DBN_VERSION
}

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

    fn decode_stream<T: HasRType>(self) -> super::StreamIterDecoder<Self, T> {
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

/// A DBN decoder of records
pub struct RecordDecoder<R>
where
    R: io::Read,
{
    reader: R,
    buffer: Vec<u8>,
}

impl<R> RecordDecoder<R>
where
    R: io::Read,
{
    /// Creates a new `RecordDecoder` that will decode from `reader`.
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            // `buffer` should have capacity for reading `length`
            buffer: vec![0],
        }
    }

    /// Returns a mutable reference to the inner reader.
    pub fn get_mut(&mut self) -> &mut R {
        &mut self.reader
    }

    /// Returns a reference to the inner reader.
    pub fn get_ref(&self) -> &R {
        &self.reader
    }

    /// Consumes the decoder and returns the inner reader.
    pub fn into_inner(self) -> R {
        self.reader
    }

    /// Tries to decode the next record of type `T`. Returns `Ok(None)` if
    /// the reader is exhausted.
    ///
    /// # Errors
    /// This function returns an error if the underlying reader returns an
    /// error of a kind other than `io::ErrorKind::UnexpectedEof` upon reading.
    ///
    /// If the next record is of a different type than `T`,
    /// this function returns an error of kind `io::ErrorKind::InvalidData`.
    pub fn decode<T: HasRType>(&mut self) -> crate::Result<Option<&T>> {
        let rec_ref = self.decode_record_ref()?;
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

    /// Tries to decode a generic reference a record.
    ///
    /// # Errors
    /// This function returns an error if the underlying reader returns an
    /// error of a kind other than `io::ErrorKind::UnexpectedEof` upon reading.
    /// It will also return an error if it encounters an invalid record.
    pub fn decode_ref(&mut self) -> crate::Result<Option<RecordRef>> {
        let io_err = |e| crate::Error::io(e, "decoding record reference");
        if let Err(err) = self.reader.read_exact(&mut self.buffer[..1]) {
            return silence_eof_error(err).map_err(io_err);
        }
        let length = self.buffer[0] as usize * RecordHeader::LENGTH_MULTIPLIER;
        if length < mem::size_of::<RecordHeader>() {
            return Err(crate::Error::decode(format!(
                "Invalid record with length {length} shorter than header"
            )));
        }
        if length > self.buffer.len() {
            self.buffer.resize(length, 0);
        }
        if let Err(err) = self.reader.read_exact(&mut self.buffer[1..length]) {
            return silence_eof_error(err).map_err(io_err);
        }
        // Safety: `buffer` is resized to contain at least `length` bytes.
        Ok(Some(unsafe { RecordRef::new(self.buffer.as_mut_slice()) }))
    }
}

impl<R> DecodeRecordRef for RecordDecoder<R>
where
    R: io::Read,
{
    fn decode_record_ref(&mut self) -> crate::Result<Option<RecordRef>> {
        self.decode_ref()
    }
}

impl<R> BufferSlice for RecordDecoder<R>
where
    R: io::Read,
{
    fn buffer_slice(&self) -> &[u8] {
        self.buffer.as_slice()
    }
}

/// Type for decoding [`Metadata`] from Databento Binary Encoding (DBN).
pub struct MetadataDecoder<R>
where
    R: io::Read,
{
    reader: R,
}

impl<R> MetadataDecoder<R>
where
    R: io::Read,
{
    const U32_SIZE: usize = mem::size_of::<u32>();

    /// Creates a new DBN [`MetadataDecoder`] from `reader`.
    pub fn new(reader: R) -> Self {
        Self { reader }
    }

    /// Decodes and returns a DBN [`Metadata`].
    ///
    /// # Errors
    /// This function will return an error if it is unable to parse the metadata.
    pub fn decode(&mut self) -> crate::Result<Metadata> {
        let mut prelude_buffer = [0u8; 8];
        self.reader
            .read_exact(&mut prelude_buffer)
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
            .map_err(|e| crate::Error::io(e, "reading fixed metadata"))?;
        Self::decode_metadata_fields(version, metadata_buffer)
    }

    fn decode_metadata_fields(version: u8, buffer: Vec<u8>) -> crate::Result<Metadata> {
        const U64_SIZE: usize = mem::size_of::<u64>();
        let mut pos = 0;
        let dataset = std::str::from_utf8(&buffer[pos..pos + crate::METADATA_DATASET_CSTR_LEN])
            .map_err(|e| crate::Error::utf8(e, "reading dataset from metadata"))?
            // remove null bytes
            .trim_end_matches('\0')
            .to_owned();
        pos += crate::METADATA_DATASET_CSTR_LEN;

        let raw_schema = u16::from_le_slice(&buffer[pos..]);
        let schema = if raw_schema == NULL_SCHEMA {
            None
        } else {
            Some(Schema::try_from(raw_schema).map_err(|_| {
                crate::Error::conversion::<Schema>(format!("{:?}", &buffer[pos..pos + 2]))
            })?)
        };
        pos += mem::size_of::<Schema>();
        let start = u64::from_le_slice(&buffer[pos..]);
        pos += U64_SIZE;
        let end = u64::from_le_slice(&buffer[pos..]);
        pos += U64_SIZE;
        let limit = NonZeroU64::new(u64::from_le_slice(&buffer[pos..]));
        pos += U64_SIZE;
        // skip deprecated record_count
        pos += U64_SIZE;
        let stype_in = if buffer[pos] == NULL_STYPE {
            None
        } else {
            Some(
                SType::try_from(buffer[pos])
                    .map_err(|_| crate::Error::conversion::<SType>(format!("{}", buffer[pos])))?,
            )
        };
        pos += mem::size_of::<SType>();
        let stype_out = SType::try_from(buffer[pos])
            .map_err(|_| crate::Error::conversion::<SType>(format!("{}", buffer[pos])))?;
        pos += mem::size_of::<SType>();
        let ts_out = buffer[pos] != 0;
        pos += mem::size_of::<bool>();
        // skip reserved
        pos += crate::METADATA_RESERVED_LEN;
        let schema_definition_length = u32::from_le_slice(&buffer[pos..]);
        if schema_definition_length != 0 {
            return Err(crate::Error::decode(
                "This version of dbn can't parse schema definitions",
            ));
        }
        pos += Self::U32_SIZE + (schema_definition_length as usize);
        let symbols = Self::decode_repeated_symbol_cstr(buffer.as_slice(), &mut pos)?;
        let partial = Self::decode_repeated_symbol_cstr(buffer.as_slice(), &mut pos)?;
        let not_found = Self::decode_repeated_symbol_cstr(buffer.as_slice(), &mut pos)?;
        let mappings = Self::decode_symbol_mappings(buffer.as_slice(), &mut pos)?;

        Ok(Metadata {
            version,
            dataset,
            schema,
            stype_in,
            stype_out,
            start,
            end: if end == UNDEF_TIMESTAMP {
                None
            } else {
                NonZeroU64::new(end)
            },
            limit,
            ts_out,
            symbols,
            partial,
            not_found,
            mappings,
        })
    }

    fn decode_repeated_symbol_cstr(buffer: &[u8], pos: &mut usize) -> crate::Result<Vec<String>> {
        if *pos + Self::U32_SIZE > buffer.len() {
            return Err(crate::Error::decode("Unexpected end of metadata buffer"));
        }
        let count = u32::from_le_slice(&buffer[*pos..]) as usize;
        *pos += Self::U32_SIZE;
        let read_size = count * crate::SYMBOL_CSTR_LEN;
        if *pos + read_size > buffer.len() {
            return Err(crate::Error::decode("Unexpected end of metadata buffer"));
        }
        let mut res = Vec::with_capacity(count);
        for i in 0..count {
            res.push(Self::decode_symbol(buffer, pos).map_err(|e| {
                crate::Error::utf8(e, format!("Failed to decode symbol at index {i}"))
            })?);
        }
        Ok(res)
    }

    fn decode_symbol_mappings(buffer: &[u8], pos: &mut usize) -> crate::Result<Vec<SymbolMapping>> {
        if *pos + Self::U32_SIZE > buffer.len() {
            return Err(crate::Error::decode("Unexpected end of metadata buffer"));
        }
        let count = u32::from_le_slice(&buffer[*pos..]) as usize;
        *pos += Self::U32_SIZE;
        let mut res = Vec::with_capacity(count);
        // Because each `SymbolMapping` itself is of a variable length, decoding it requires frequent bounds checks
        for i in 0..count {
            res.push(Self::decode_symbol_mapping(i, buffer, pos)?);
        }
        Ok(res)
    }

    fn decode_symbol_mapping(
        idx: usize,
        buffer: &[u8],
        pos: &mut usize,
    ) -> crate::Result<SymbolMapping> {
        const MIN_SYMBOL_MAPPING_ENCODED_LEN: usize =
            crate::SYMBOL_CSTR_LEN + mem::size_of::<u32>();
        const MAPPING_INTERVAL_ENCODED_LEN: usize =
            mem::size_of::<u32>() * 2 + crate::SYMBOL_CSTR_LEN;

        if *pos + MIN_SYMBOL_MAPPING_ENCODED_LEN > buffer.len() {
            return Err(crate::Error::decode(format!(
                "Unexpected end of metadata buffer while parsing symbol mapping at index {idx}"
            )));
        }
        let raw_symbol = Self::decode_symbol(buffer, pos)
            .map_err(|e| crate::Error::utf8(e, "parsing raw symbol"))?;
        let interval_count = u32::from_le_slice(&buffer[*pos..]) as usize;
        *pos += Self::U32_SIZE;
        let read_size = interval_count * MAPPING_INTERVAL_ENCODED_LEN;
        if *pos + read_size > buffer.len() {
            return Err(crate::Error::decode(format!(
                "Symbol mapping at index {idx} with interval_count ({interval_count}) doesn't match size of buffer \
                which only contains space for {} intervals",
                (buffer.len() - *pos) / MAPPING_INTERVAL_ENCODED_LEN
            )));
        }
        let mut intervals = Vec::with_capacity(interval_count);
        for i in 0..interval_count {
            let raw_start_date = u32::from_le_slice(&buffer[*pos..]);
            *pos += Self::U32_SIZE;
            let start_date = decode_iso8601(raw_start_date).map_err(|e| {
                crate::Error::decode(format!("{e} while parsing start date of mapping interval at index {i} within mapping at index {idx}"))
            })?;
            let raw_end_date = u32::from_le_slice(&buffer[*pos..]);
            *pos += Self::U32_SIZE;
            let end_date = decode_iso8601(raw_end_date).map_err(|e| {
                crate::Error::decode(format!("{e} while parsing end date of mapping interval at index {i} within mapping at index {idx}"))
            })?;
            let symbol = Self::decode_symbol(buffer, pos).map_err(|e| {
                crate::Error::utf8(e, format!("Failed to parse symbol for mapping interval at index {i} within mapping at index {idx}"))
            })?;
            intervals.push(MappingInterval {
                start_date,
                end_date,
                symbol,
            });
        }
        Ok(SymbolMapping {
            raw_symbol,
            intervals,
        })
    }

    fn decode_symbol(buffer: &[u8], pos: &mut usize) -> Result<String, Utf8Error> {
        let symbol_slice = &buffer[*pos..*pos + crate::SYMBOL_CSTR_LEN];
        let symbol = std::str::from_utf8(symbol_slice)?
            // remove null bytes
            .trim_end_matches('\0')
            .to_owned();
        *pos += crate::SYMBOL_CSTR_LEN;
        Ok(symbol)
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

pub(crate) fn decode_iso8601(raw: u32) -> Result<time::Date, String> {
    let year = raw / 10_000;
    let remaining = raw % 10_000;
    let raw_month = remaining / 100;
    let month = u8::try_from(raw_month)
        .map_err(|e| format!("Error {e:?} while parsing {raw} into date"))
        .and_then(|m| {
            time::Month::try_from(m)
                .map_err(|e| format!("Error {e:?} while parsing {raw} into date"))
        })?;
    let day = remaining % 100;
    time::Date::from_calendar_date(year as i32, month, day as u8)
        .map_err(|e| format!("Couldn't convert {raw} to a valid date: {e:?}"))
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use super::*;
    use crate::{
        datasets::XNAS_ITCH,
        decode::tests::TEST_DATA_PATH,
        encode::{dbn::Encoder, EncodeDbn, EncodeRecord},
        enums::rtype,
        record::{
            ErrorMsg, ImbalanceMsg, InstrumentDefMsg, MboMsg, Mbp10Msg, Mbp1Msg, OhlcvMsg,
            RecordHeader, StatMsg, TbboMsg, TradeMsg,
        },
        Error, MetadataBuilder, SYMBOL_CSTR_LEN,
    };

    #[test]
    fn test_decode_symbol() {
        let bytes = b"SPX.1.2\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0";
        assert_eq!(bytes.len(), SYMBOL_CSTR_LEN);
        let mut pos = 0;
        let res = MetadataDecoder::<File>::decode_symbol(bytes.as_slice(), &mut pos).unwrap();
        assert_eq!(pos, crate::SYMBOL_CSTR_LEN);
        assert_eq!(&res, "SPX.1.2");
    }

    #[test]
    fn test_decode_symbol_invalid_utf8() {
        const BYTES: [u8; SYMBOL_CSTR_LEN] = [
            // continuation byte
            0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ];
        let mut pos = 0;
        let res = MetadataDecoder::<File>::decode_symbol(BYTES.as_slice(), &mut pos);
        assert!(res.is_err());
    }

    #[test]
    fn test_decode_iso8601_valid() {
        let res = decode_iso8601(20151031).unwrap();
        let exp: time::Date =
            time::Date::from_calendar_date(2015, time::Month::October, 31).unwrap();
        assert_eq!(res, exp);
    }

    #[test]
    fn test_decode_iso8601_invalid_month() {
        let res = decode_iso8601(20101305);
        dbg!(&res);
        assert!(matches!(res, Err(e) if e.contains("month")));
    }

    #[test]
    fn test_decode_iso8601_invalid_day() {
        let res = decode_iso8601(20100600);
        dbg!(&res);
        assert!(matches!(res, Err(e) if e.contains("a valid date")));
    }

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

    #[test]
    fn test_decode_record_ref() {
        let mut buffer = Vec::new();
        let mut encoder = Encoder::new(
            &mut buffer,
            &MetadataBuilder::new()
                .dataset(XNAS_ITCH.to_owned())
                .schema(Some(Schema::Mbo))
                .start(0)
                .stype_in(Some(SType::InstrumentId))
                .stype_out(SType::InstrumentId)
                .build(),
        )
        .unwrap();
        const OHLCV_MSG: OhlcvMsg = OhlcvMsg {
            hd: RecordHeader::new::<OhlcvMsg>(rtype::OHLCV_1S, 1, 1, 0),
            open: 100,
            high: 200,
            low: 75,
            close: 125,
            volume: 65,
        };
        let error_msg: ErrorMsg = ErrorMsg::new(0, "Test failed successfully");
        encoder.encode_record(&OHLCV_MSG).unwrap();
        encoder.encode_record(&error_msg).unwrap();

        let mut decoder = Decoder::new(buffer.as_slice()).unwrap();
        let ref1 = decoder.decode_record_ref().unwrap().unwrap();
        assert_eq!(*ref1.get::<OhlcvMsg>().unwrap(), OHLCV_MSG);
        let ref2 = decoder.decode_record_ref().unwrap().unwrap();
        assert_eq!(*ref2.get::<ErrorMsg>().unwrap(), error_msg);
        assert!(decoder.decode_record_ref().unwrap().is_none());
    }

    #[test]
    fn test_decode_record_0_length() {
        let buf = vec![0];
        let mut target = RecordDecoder::new(buf.as_slice());
        assert!(
            matches!(target.decode_ref(), Err(Error::Decode(msg)) if msg.starts_with("Invalid record with length"))
        );
    }

    #[test]
    fn test_decode_record_length_less_than_header() {
        let buf = vec![3u8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];
        assert_eq!(buf[0] as usize * RecordHeader::LENGTH_MULTIPLIER, buf.len());

        let mut target = RecordDecoder::new(buf.as_slice());
        assert!(
            matches!(target.decode_ref(), Err(Error::Decode(msg)) if msg.starts_with("Invalid record with length"))
        );
    }

    #[test]
    fn test_decode_record_length_longer_than_buffer() {
        let rec = ErrorMsg::new(1680703198000000000, "Test");
        let mut target = RecordDecoder::new(&rec.as_ref()[..rec.record_size() - 1]);
        assert!(matches!(target.decode_ref(), Ok(None)));
    }

    #[test]
    fn test_decode_multiframe_zst() {
        let mut decoder = RecordDecoder::new(
            zstd::stream::Decoder::new(
                File::open(format!(
                    "{TEST_DATA_PATH}/multi-frame.definition.dbn.frag.zst"
                ))
                .unwrap(),
            )
            .unwrap(),
        );
        let mut count = 0;
        while let Some(_rec) = decoder.decode::<InstrumentDefMsg>().unwrap() {
            count += 1;
        }
        assert_eq!(count, 8);
    }
}

#[cfg(feature = "async")]
pub use r#async::{
    Decoder as AsyncDecoder, MetadataDecoder as AsyncMetadataDecoder,
    RecordDecoder as AsyncRecordDecoder,
};

#[cfg(feature = "async")]
mod r#async {
    use async_compression::tokio::bufread::ZstdDecoder;
    use tokio::io::{self, BufReader};

    use crate::{
        decode::FromLittleEndianSlice,
        error::silence_eof_error,
        record::{HasRType, RecordHeader},
        record_ref::RecordRef,
        Metadata, Result, DBN_VERSION, METADATA_FIXED_LEN,
    };

    /// Helper to always set multiple members.
    fn zstd_decoder<R>(reader: R) -> ZstdDecoder<R>
    where
        R: io::AsyncBufReadExt + Unpin,
    {
        let mut zstd_decoder = ZstdDecoder::new(reader);
        // explicitly enable decoding multiple frames
        zstd_decoder.multiple_members(true);
        zstd_decoder
    }

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
            if &prelude_buffer[..super::DBN_PREFIX_LEN] != super::DBN_PREFIX {
                return Err(crate::Error::decode("Invalid DBN header"));
            }
            let version = prelude_buffer[super::DBN_PREFIX_LEN];
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
            super::MetadataDecoder::<std::fs::File>::decode_metadata_fields(
                version,
                metadata_buffer,
            )
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

    #[cfg(test)]
    mod tests {
        use tokio::io::AsyncWriteExt;

        use super::*;
        use crate::{
            decode::tests::TEST_DATA_PATH,
            encode::dbn::{AsyncMetadataEncoder, AsyncRecordEncoder},
            enums::{rtype, Schema},
            record::{
                ErrorMsg, ImbalanceMsg, InstrumentDefMsg, MboMsg, Mbp10Msg, Mbp1Msg, OhlcvMsg,
                RecordHeader, StatMsg, TbboMsg, TradeMsg, WithTsOut,
            },
            Error,
        };

        macro_rules! test_dbn_identity {
            ($test_name:ident, $record_type:ident, $schema:expr) => {
                #[tokio::test]
                async fn $test_name() {
                    let mut file = tokio::fs::File::open(format!(
                        "{TEST_DATA_PATH}/test_data.{}.dbn",
                        $schema
                    ))
                    .await
                    .unwrap();
                    let file_metadata = MetadataDecoder::new(&mut file).decode().await.unwrap();
                    let mut file_decoder = RecordDecoder::new(&mut file);
                    let mut file_records = Vec::new();
                    while let Ok(Some(record)) = file_decoder.decode::<$record_type>().await {
                        file_records.push(record.clone());
                    }
                    let mut buffer = Vec::new();
                    AsyncMetadataEncoder::new(&mut buffer)
                        .encode(&file_metadata)
                        .await
                        .unwrap();
                    assert_eq!(file_records.is_empty(), $schema == Schema::Ohlcv1D);
                    let mut buf_encoder = AsyncRecordEncoder::new(&mut buffer);
                    for record in file_records.iter() {
                        buf_encoder.encode(record).await.unwrap();
                    }
                    let mut buf_cursor = std::io::Cursor::new(&mut buffer);
                    let buf_metadata = MetadataDecoder::new(&mut buf_cursor)
                        .decode()
                        .await
                        .unwrap();
                    assert_eq!(buf_metadata, file_metadata);
                    let mut buf_decoder = RecordDecoder::new(&mut buf_cursor);
                    let mut buf_records = Vec::new();
                    while let Ok(Some(record)) = buf_decoder.decode::<$record_type>().await {
                        buf_records.push(record.clone());
                    }
                    assert_eq!(buf_records, file_records);
                }
            };
        }

        macro_rules! test_dbn_zstd_identity {
            ($test_name:ident, $record_type:ident, $schema:expr) => {
                #[tokio::test]
                async fn $test_name() {
                    let file = tokio::fs::File::open(format!(
                        "{TEST_DATA_PATH}/test_data.{}.dbn.zst",
                        $schema
                    ))
                    .await
                    .unwrap();
                    let mut file_decoder = Decoder::with_zstd(file).await.unwrap();
                    let file_metadata = file_decoder.metadata().clone();
                    let mut file_records = Vec::new();
                    while let Ok(Some(record)) = file_decoder.decode_record::<$record_type>().await
                    {
                        file_records.push(record.clone());
                    }
                    let mut buffer = Vec::new();
                    let mut meta_encoder = AsyncMetadataEncoder::with_zstd(&mut buffer);
                    meta_encoder.encode(&file_metadata).await.unwrap();
                    assert_eq!(file_records.is_empty(), $schema == Schema::Ohlcv1D);
                    let mut buf_encoder = AsyncRecordEncoder::from(meta_encoder);
                    for record in file_records.iter() {
                        buf_encoder.encode(record).await.unwrap();
                    }
                    buf_encoder.into_inner().shutdown().await.unwrap();
                    let mut buf_cursor = std::io::Cursor::new(&mut buffer);
                    let mut buf_decoder = Decoder::with_zstd_buffer(&mut buf_cursor).await.unwrap();
                    let buf_metadata = buf_decoder.metadata().clone();
                    assert_eq!(buf_metadata, file_metadata);
                    let mut buf_records = Vec::new();
                    while let Ok(Some(record)) = buf_decoder.decode_record::<$record_type>().await {
                        buf_records.push(record.clone());
                    }
                    assert_eq!(buf_records, file_records);
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

        #[tokio::test]
        async fn test_dbn_identity_with_ts_out() {
            let rec1 = WithTsOut {
                rec: OhlcvMsg {
                    hd: RecordHeader::new::<WithTsOut<OhlcvMsg>>(
                        rtype::OHLCV_1D,
                        1,
                        446,
                        1678284110,
                    ),
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
            let res1_without = *decoder_without
                .decode::<OhlcvMsg>()
                .await
                .unwrap()
                .unwrap();
            let res2_without = *decoder_without
                .decode::<OhlcvMsg>()
                .await
                .unwrap()
                .unwrap();
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
}
