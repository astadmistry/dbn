use std::{io, mem};

use crate::{
    decode::{private::BufferSlice, DecodeRecordRef},
    error::silence_eof_error,
    record::{HasRType, RecordHeader},
    record_ref::RecordRef,
};

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

#[cfg(test)]
mod tests {
    use std::fs::File;

    use super::*;
    use crate::{
        datasets::XNAS_ITCH,
        decode::{dbn::synchronous::Decoder, tests::TEST_DATA_PATH},
        encode::{dbn::Encoder, EncodeRecord},
        enums::{rtype, SType, Schema},
        record::{ErrorMsg, InstrumentDefMsg, OhlcvMsg, RecordHeader},
        Error, MetadataBuilder,
    };

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
