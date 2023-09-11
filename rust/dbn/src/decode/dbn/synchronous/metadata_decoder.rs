use std::{io, mem, num::NonZeroU64, str::Utf8Error};

use crate::{
    decode::{
        dbn::{decode_iso8601, DBN_PREFIX, DBN_PREFIX_LEN},
        FromLittleEndianSlice,
    },
    enums::{SType, Schema},
    MappingInterval, Metadata, MetadataPrelude, SymbolMapping, DBN_VERSION, METADATA_FIXED_LEN,
    NULL_SCHEMA, NULL_STYPE, UNDEF_TIMESTAMP,
};

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
        let MetadataPrelude { version, length } = self.decode_prelude()?;
        let mut metadata_buffer = vec![0u8; length as usize];
        self.reader
            .read_exact(&mut metadata_buffer)
            .map_err(|e| crate::Error::io(e, "reading fixed metadata"))?;
        Self::decode_metadata_fields(version, metadata_buffer)
    }

    pub(crate) fn decode_prelude(&mut self) -> crate::Result<MetadataPrelude> {
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
        Ok(MetadataPrelude { version, length })
    }

    pub(crate) fn decode_metadata_fields(version: u8, buffer: Vec<u8>) -> crate::Result<Metadata> {
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

#[cfg(test)]
mod tests {
    use std::fs::File;

    use super::*;
    use crate::SYMBOL_CSTR_LEN;

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
}
