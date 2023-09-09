/// This module contains asynchronous DBN decoders.
#[cfg(feature = "async")] pub mod asynchronous;
#[cfg(feature = "async")] pub use asynchronous::AsyncDecoder;
#[cfg(feature = "async")] pub use asynchronous::AsyncMetadataDecoder;
#[cfg(feature = "async")] pub use asynchronous::AsyncRecordDecoder;

/// This module contains synchronous DBN decoders.
pub mod synchronous;
pub use synchronous::Decoder;
pub use synchronous::MetadataDecoder;
pub use synchronous::RecordDecoder;

const DBN_PREFIX: &[u8] = b"DBN";
const DBN_PREFIX_LEN: usize = DBN_PREFIX.len();

/// Returns `true` if `bytes` starts with valid uncompressed DBN.
pub fn starts_with_prefix(bytes: &[u8]) -> bool {
    bytes.len() > DBN_PREFIX_LEN
        && &bytes[..DBN_PREFIX_LEN] == DBN_PREFIX
        && bytes[DBN_PREFIX_LEN] <= crate::DBN_VERSION
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