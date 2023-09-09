/// Decoder module
pub mod decoder;
pub use decoder::Decoder;
pub use decoder::Decoder as AsyncDecoder;

/// Metadata decoder module
pub mod metadata_decoder;
pub use metadata_decoder::MetadataDecoder;
pub use metadata_decoder::MetadataDecoder as AsyncMetadataDecoder;

/// Record decoder module
pub mod record_decoder;
pub use record_decoder::RecordDecoder;
pub use record_decoder::RecordDecoder as AsyncRecordDecoder;

use async_compression::tokio::bufread::ZstdDecoder;
use tokio::io;

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

#[cfg(test)]
mod tests {
    use tokio::io::AsyncWriteExt;

    use super::*;

    use crate::{
        decode::tests::TEST_DATA_PATH,
        encode::dbn::{AsyncMetadataEncoder, AsyncRecordEncoder},
        enums::Schema,
        record::{
            ImbalanceMsg, InstrumentDefMsg, MboMsg, Mbp10Msg, Mbp1Msg, OhlcvMsg, StatMsg, TbboMsg, TradeMsg,
        },
    };

    macro_rules! test_dbn_identity {
        ($test_name:ident, $record_type:ident, $schema:expr) => {
            #[tokio::test]
            async fn $test_name() {
                let mut file =
                    tokio::fs::File::open(format!("{TEST_DATA_PATH}/test_data.{}.dbn", $schema))
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
                while let Ok(Some(record)) = file_decoder.decode_record::<$record_type>().await {
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
}
