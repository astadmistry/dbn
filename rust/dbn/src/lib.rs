//! The official crate for working with the [Databento](https://databento.com) Binary
//! Encoding (DBN), a fast message encoding and storage format for normalized market
//! data. The DBN specification includes a simple metadata header, and a fixed set of
//! struct definitions, which enforce a standardized way to normalize market data.
//!
//! The crate supports reading DBN files and streams and converting them to other
//! [`Encoding`](enums::Encoding)s, as well as updating legacy DBZ files to DBN.
//!
//! This crate provides:
//! - [Decoders](crate::decode) for DBN and DBZ (the precursor to DBN), both
//!   sync and async, with the `async` feature flag
//! - [Encoders](crate::encode) for CSV, DBN, and JSON, both sync and async,
//!   with the `async` feature flag
//! - [Normalized market data struct definitions](crate::record) corresponding to the
//!   different market data schemas offered by Databento
//! - A [wrapper type](crate::RecordRef) for holding a reference to a record struct of
//!   a dynamic type
//! - Helper functions and [macros] for common tasks
//!
//! # Feature flags
//! - `async`: enables async decoding and encoding
//! - `python`: enables `pyo3` bindings
//! - `serde`: enables deriving `serde` traits for types
//! - `trivial_copy`: enables deriving the `Copy` trait for records

// Experimental feature to allow docs.rs to display features
#![cfg_attr(docsrs, feature(doc_auto_cfg))]
#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]
#![deny(clippy::missing_errors_doc)]

/// This module is for decoding.
pub mod decode;
pub mod encode;
pub mod enums;
pub mod error;
#[doc(hidden)]
pub mod json_writer;
pub mod macros;
pub mod metadata;
pub mod pretty;
/// Enumerations for different data sources, venues, and publishers.
pub mod publishers;
#[cfg(feature = "python")]
pub mod python;
pub mod record;
pub mod record_ref;

pub use crate::error::{Error, Result};
pub use crate::metadata::{
    MappingInterval, Metadata, MetadataBuilder, MetadataPrelude, SymbolMapping,
};
pub use crate::record_ref::RecordRef;

/// The current version of the DBN encoding, which is different from the crate version.
pub const DBN_VERSION: u8 = 1;
/// The length of symbol fields (21 characters plus null terminator).
pub const SYMBOL_CSTR_LEN: usize = 22;

const METADATA_DATASET_CSTR_LEN: usize = 16;
const METADATA_RESERVED_LEN: usize = 47;
/// Excludes magic string, version, and length.
const METADATA_FIXED_LEN: usize = 100;
const NULL_LIMIT: u64 = 0;
const NULL_RECORD_COUNT: u64 = u64::MAX;
const NULL_SCHEMA: u16 = u16::MAX;
const NULL_STYPE: u8 = u8::MAX;

/// The denominator of fixed prices in DBN.
pub const FIXED_PRICE_SCALE: i64 = 1_000_000_000;
/// The sentinel value for an unset or null price.
pub const UNDEF_PRICE: i64 = i64::MAX;
/// The sentinel value for an unset or null order quantity.
pub const UNDEF_ORDER_SIZE: u32 = u32::MAX;
/// The sentinel value for an unset or null stat quantity.
pub const UNDEF_STAT_QUANTITY: i32 = i32::MAX;
/// The sentinel value for an unset or null timestamp.
pub const UNDEF_TIMESTAMP: u64 = u64::MAX;

/// Contains dataset code constants.
pub mod datasets {
    use crate::publishers::Dataset;

    /// The dataset code for Databento Equity Basic.
    pub const DBEQ_BASIC: &str = Dataset::DbeqBasic.as_str();
    /// The dataset code for CME Globex MDP 3.0.
    pub const GLBX_MDP3: &str = Dataset::GlbxMdp3.as_str();
    /// The dataset code for OPRA PILLAR.
    pub const OPRA_PILLAR: &str = Dataset::OpraPillar.as_str();
    /// The dataset code for Nasdaq TotalView ITCH.
    pub const XNAS_ITCH: &str = Dataset::XnasItch.as_str();
}
