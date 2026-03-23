// sync-engine/src/sinks/mod.rs
//
// File-based sinks. Each backend is gated behind a Cargo feature:
//
//   [dependencies]
//   sync-engine = { features = ["csv"] }       # CSV only
//   sync-engine = { features = ["excel"] }     # Excel only
//   sync-engine = { features = ["csv","excel"] }
//
// The FileSink trait is always available so downstream code can be written
// generically. Only the implementations are feature-gated.

use anyhow::Result;
use async_trait::async_trait;
use std::path::Path;

/// Write a typed batch to a file path.
/// Implemented by CsvSink and ExcelSink.
#[async_trait]
pub trait FileSink<T: Send + Sync>: Send + Sync {
    /// Append `items` to the file at `path`, creating it if needed.
    /// CSV appends rows; Excel appends rows to the first sheet.
    async fn write_batch(&self, items: &[T], path: &Path) -> Result<()>;

    /// Write `items` to `path`, overwriting any existing file.
    async fn write_all(&self, items: &[T], path: &Path) -> Result<()>;
}

#[cfg(feature = "csv")]
pub mod csv;

#[cfg(feature = "excel")]
pub mod excel;

#[cfg(feature = "csv")]
pub use csv::CsvSink;

#[cfg(feature = "excel")]
pub use excel::ExcelSink;
