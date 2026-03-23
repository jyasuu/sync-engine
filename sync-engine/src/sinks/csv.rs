// sync-engine/src/sinks/csv.rs — requires feature "csv"

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::Serialize;
use std::marker::PhantomData;
use std::path::Path;
use tracing::info;

use super::FileSink;

/// Writes serializable records to a CSV file.
///
/// Field names come from the serde field names of `T` — the first batch
/// written to a new file emits the header row automatically.
///
/// ```toml
/// # pipeline.toml
/// [[post_job.steps]]
/// type  = "csv_sink"
/// reads = "db_rows"
/// path  = { env = "OUTPUT__CSV_PATH", default = "/tmp/output.csv" }
/// mode  = "overwrite"   # or "append"
/// ```
pub struct CsvSink<T> {
    /// If true, a header row is written when creating a new file.
    pub write_header: bool,
    _phantom: PhantomData<T>,
}

impl<T: Serialize + Send + Sync> CsvSink<T> {
    pub fn new() -> Self {
        Self { write_header: true, _phantom: PhantomData }
    }

    pub fn without_header(mut self) -> Self {
        self.write_header = false;
        self
    }
}

impl<T: Serialize + Send + Sync> Default for CsvSink<T> {
    fn default() -> Self { Self::new() }
}

#[async_trait]
impl<T: Serialize + Send + Sync> FileSink<T> for CsvSink<T> {
    /// Append rows to an existing CSV, or create with header if new.
    async fn write_batch(&self, items: &[T], path: &Path) -> Result<()> {
        if items.is_empty() { return Ok(()); }

        let exists  = path.exists();
        let file    = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .with_context(|| format!("Cannot open CSV for append: {}", path.display()))?;

        let mut wtr = csv::WriterBuilder::new()
            .has_headers(self.write_header && !exists)
            .from_writer(file);

        for item in items {
            wtr.serialize(item)
                .with_context(|| "CSV serialize failed")?;
        }
        wtr.flush().context("CSV flush failed")?;

        info!(rows = items.len(), path = %path.display(), "CSV append OK");
        Ok(())
    }

    /// Overwrite (or create) the CSV file with a fresh header + all rows.
    async fn write_all(&self, items: &[T], path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("Cannot create dir {}", parent.display()))?;
        }

        let file = std::fs::File::create(path)
            .with_context(|| format!("Cannot create CSV: {}", path.display()))?;

        let mut wtr = csv::WriterBuilder::new()
            .has_headers(self.write_header)
            .from_writer(file);

        for item in items {
            wtr.serialize(item)
                .with_context(|| "CSV serialize failed")?;
        }
        wtr.flush().context("CSV flush failed")?;

        info!(rows = items.len(), path = %path.display(), "CSV write OK");
        Ok(())
    }
}
