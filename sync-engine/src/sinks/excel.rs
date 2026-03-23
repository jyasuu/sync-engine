// sync-engine/src/sinks/excel.rs — requires feature "excel"

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::Serialize;
use std::marker::PhantomData;
use std::path::Path;
use tracing::info;

use super::FileSink;

/// Writes serializable records to an Excel (.xlsx) file.
///
/// Each `write_all` call produces a fresh workbook with one sheet named
/// after the struct type. `write_batch` appends rows to an existing file
/// by reading it first, appending in memory, and rewriting — Excel has no
/// native append-only mode.
///
/// Field order matches serde serialization order. The header row is the
/// serde field names of `T`.
///
/// ```toml
/// # pipeline.toml
/// [[post_job.steps]]
/// type       = "excel_sink"
/// reads      = "db_rows"
/// path       = { env = "OUTPUT__EXCEL_PATH", default = "/tmp/output.xlsx" }
/// sheet_name = "users"
/// ```
pub struct ExcelSink<T> {
    pub sheet_name:   String,
    pub write_header: bool,
    _phantom: PhantomData<T>,
}

impl<T: Serialize + Send + Sync> ExcelSink<T> {
    pub fn new(sheet_name: impl Into<String>) -> Self {
        Self {
            sheet_name:   sheet_name.into(),
            write_header: true,
            _phantom:     PhantomData,
        }
    }

    pub fn without_header(mut self) -> Self {
        self.write_header = false;
        self
    }
}

impl<T: Serialize + Send + Sync> Default for ExcelSink<T> {
    fn default() -> Self { Self::new("Sheet1") }
}

// ── Serialization helpers ─────────────────────────────────────────────────

/// Extract field names from a serde-serializable value using a probing serializer.
fn field_names<T: Serialize>(sample: &T) -> Vec<String> {
    let mut prober = FieldNameProber::default();
    let _ = sample.serialize(&mut prober);
    prober.names
}

/// Extract cell values as strings from a serde-serializable value.
fn field_values<T: Serialize>(item: &T) -> Vec<String> {
    let mut extractor = ValueExtractor::default();
    let _ = item.serialize(&mut extractor);
    extractor.values
}

#[async_trait]
impl<T: Serialize + Send + Sync> FileSink<T> for ExcelSink<T> {
    async fn write_batch(&self, items: &[T], path: &Path) -> Result<()> {
        if items.is_empty() { return Ok(()); }

        // Read existing rows if the file exists, then append + rewrite.
        let existing_rows: Vec<Vec<String>> = if path.exists() {
            read_excel_rows(path, &self.sheet_name)?
        } else {
            vec![]
        };

        let header = if !items.is_empty() {
            field_names(&items[0])
        } else {
            vec![]
        };

        let new_rows: Vec<Vec<String>> = items.iter()
            .map(field_values)
            .collect();

        write_excel(path, &self.sheet_name, &header, &existing_rows, &new_rows, self.write_header)?;
        info!(rows = items.len(), path = %path.display(), "Excel append OK");
        Ok(())
    }

    async fn write_all(&self, items: &[T], path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("Cannot create dir {}", parent.display()))?;
        }

        let header = if !items.is_empty() {
            field_names(&items[0])
        } else {
            vec![]
        };

        let rows: Vec<Vec<String>> = items.iter()
            .map(field_values)
            .collect();

        write_excel(path, &self.sheet_name, &header, &[], &rows, self.write_header)?;
        info!(rows = items.len(), path = %path.display(), "Excel write OK");
        Ok(())
    }
}

// ── Internal write helper ─────────────────────────────────────────────────

fn write_excel(
    path:         &Path,
    sheet_name:   &str,
    header:       &[String],
    existing:     &[Vec<String>],
    new_rows:     &[Vec<String>],
    write_header: bool,
) -> Result<()> {
    use rust_xlsxwriter::{Workbook, Format};

    let mut wb    = Workbook::new();
    let ws    = wb.add_worksheet();
    ws.set_name(sheet_name).context("Invalid sheet name")?;

    let bold = Format::new().set_bold();
    let mut row_idx: u32 = 0;

    // Write header
    if write_header && !header.is_empty() {
        for (col, name) in header.iter().enumerate() {
            ws.write_string_with_format(row_idx, col as u16, name, &bold)
                .context("Excel header write failed")?;
        }
        row_idx += 1;
    }

    // Re-emit existing rows (for append mode)
    for row in existing {
        for (col, val) in row.iter().enumerate() {
            ws.write_string(row_idx, col as u16, val)
                .context("Excel existing row write failed")?;
        }
        row_idx += 1;
    }

    // Write new rows
    for row in new_rows {
        for (col, val) in row.iter().enumerate() {
            ws.write_string(row_idx, col as u16, val)
                .context("Excel row write failed")?;
        }
        row_idx += 1;
    }

    wb.save(path)
        .with_context(|| format!("Excel save failed: {}", path.display()))?;
    Ok(())
}

// ── Read existing Excel rows for append ───────────────────────────────────

fn read_excel_rows(path: &Path, sheet_name: &str) -> Result<Vec<Vec<String>>> {
    use calamine::{open_workbook_auto, Reader};

    let mut wb = open_workbook_auto(path)
        .with_context(|| format!("Cannot open Excel: {}", path.display()))?;

    let range = wb.worksheet_range(sheet_name)
        .with_context(|| format!("Sheet \"{}\" not found in {}", sheet_name, path.display()))?;

    let mut rows = Vec::new();
    let mut iter = range.rows();
    iter.next(); // skip header row — we'll rewrite it

    for row in iter {
        let cells: Vec<String> = row.iter()
            .map(|c| c.to_string())
            .collect();
        rows.push(cells);
    }
    Ok(rows)
}

// ── Probing serializers ───────────────────────────────────────────────────
// These are minimal serde Serializer impls that extract field names and
// string values from any Serialize type without needing reflection.

use serde::ser::{self, Impossible};

#[derive(Default)]
struct FieldNameProber {
    names: Vec<String>,
    in_struct: bool,
}

impl ser::Serializer for &mut FieldNameProber {
    type Ok    = ();
    type Error = ProbeError;
    type SerializeStruct = Self;
    type SerializeMap           = Impossible<(), ProbeError>;
    type SerializeSeq           = Impossible<(), ProbeError>;
    type SerializeTuple         = Impossible<(), ProbeError>;
    type SerializeTupleStruct   = Impossible<(), ProbeError>;
    type SerializeTupleVariant  = Impossible<(), ProbeError>;
    type SerializeStructVariant = Impossible<(), ProbeError>;

    fn serialize_struct(self, _n: &'static str, _len: usize) -> Result<Self, ProbeError> {
        self.in_struct = true;
        Ok(self)
    }
    fn serialize_bool(self, _: bool)     -> Result<(), ProbeError> { Ok(()) }
    fn serialize_i8(self, _: i8)         -> Result<(), ProbeError> { Ok(()) }
    fn serialize_i16(self, _: i16)       -> Result<(), ProbeError> { Ok(()) }
    fn serialize_i32(self, _: i32)       -> Result<(), ProbeError> { Ok(()) }
    fn serialize_i64(self, _: i64)       -> Result<(), ProbeError> { Ok(()) }
    fn serialize_u8(self, _: u8)         -> Result<(), ProbeError> { Ok(()) }
    fn serialize_u16(self, _: u16)       -> Result<(), ProbeError> { Ok(()) }
    fn serialize_u32(self, _: u32)       -> Result<(), ProbeError> { Ok(()) }
    fn serialize_u64(self, _: u64)       -> Result<(), ProbeError> { Ok(()) }
    fn serialize_f32(self, _: f32)       -> Result<(), ProbeError> { Ok(()) }
    fn serialize_f64(self, _: f64)       -> Result<(), ProbeError> { Ok(()) }
    fn serialize_char(self, _: char)     -> Result<(), ProbeError> { Ok(()) }
    fn serialize_str(self, _: &str)      -> Result<(), ProbeError> { Ok(()) }
    fn serialize_bytes(self, _: &[u8])   -> Result<(), ProbeError> { Ok(()) }
    fn serialize_none(self)              -> Result<(), ProbeError> { Ok(()) }
    fn serialize_unit(self)              -> Result<(), ProbeError> { Ok(()) }
    fn serialize_unit_struct(self, _: &'static str) -> Result<(), ProbeError> { Ok(()) }
    fn serialize_some<V: Serialize + ?Sized>(self, _: &V) -> Result<(), ProbeError> { Ok(()) }
    fn serialize_unit_variant(self, _: &'static str, _: u32, _: &'static str) -> Result<(), ProbeError> { Ok(()) }
    fn serialize_newtype_struct<V: Serialize + ?Sized>(self, _: &'static str, v: &V) -> Result<(), ProbeError> { v.serialize(self) }
    fn serialize_newtype_variant<V: Serialize + ?Sized>(self, _: &'static str, _: u32, _: &'static str, _: &V) -> Result<(), ProbeError> { Ok(()) }
    fn serialize_seq(self, _: Option<usize>)                -> Result<Impossible<(), ProbeError>, ProbeError> { Err(ProbeError) }
    fn serialize_tuple(self, _: usize)                      -> Result<Impossible<(), ProbeError>, ProbeError> { Err(ProbeError) }
    fn serialize_tuple_struct(self, _: &'static str, _: usize) -> Result<Impossible<(), ProbeError>, ProbeError> { Err(ProbeError) }
    fn serialize_tuple_variant(self, _: &'static str, _: u32, _: &'static str, _: usize) -> Result<Impossible<(), ProbeError>, ProbeError> { Err(ProbeError) }
    fn serialize_map(self, _: Option<usize>)                -> Result<Impossible<(), ProbeError>, ProbeError> { Err(ProbeError) }
    fn serialize_struct_variant(self, _: &'static str, _: u32, _: &'static str, _: usize) -> Result<Impossible<(), ProbeError>, ProbeError> { Err(ProbeError) }
}

impl ser::SerializeStruct for &mut FieldNameProber {
    type Ok = ();
    type Error = ProbeError;
    fn serialize_field<V: Serialize + ?Sized>(&mut self, key: &'static str, _: &V) -> Result<(), ProbeError> {
        self.names.push(key.to_owned());
        Ok(())
    }
    fn end(self) -> Result<(), ProbeError> { Ok(()) }
}

#[derive(Default)]
struct ValueExtractor {
    values: Vec<String>,
}

impl ser::Serializer for &mut ValueExtractor {
    type Ok    = ();
    type Error = ProbeError;
    type SerializeStruct = Self;
    type SerializeMap           = Impossible<(), ProbeError>;
    type SerializeSeq           = Impossible<(), ProbeError>;
    type SerializeTuple         = Impossible<(), ProbeError>;
    type SerializeTupleStruct   = Impossible<(), ProbeError>;
    type SerializeTupleVariant  = Impossible<(), ProbeError>;
    type SerializeStructVariant = Impossible<(), ProbeError>;

    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Self, ProbeError> { Ok(self) }
    fn serialize_bool(self, v: bool)   -> Result<(), ProbeError> { self.values.push(v.to_string()); Ok(()) }
    fn serialize_i64(self, v: i64)     -> Result<(), ProbeError> { self.values.push(v.to_string()); Ok(()) }
    fn serialize_u64(self, v: u64)     -> Result<(), ProbeError> { self.values.push(v.to_string()); Ok(()) }
    fn serialize_f64(self, v: f64)     -> Result<(), ProbeError> { self.values.push(v.to_string()); Ok(()) }
    fn serialize_str(self, v: &str)    -> Result<(), ProbeError> { self.values.push(v.to_owned()); Ok(()) }
    fn serialize_none(self)            -> Result<(), ProbeError> { self.values.push(String::new()); Ok(()) }
    fn serialize_unit(self)            -> Result<(), ProbeError> { self.values.push(String::new()); Ok(()) }
    fn serialize_i8(self, v: i8)       -> Result<(), ProbeError> { self.values.push(v.to_string()); Ok(()) }
    fn serialize_i16(self, v: i16)     -> Result<(), ProbeError> { self.values.push(v.to_string()); Ok(()) }
    fn serialize_i32(self, v: i32)     -> Result<(), ProbeError> { self.values.push(v.to_string()); Ok(()) }
    fn serialize_u8(self, v: u8)       -> Result<(), ProbeError> { self.values.push(v.to_string()); Ok(()) }
    fn serialize_u16(self, v: u16)     -> Result<(), ProbeError> { self.values.push(v.to_string()); Ok(()) }
    fn serialize_u32(self, v: u32)     -> Result<(), ProbeError> { self.values.push(v.to_string()); Ok(()) }
    fn serialize_f32(self, v: f32)     -> Result<(), ProbeError> { self.values.push(v.to_string()); Ok(()) }
    fn serialize_char(self, v: char)   -> Result<(), ProbeError> { self.values.push(v.to_string()); Ok(()) }
    fn serialize_bytes(self, v: &[u8]) -> Result<(), ProbeError> { self.values.push(format!("{v:?}")); Ok(()) }
    fn serialize_unit_struct(self, _: &'static str) -> Result<(), ProbeError> { Ok(()) }
    fn serialize_unit_variant(self, _: &'static str, _: u32, v: &'static str) -> Result<(), ProbeError> { self.values.push(v.to_owned()); Ok(()) }
    fn serialize_some<V: Serialize + ?Sized>(self, v: &V) -> Result<(), ProbeError> { v.serialize(self) }
    fn serialize_newtype_struct<V: Serialize + ?Sized>(self, _: &'static str, v: &V) -> Result<(), ProbeError> { v.serialize(self) }
    fn serialize_newtype_variant<V: Serialize + ?Sized>(self, _: &'static str, _: u32, _: &'static str, v: &V) -> Result<(), ProbeError> { v.serialize(self) }
    fn serialize_seq(self, _: Option<usize>)                -> Result<Impossible<(), ProbeError>, ProbeError> { Err(ProbeError) }
    fn serialize_tuple(self, _: usize)                      -> Result<Impossible<(), ProbeError>, ProbeError> { Err(ProbeError) }
    fn serialize_tuple_struct(self, _: &'static str, _: usize) -> Result<Impossible<(), ProbeError>, ProbeError> { Err(ProbeError) }
    fn serialize_tuple_variant(self, _: &'static str, _: u32, _: &'static str, _: usize) -> Result<Impossible<(), ProbeError>, ProbeError> { Err(ProbeError) }
    fn serialize_map(self, _: Option<usize>)                -> Result<Impossible<(), ProbeError>, ProbeError> { Err(ProbeError) }
    fn serialize_struct_variant(self, _: &'static str, _: u32, _: &'static str, _: usize) -> Result<Impossible<(), ProbeError>, ProbeError> { Err(ProbeError) }
}

impl ser::SerializeStruct for &mut ValueExtractor {
    type Ok = ();
    type Error = ProbeError;
    fn serialize_field<V: Serialize + ?Sized>(&mut self, _: &'static str, value: &V) -> Result<(), ProbeError> {
        value.serialize(&mut **self)
    }
    fn end(self) -> Result<(), ProbeError> { Ok(()) }
}

#[derive(Debug)]
struct ProbeError;
impl std::fmt::Display for ProbeError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { write!(f, "probe error") }
}
impl ser::Error for ProbeError {
    fn custom<T: std::fmt::Display>(_: T) -> Self { ProbeError }
}
impl std::error::Error for ProbeError {}
