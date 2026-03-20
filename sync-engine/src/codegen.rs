// sync-engine/src/codegen.rs
//
// Codegen library. Downstream build.rs scripts call:
//
//   fn main() {
//       sync_engine::codegen::generate("schema.toml");
//   }
//
// This works because sync-engine is listed in BOTH [dependencies] and
// [build-dependencies] — Cargo compiles it twice (host + target), no conflict.

use serde::Deserialize;
use std::collections::HashMap;
use std::fmt::Write as FmtWrite;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Deserialize)]
pub struct Schema {
    pub record: HashMap<String, RecordDef>,
    pub mapping: HashMap<String, MappingDef>,
}

#[derive(Debug, Deserialize)]
pub struct RecordDef {
    #[serde(default)]
    pub serde_rename: Option<String>,
    #[serde(default)]
    pub fetcher: Option<FetcherHint>,
    #[serde(default)]
    pub sink: Option<SinkHint>,
    pub fields: Vec<FieldDef>,
}

#[derive(Debug, Deserialize)]
pub struct FetcherHint {
    pub envelope_field: String,
}

#[derive(Debug, Deserialize)]
pub struct SinkHint {
    pub table: String,
    pub primary_key: String,
    #[serde(default)]
    pub extra_copy: Vec<String>,
    #[serde(default)]
    pub upsert: bool,
}

#[derive(Debug, Deserialize)]
pub struct FieldDef {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: String,
}

#[derive(Debug, Deserialize)]
pub struct MappingDef {
    pub name: String,
    pub from: String,
    pub to: String,
    pub rules: Vec<RuleDef>,
}

#[derive(Debug, Deserialize)]
pub struct RuleDef {
    pub field: String,
    pub rule: String,
}

fn gen_struct(name: &str, def: &RecordDef) -> String {
    let mut out = String::new();
    writeln!(out, "#[derive(Debug, Clone, serde::Deserialize)]").unwrap();
    if let Some(ref s) = def.serde_rename {
        writeln!(out, "#[serde(rename_all = \"{s}\")]").unwrap();
    }
    writeln!(out, "pub struct {name} {{").unwrap();
    for f in &def.fields {
        let has_dt = f.ty.contains("DateTime");
        if has_dt && f.ty.starts_with("Option") {
            writeln!(
                out,
                "    #[serde(default, with = \"chrono::serde::ts_milliseconds_option\")]"
            )
            .unwrap();
        } else if has_dt {
            writeln!(
                out,
                "    #[serde(with = \"chrono::serde::ts_milliseconds\")]"
            )
            .unwrap();
        }
        writeln!(out, "    pub {}: {},", f.name, f.ty).unwrap();
    }
    writeln!(out, "}}").unwrap();
    out
}

fn gen_envelope(record_name: &str, hint: &FetcherHint) -> String {
    let field = &hint.envelope_field;
    let mut out = String::new();
    writeln!(out, "#[allow(dead_code)]").unwrap();
    writeln!(out, "#[derive(Debug, serde::Deserialize)]").unwrap();
    writeln!(out, "#[allow(dead_code)]").unwrap();
    writeln!(
        out,
        "#[allow(dead_code)]
pub struct {record_name}Response {{"
    )
    .unwrap();
    writeln!(out, "    pub timestamp:   i64,").unwrap();
    writeln!(out, "    pub status_code: i64,").unwrap();
    writeln!(out, "    pub {field}:     Vec<{record_name}>,").unwrap();
    writeln!(out, "}}").unwrap();
    writeln!(
        out,
        "impl sync_engine::HasEnvelope for {record_name}Response {{"
    )
    .unwrap();
    writeln!(out, "    type Item = {record_name};").unwrap();
    writeln!(
        out,
        "    fn into_items(self) -> Vec<{record_name}> {{ self.{field} }}"
    )
    .unwrap();
    writeln!(out, "}}").unwrap();
    out
}

fn gen_upsert(record_name: &str, hint: &SinkHint, fields: &[FieldDef]) -> String {
    let table = &hint.table;
    let pk = &hint.primary_key;

    let col_list: Vec<&str> = fields.iter().map(|f| f.name.as_str()).collect();
    let placeholders: Vec<String> = col_list
        .iter()
        .enumerate()
        .map(|(i, _)| format!("${}", i + 1))
        .collect();

    let extra_col_names: Vec<&str> = hint
        .extra_copy
        .iter()
        .map(|s| s.split('=').next().unwrap().trim())
        .collect();
    let extra_assigns: Vec<String> = hint
        .extra_copy
        .iter()
        .map(|s| {
            let parts: Vec<&str> = s.split('=').map(str::trim).collect();
            let idx = col_list.iter().position(|c| *c == parts[1]).unwrap();
            format!("{} = ${}", parts[0], idx + 1)
        })
        .collect();

    let update_set: Vec<String> = col_list
        .iter()
        .filter(|c| **c != pk)
        .map(|c| format!("{c} = EXCLUDED.{c}"))
        .collect();

    let all_cols = if extra_col_names.is_empty() {
        col_list.join(", ")
    } else {
        format!("{}, {}", col_list.join(", "), extra_col_names.join(", "))
    };
    let all_vals = if extra_assigns.is_empty() {
        placeholders.join(", ")
    } else {
        format!("{}, {}", placeholders.join(", "), extra_assigns.join(", "))
    };
    let upsert_clause = if hint.upsert {
        format!("ON CONFLICT ({pk}) DO UPDATE SET {}", update_set.join(", "))
    } else {
        String::new()
    };
    let sql = format!("INSERT INTO {table} ({all_cols}) VALUES ({all_vals}) {upsert_clause}");

    let mut out = String::new();
    writeln!(out, "#[async_trait::async_trait]").unwrap();
    writeln!(out, "impl sync_engine::Upsertable for {record_name} {{").unwrap();
    writeln!(
        out,
        "    async fn upsert(&self, pool: &sqlx::PgPool) -> anyhow::Result<()> {{"
    )
    .unwrap();
    writeln!(out, "        sqlx::query(\"{sql}\")").unwrap();
    for f in fields {
        writeln!(out, "            .bind(&self.{})", f.name).unwrap();
    }
    writeln!(
        out,
        "            .execute(pool).await.map(|_| ()).map_err(Into::into)"
    )
    .unwrap();
    writeln!(out, "    }}").unwrap();
    writeln!(out, "}}").unwrap();
    out
}

fn gen_transform(mapping: &MappingDef) -> String {
    let mut out = String::new();
    let (name, from, to) = (&mapping.name, &mapping.from, &mapping.to);
    writeln!(out, "pub struct {name};").unwrap();
    writeln!(out, "impl sync_engine::Transform for {name} {{").unwrap();
    writeln!(out, "    type Input  = {from};").unwrap();
    writeln!(out, "    type Output = {to};").unwrap();
    writeln!(
        out,
        "    fn apply(&self, u: {from}) -> anyhow::Result<{to}> {{"
    )
    .unwrap();
    writeln!(out, "        Ok({to} {{").unwrap();
    for rule in &mapping.rules {
        let expr = gen_rule_expr(&rule.field, &rule.rule);
        writeln!(out, "            {}: {expr},", rule.field).unwrap();
    }
    writeln!(out, "        }})").unwrap();
    writeln!(out, "    }}").unwrap();
    writeln!(out, "}}").unwrap();
    out
}

fn gen_rule_expr(field: &str, rule: &str) -> String {
    match rule {
        "copy" => format!("u.{field}"),
        "null_to_empty" => format!("u.{field}.unwrap_or_default()"),
        "bool_to_yn" => format!("if u.{field} {{ \"Y\".to_owned() }} else {{ \"N\".to_owned() }}"),
        "epoch_ms_to_ts" => format!("crate::generated::rules::epoch_ms_to_ts(u.{field})?"),
        "to_string" => format!("u.{field}.to_string()"),
        other => panic!("Unknown rule \"{other}\" for field \"{field}\""),
    }
}

/// Call from a business crate's build.rs:
///
/// ```rust
/// // user-sync/build.rs
/// fn main() {
///     sync_engine::codegen::generate("schema.toml");
/// }
/// ```
pub fn generate(schema_path: impl AsRef<Path>) {
    let path = schema_path.as_ref();
    println!("cargo:rerun-if-changed={}", path.display());

    let raw = fs::read_to_string(path).unwrap_or_else(|_| panic!("Cannot read {}", path.display()));
    let schema: Schema =
        toml::from_str(&raw).unwrap_or_else(|e| panic!("Cannot parse {}: {e}", path.display()));

    let out = PathBuf::from(std::env::var("OUT_DIR").unwrap());

    // records.rs — no chrono header; the include! site provides it
    let mut records = String::from("// @generated\n\n");
    let mut envelopes = String::from("// @generated\n\n");
    let mut upserts = String::from("// @generated\n\n");
    let mut transforms = String::from("// @generated\n\n");

    for (name, def) in &schema.record {
        records.push_str(&gen_struct(name, def));
        records.push('\n');
        if let Some(ref fh) = def.fetcher {
            envelopes.push_str(&gen_envelope(name, fh));
            envelopes.push('\n');
        }
        if let Some(ref sh) = def.sink {
            upserts.push_str(&gen_upsert(name, sh, &def.fields));
            upserts.push('\n');
        }
    }
    for mapping in schema.mapping.values() {
        transforms.push_str(&gen_transform(mapping));
        transforms.push('\n');
    }

    fs::write(out.join("records.rs"), &records).unwrap();
    fs::write(out.join("envelopes.rs"), &envelopes).unwrap();
    fs::write(out.join("upserts.rs"), &upserts).unwrap();
    fs::write(out.join("transforms.rs"), &transforms).unwrap();
}
