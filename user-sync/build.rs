// user-sync/build.rs
//
// Same codegen as before.
// The only difference: generated code refers to sync_engine:: instead of crate::
// for trait paths, since the engine lives in a separate crate.

use serde::Deserialize;
use std::collections::HashMap;
use std::fmt::Write as FmtWrite;
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Deserialize)]
struct Schema {
    record: HashMap<String, RecordDef>,
    mapping: HashMap<String, MappingDef>,
}

#[derive(Debug, Deserialize)]
struct RecordDef {
    #[serde(default)]
    serde_rename: Option<String>,
    #[serde(default)]
    fetcher: Option<FetcherHint>,
    #[serde(default)]
    sink: Option<SinkHint>,
    fields: Vec<FieldDef>,
}

#[derive(Debug, Deserialize)]
struct FetcherHint {
    envelope_field: String,
}

#[derive(Debug, Deserialize)]
struct SinkHint {
    table: String,
    primary_key: String,
    #[serde(default)]
    extra_copy: Vec<String>,
    #[serde(default)]
    upsert: bool,
}

#[derive(Debug, Deserialize)]
struct FieldDef {
    name: String,
    #[serde(rename = "type")]
    ty: String,
}

#[derive(Debug, Deserialize)]
struct MappingDef {
    name: String,
    from: String,
    to: String,
    rules: Vec<RuleDef>,
}

#[derive(Debug, Deserialize)]
struct RuleDef {
    field: String,
    rule: String,
}

fn gen_struct(name: &str, def: &RecordDef) -> String {
    let mut out = String::new();
    writeln!(out, "#[derive(Debug, Clone, serde::Deserialize)]").unwrap();
    if let Some(ref s) = def.serde_rename {
        writeln!(out, "#[serde(rename_all = \"{s}\")]").unwrap();
    }
    writeln!(out, "pub struct {name} {{").unwrap();
    for f in &def.fields {
        let needs_with = f.ty.contains("DateTime");
        if needs_with && f.ty.starts_with("Option") {
            writeln!(
                out,
                "    #[serde(default, with = \"chrono::serde::ts_milliseconds_option\")]"
            )
            .unwrap();
        } else if needs_with {
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
    writeln!(out, "#[derive(Debug, serde::Deserialize)]").unwrap();
    writeln!(out, "pub struct {record_name}Response {{").unwrap();
    writeln!(out, "    pub timestamp:   i64,").unwrap();
    writeln!(out, "    pub status_code: i64,").unwrap();
    writeln!(out, "    pub {field}: Vec<{record_name}>,").unwrap();
    writeln!(out, "}}").unwrap();
    // impl HasEnvelope — now references sync_engine::
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
    let extra_val_refs: Vec<String> = hint
        .extra_copy
        .iter()
        .map(|s| {
            let src = s.split('=').nth(1).unwrap().trim();
            let idx = col_list
                .iter()
                .position(|c| *c == src)
                .unwrap_or_else(|| panic!("extra_copy ref '{src}' not found in fields"));
            format!("${}", idx + 1)
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

    let all_vals = if extra_val_refs.is_empty() {
        placeholders.join(", ")
    } else {
        format!("{}, {}", placeholders.join(", "), extra_val_refs.join(", "))
    };

    let conflict_clause = if hint.upsert {
        format!("ON CONFLICT ({pk}) DO UPDATE SET {}", update_set.join(", "))
    } else {
        String::new()
    };

    // SQL is fully assembled at codegen time and embedded as a raw string literal
    // sqlx::query (no !) is a plain runtime function — no DATABASE_URL needed at compile time
    let sql = format!("INSERT INTO {table} ({all_cols}) VALUES ({all_vals}) {conflict_clause}");

    let mut out = String::new();
    writeln!(out, "#[async_trait::async_trait]").unwrap();
    writeln!(out, "impl sync_engine::Upsertable for {record_name} {{").unwrap();
    writeln!(
        out,
        "    async fn upsert(&self, pool: &sqlx::PgPool) -> anyhow::Result<()> {{"
    )
    .unwrap();
    writeln!(out, "        sqlx::query(r#\"{sql}\"#)").unwrap();
    for f in fields {
        writeln!(out, "            .bind(&self.{})", f.name).unwrap();
    }
    writeln!(out, "            .execute(pool).await").unwrap();
    writeln!(out, "            .map(|_| ()).map_err(anyhow::Error::from)").unwrap();
    writeln!(out, "    }}").unwrap();
    writeln!(out, "}}").unwrap();
    out
}

fn gen_transform(mapping: &MappingDef) -> String {
    let mut out = String::new();
    let name = &mapping.name;
    let from = &mapping.from;
    let to = &mapping.to;
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

fn main() {
    println!("cargo:rerun-if-changed=schema.toml");

    let src: Schema =
        toml::from_str(&fs::read_to_string("schema.toml").expect("Cannot read schema.toml"))
            .expect("Cannot parse schema.toml");

    let out = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    let hdr = "// @generated — do not edit. Source: schema.toml\n";

    let mut records = format!("{hdr}use chrono::{{DateTime, Utc}};\n\n");
    let mut envelopes = format!("{hdr}\n");
    let mut upserts = format!("{hdr}\n");
    let mut transforms = format!("{hdr}\n");

    for (name, def) in &src.record {
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
    for mapping in src.mapping.values() {
        transforms.push_str(&gen_transform(mapping));
        transforms.push('\n');
    }

    fs::write(out.join("records.rs"), &records).unwrap();
    fs::write(out.join("envelopes.rs"), &envelopes).unwrap();
    fs::write(out.join("upserts.rs"), &upserts).unwrap();
    fs::write(out.join("transforms.rs"), &transforms).unwrap();
}
