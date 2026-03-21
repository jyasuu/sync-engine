// sync-engine/src/codegen.rs
//
// Codegen library. Downstream build.rs scripts call:
//
//   fn main() {
//       sync_engine::codegen::generate("schema.toml");
//       sync_engine::codegen::generate_config_doc("config.toml", "schema.toml", "CONFIG.md");
//   }
//
// Changes vs original:
//   - gen_upsert now emits BOTH Upsertable (pool) AND UpsertableInTx (transaction)
//     so the hand-written upsert_in_tx blocks in business crates are no longer needed.

use serde::Deserialize;
use std::collections::HashMap;
use std::fmt::Write as FmtWrite;
use std::fs;
use std::path::{Path, PathBuf};

// ── Schema types ──────────────────────────────────────────────────────────

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
    #[serde(default)]
    pub envelope_meta: Vec<EnvelopeMetaField>,
}

#[derive(Debug, Deserialize)]
pub struct EnvelopeMetaField {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: String,
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
    /// The target (DbModel) field name — always snake_case.
    pub field: String,
    /// The source (ApiRecord) field name. Defaults to `field` if omitted,
    /// which works when both sides have the same name. Set explicitly when
    /// the API uses camelCase and the DB uses snake_case.
    pub source: Option<String>,
    pub rule: String,
}

// ── Generators ────────────────────────────────────────────────────────────

fn gen_struct(name: &str, def: &RecordDef) -> String {
    let mut out = String::new();
    if def.serde_rename.is_some() {
        // Fields will be camelCase Rust identifiers — suppress the lint.
        writeln!(out, "#[allow(non_snake_case)]").unwrap();
    }
    // API records (with fetcher) need Deserialize.
    // DB records (with sink) need both Deserialize and Serialize so they can
    // be published to a RabbitMQ queue in the producer/consumer pattern.
    let derives = if def.sink.is_some() {
        "#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]"
    } else {
        "#[derive(Debug, Clone, serde::Deserialize)]"
    };
    writeln!(out, "{derives}").unwrap();
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
    writeln!(out, "pub struct {record_name}Response {{").unwrap();
    for meta in &hint.envelope_meta {
        writeln!(out, "    pub {}: {},", meta.name, meta.ty).unwrap();
    }
    writeln!(out, "    pub {field}: Vec<{record_name}>,").unwrap();
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

struct UpsertSql {
    sql: String,
    field_names: Vec<String>,
}

fn build_upsert_sql(hint: &SinkHint, fields: &[FieldDef]) -> UpsertSql {
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

    UpsertSql {
        sql: format!("INSERT INTO {table} ({all_cols}) VALUES ({all_vals}) {upsert_clause}"),
        field_names: fields.iter().map(|f| f.name.clone()).collect(),
    }
}

fn gen_bind_chain(field_names: &[String]) -> String {
    field_names
        .iter()
        .map(|n| format!("            .bind(&self.{n})"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Emits both `Upsertable` (pool) and `UpsertableInTx` (transaction) impls.
fn gen_upsert(record_name: &str, hint: &SinkHint, fields: &[FieldDef]) -> String {
    let UpsertSql { sql, field_names } = build_upsert_sql(hint, fields);
    let binds = gen_bind_chain(&field_names);
    let mut out = String::new();

    // Pool-based impl — used by PostgresWriter
    writeln!(out, "#[async_trait::async_trait]").unwrap();
    writeln!(out, "impl sync_engine::Upsertable for {record_name} {{").unwrap();
    writeln!(
        out,
        "    async fn upsert(&self, pool: &sqlx::PgPool) -> anyhow::Result<()> {{"
    )
    .unwrap();
    writeln!(out, "        sqlx::query(\"{sql}\")").unwrap();
    writeln!(out, "{binds}").unwrap();
    writeln!(
        out,
        "            .execute(pool).await.map(|_| ()).map_err(Into::into)"
    )
    .unwrap();
    writeln!(out, "    }}").unwrap();
    writeln!(out, "}}").unwrap();
    out.push('\n');

    // Transaction-based impl — used by TxWriter (new)
    writeln!(out, "#[async_trait::async_trait]").unwrap();
    writeln!(out, "impl sync_engine::UpsertableInTx for {record_name} {{").unwrap();
    writeln!(
        out,
        "    async fn upsert_in_tx(&self, tx: &mut sqlx::Transaction<'_, sqlx::Postgres>) -> anyhow::Result<()> {{"
    )
    .unwrap();
    writeln!(out, "        sqlx::query(\"{sql}\")").unwrap();
    writeln!(out, "{binds}").unwrap();
    writeln!(
        out,
        "            .execute(&mut **tx).await.map(|_| ()).map_err(Into::into)"
    )
    .unwrap();
    writeln!(out, "    }}").unwrap();
    writeln!(out, "}}").unwrap();
    out
}

fn gen_transform(mapping: &MappingDef) -> String {
    let mut out = String::new();
    let (name, from, to) = (&mapping.name, &mapping.from, &mapping.to);
    writeln!(out, "#[derive(Default)]").unwrap();
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
        // source is the ApiRecord field name (may be camelCase);
        // field is the DbModel field name (always snake_case).
        let src = rule.source.as_deref().unwrap_or(&rule.field);
        let expr = gen_rule_expr(&rule.field, src, &rule.rule);
        writeln!(out, "            {}: {expr},", rule.field).unwrap();
    }
    writeln!(out, "        }})").unwrap();
    writeln!(out, "    }}").unwrap();
    writeln!(out, "}}").unwrap();
    out
}

fn gen_rule_expr(field: &str, src: &str, rule: &str) -> String {
    match rule {
        "copy"              => format!("u.{src}"),
        "null_to_empty"     => format!("u.{src}.unwrap_or_default()"),
        "bool_to_yn"        => format!("if u.{src} {{ \"Y\".to_owned() }} else {{ \"N\".to_owned() }}"),
        "option_bool_to_yn" => format!("u.{src}.map(|v| if v {{ \"Y\".to_owned() }} else {{ \"N\".to_owned() }}).unwrap_or_default()"),
        "epoch_ms_to_ts"    => format!("crate::generated::rules::epoch_ms_to_ts(u.{src})?"),
        "to_string"         => format!("u.{src}.to_string()"),
        other => panic!("Unknown rule \"{other}\" for field \"{field}\""),
    }
}

// ── Public entry point ────────────────────────────────────────────────────

pub fn generate(schema_path: impl AsRef<Path>) {
    let path = schema_path.as_ref();
    println!("cargo:rerun-if-changed={}", path.display());

    let raw = fs::read_to_string(path).unwrap_or_else(|_| panic!("Cannot read {}", path.display()));
    let schema: Schema =
        toml::from_str(&raw).unwrap_or_else(|e| panic!("Cannot parse {}: {e}", path.display()));

    let out = PathBuf::from(std::env::var("OUT_DIR").unwrap());

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

// ── Pipeline skeleton generator ───────────────────────────────────────────

/// Generates a ready-to-use `pipeline.toml` skeleton with the correct
/// generated type names pre-filled. Called from build.rs alongside generate().
///
/// The skeleton uses case 1 (tx per window) as the default, with the other
/// three patterns commented out inline.
///
/// ```rust
/// // user-sync/build.rs
/// fn main() {
///     sync_engine::codegen::generate("schema.toml");
///     sync_engine::codegen::generate_pipeline_skeleton("schema.toml", "pipeline.toml");
///     sync_engine::codegen::generate_config_doc("config.toml", "schema.toml", "CONFIG.md");
/// }
/// ```
pub fn generate_pipeline_skeleton(schema_path: impl AsRef<Path>, out_path: impl AsRef<Path>) {
    let schema_path = schema_path.as_ref();
    let out_path = out_path.as_ref();

    println!("cargo:rerun-if-changed={}", schema_path.display());

    // Skip if pipeline.toml already exists — never overwrite user edits.
    if out_path.exists() {
        return;
    }

    let raw = fs::read_to_string(schema_path)
        .unwrap_or_else(|_| panic!("Cannot read {}", schema_path.display()));
    let schema: Schema = toml::from_str(&raw)
        .unwrap_or_else(|e| panic!("Cannot parse {}: {e}", schema_path.display()));

    // Find the envelope, transform, and model type names from schema
    let mut envelope_name = String::from("ApiResponse");
    let mut api_record = String::from("ApiRecord");
    let mut db_record = String::from("DbRecord");
    let mut transform_name = String::from("RecordTransform");
    let mut db_table = String::from("records");

    for (name, def) in &schema.record {
        if def.fetcher.is_some() {
            api_record = name.clone();
            envelope_name = format!("{name}Response");
        }
        if def.sink.is_some() {
            db_record = name.clone();
            if let Some(ref sh) = def.sink {
                db_table = sh.table.clone();
            }
        }
    }
    for mapping in schema.mapping.values() {
        transform_name = mapping.name.clone();
    }

    let mut doc = String::new();
    doc.push_str(&format!(
        "# pipeline.toml — generated skeleton for {db_table}\n\
         # Generated by build.rs from schema.toml. Edit freely.\n\
         # Type names: envelope={envelope_name}, transform={transform_name}, model={db_record}\n\
         #\n\
         # main.rs registration:\n\
         #   registry.register_envelope::<{envelope_name}>(\"{envelope_name}\");\n\
         #   registry.register_transform::<{api_record}, {db_record}, {transform_name}>(\"{transform_name}\");\n\
         #   registry.register_model::<{db_record}>(\"{db_record}\");\n\n"
    ));

    doc.push_str("[job]\nname = \"");
    doc.push_str(&db_table.replace('_', "-"));
    doc.push_str("\"\n\n[job.scheduler]\ncron = { env = \"SCHEDULER__CRON\", default = \"0 */30 * * * *\" }\n\n");

    doc.push_str(
        "# ── Resources ─────────────────────────────────────────────────────────────\n\n",
    );
    doc.push_str("[resources.pg]\ntype            = \"postgres\"\nurl             = { env = \"SINK__DATABASE_URL\" }\nmax_connections = 5\n\n");
    doc.push_str("[resources.auth]\ntype          = \"oauth2\"\ntoken_url     = { env = \"AUTH__TOKEN_URL\" }\nclient_id     = { env = \"AUTH__CLIENT_ID\" }\nclient_secret = { env = \"AUTH__CLIENT_SECRET\" }\n\n");
    doc.push_str("[resources.http]\ntype           = \"http_client\"\ntimeout_secs   = 620\nkeepalive_secs = 30\n\n");
    doc.push_str("[resources.svc]\ntype       = \"http_service\"\nhttp       = \"http\"\nauth       = \"auth\"\nendpoint   = { env = \"SOURCE__ENDPOINT\" }\nrealm_type = { env = \"SOURCE__REALM_TYPE\", default = \"\" }\n\n");

    doc.push_str("# ── Slots ─────────────────────────────────────────────────────────────────\n");
    doc.push_str(
        "# scope = \"window\" (case 1/3/4) or \"job\" (case 2: accumulate across windows)\n\n",
    );
    doc.push_str(&format!(
        "[slots.api_rows]\ntype  = \"{api_record}\"\nscope = \"window\"\n\n"
    ));
    doc.push_str(&format!(
        "[slots.db_rows]\ntype  = \"{db_record}\"\nscope = \"window\"\n\n"
    ));

    doc.push_str(
        "# ── Pre-job ───────────────────────────────────────────────────────────────\n\n",
    );
    doc.push_str("[pre_job]\ninit_resources = true\n\n");
    doc.push_str("# Case 3/4 — uncomment to spawn async consumer:\n");
    doc.push_str(&format!("# [[pre_job.steps]]\n# type        = \"spawn_consumer\"\n# queue       = \"db_rows\"\n# model       = \"{db_record}\"\n# commit_mode = \"per_batch\"\n\n"));

    doc.push_str(
        "# ── Main job ──────────────────────────────────────────────────────────────\n\n",
    );
    doc.push_str("[main_job.iterator]\ntype           = \"date_window\"\nstart_interval = { env = \"SOURCE__START_INTERVAL\",    default = \"30\" }\nend_interval   = { env = \"SOURCE__END_INTERVAL\",      default = \"0\" }\ninterval_limit = { env = \"SOURCE__INTERVAL_LIMIT\",    default = \"7\" }\nsleep_secs     = { env = \"SOURCE__WINDOW_SLEEP_SECS\", default = \"60\" }\n\n");
    doc.push_str("[main_job.retry]\nmax_attempts = 5\nbackoff_secs = 2\n\n");

    doc.push_str(&format!(
        "[[main_job.retry_steps]]\ntype     = \"fetch\"\nenvelope = \"{envelope_name}\"\nwrites   = \"api_rows\"\nappend   = false\n\n\
         [[main_job.retry_steps]]\ntype      = \"transform\"\ntransform = \"{transform_name}\"\nreads     = \"api_rows\"\nwrites    = \"db_rows\"\nappend    = false\n\n\
         # Case 1 (active): tx per window\n\
         [[main_job.retry_steps]]\ntype  = \"tx_upsert\"\nmodel = \"{db_record}\"\nreads = \"db_rows\"\n\n\
         # Case 3/4: replace tx_upsert with send_to_queue:\n\
         # [[main_job.retry_steps]]\n# type  = \"send_to_queue\"\n# model = \"{db_record}\"\n# reads = \"db_rows\"\n# queue = \"db_rows\"\n\n"
    ));

    doc.push_str("[[main_job.post_window_steps]]\ntype = \"sleep\"\nsecs = { env = \"SOURCE__WINDOW_SLEEP_SECS\", default = \"60\" }\n\n");
    doc.push_str(&format!(
        "# Case 2: bulk tx after all windows — uncomment:\n\
         # [[main_job.post_loop_steps]]\n# type  = \"tx_upsert\"\n# model = \"{db_record}\"\n# reads = \"db_rows\"\n\n"
    ));

    doc.push_str(
        "# ── Post-job ──────────────────────────────────────────────────────────────\n\n",
    );
    doc.push_str("[[post_job.steps]]\ntype = \"log_summary\"\n\n");
    doc.push_str("[[post_job.steps]]\ntype          = \"raw_sql\"\nsql           = { env = \"SINK__SYNC_SQL\", default = \"\" }\nskip_if_empty = true\n\n");
    doc.push_str("# Case 3/4: drain queue before summary:\n# [[post_job.steps]]\n# type  = \"drain_queue\"\n# queue = \"db_rows\"\n\n");
    doc.push_str("# Custom hook (implement PostHook in main.rs):\n# [[post_job.steps]]\n# type = \"custom\"\n# hook = \"MyHook\"\n");

    fs::write(out_path, &doc)
        .unwrap_or_else(|e| panic!("Cannot write {}: {e}", out_path.display()));
    println!("cargo:warning=Generated {}", out_path.display());
}

pub fn generate_config_doc(
    config_path: impl AsRef<Path>,
    schema_path: impl AsRef<Path>,
    out_path: impl AsRef<Path>,
) {
    let config_path = config_path.as_ref();
    let schema_path = schema_path.as_ref();
    let out_path = out_path.as_ref();

    println!("cargo:rerun-if-changed={}", config_path.display());
    println!("cargo:rerun-if-changed={}", schema_path.display());

    let config_raw = fs::read_to_string(config_path)
        .unwrap_or_else(|_| panic!("Cannot read {}", config_path.display()));
    let schema_raw = fs::read_to_string(schema_path)
        .unwrap_or_else(|_| panic!("Cannot read {}", schema_path.display()));

    let config: toml::Value = config_raw
        .parse()
        .unwrap_or_else(|e| panic!("Cannot parse {}: {e}", config_path.display()));
    let schema: Schema = toml::from_str(&schema_raw)
        .unwrap_or_else(|e| panic!("Cannot parse {}: {e}", schema_path.display()));

    let mut doc = String::new();
    doc.push_str("# Configuration reference\n\n");
    doc.push_str(
        "> Generated from `config.toml` and `schema.toml` — do not edit this file directly.\n",
    );
    doc.push_str(">\n");
    doc.push_str("> **Override any value** with an environment variable using `__` as the section separator:  \n");
    doc.push_str("> `AUTH__CLIENT_SECRET=prod-secret` overrides `[auth] client_secret`\n\n");
    doc.push_str("---\n\n");
    doc.push_str("## Runtime config (`config.toml`)\n\n");

    if let toml::Value::Table(root) = &config {
        for (section, value) in root {
            if let toml::Value::Table(fields) = value {
                let env_prefix = section.to_uppercase();
                doc.push_str(&format!("### `[{section}]`\n\n"));
                doc.push_str("| Key | Default | Description |\n");
                doc.push_str("|-----|---------|-------------|\n");

                let keys: Vec<&String> = fields
                    .keys()
                    .filter(|k| !k.ends_with("__desc") && !k.ends_with("__example"))
                    .collect();

                for key in keys {
                    let val = &fields[key];
                    let default = toml_value_display(val);
                    let desc = fields
                        .get(&format!("{key}__desc"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    let example = fields
                        .get(&format!("{key}__example"))
                        .and_then(|v| v.as_str());
                    let mut cell = desc.to_string();
                    if let Some(ex) = example {
                        cell.push_str(&format!("<br>*e.g. `{ex}`*"));
                    }
                    let env_key = format!("`{env_prefix}__{}`", key.to_uppercase());
                    doc.push_str(&format!("| `{key}` / {env_key} | `{default}` | {cell} |\n"));
                }
                doc.push('\n');
            }
        }
    }

    doc.push_str("---\n\n");
    doc.push_str("## Schema (`schema.toml`)\n\n");
    doc.push_str("Defines the API record shape, DB target shape, and field mapping rules.  \n");
    doc.push_str(
        "`build.rs` generates all Rust structs and trait implementations from this file.\n\n",
    );

    for (record_name, def) in &schema.record {
        let kind = if def.fetcher.is_some() {
            "API source record"
        } else {
            "DB target record"
        };
        doc.push_str(&format!("### `[record.{record_name}]`  _{kind}_\n\n"));
        if let Some(ref fh) = def.fetcher {
            doc.push_str(&format!(
                "Response envelope field: `{}`  \n",
                fh.envelope_field
            ));
            if !fh.envelope_meta.is_empty() {
                let names: Vec<&str> = fh.envelope_meta.iter().map(|m| m.name.as_str()).collect();
                doc.push_str(&format!(
                    "Envelope metadata fields: {}  \n",
                    names.join(", ")
                ));
            }
            doc.push('\n');
        }
        if let Some(ref sh) = def.sink {
            doc.push_str(&format!(
                "Table: `{}` · Primary key: `{}` · Upsert: `{}`  \n\n",
                sh.table, sh.primary_key, sh.upsert
            ));
        }
        doc.push_str("| Field | Type |\n");
        doc.push_str("|-------|------|\n");
        for f in &def.fields {
            doc.push_str(&format!("| `{}` | `{}` |\n", f.name, f.ty));
        }
        doc.push('\n');
    }

    doc.push_str("### Mapping rules\n\n");
    doc.push_str("| Rule | Input | Output | Effect |\n");
    doc.push_str("|------|-------|--------|--------|\n");
    doc.push_str("| `copy` | any | same type | Direct field copy |\n");
    doc.push_str("| `null_to_empty` | `Option<String>` | `String` | `None` → `\"\"` |\n");
    doc.push_str("| `bool_to_yn` | `bool` | `String` | `true` → `\"Y\"`, `false` → `\"N\"` |\n");
    doc.push_str(
        "| `epoch_ms_to_ts` | `i64` / `Option<i64>` | `DateTime<Utc>` | Epoch ms → timestamp |\n",
    );
    doc.push_str("| `to_string` | any `Display` | `String` | `.to_string()` |\n\n");

    for (mapping_name, mapping) in &schema.mapping {
        doc.push_str(&format!(
            "#### `[mapping.{mapping_name}]` — `{}` → `{}`\n\n",
            mapping.from, mapping.to
        ));
        doc.push_str("| Field | Rule |\n");
        doc.push_str("|-------|------|\n");
        for rule in &mapping.rules {
            doc.push_str(&format!("| `{}` | `{}` |\n", rule.field, rule.rule));
        }
        doc.push('\n');
    }

    fs::write(out_path, &doc)
        .unwrap_or_else(|e| panic!("Cannot write {}: {e}", out_path.display()));
    println!("cargo:warning=Generated {}", out_path.display());
}

fn toml_value_display(v: &toml::Value) -> String {
    match v {
        toml::Value::String(s) => {
            if s.is_empty() {
                "\"\"".into()
            } else {
                s.clone()
            }
        }
        toml::Value::Integer(n) => n.to_string(),
        toml::Value::Float(f) => f.to_string(),
        toml::Value::Boolean(b) => b.to_string(),
        _ => "—".into(),
    }
}
