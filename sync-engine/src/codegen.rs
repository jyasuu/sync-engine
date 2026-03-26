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
    // All records derive both Deserialize and Serialize.
    // - API records (fetcher): Serialize enables slot_to_json passthrough to ES/Kafka.
    // - DB records (sink): Serialize is required for RabbitMQ queue and slot_to_json.
    let derives = "#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]";
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
            let source_field = parts.get(1).unwrap_or_else(|| {
                panic!("schema.toml: extra_copy entry \"{s}\" must be \"dest_col = source_col\"")
            });
            let idx = col_list.iter().position(|c| *c == *source_field)
                .unwrap_or_else(|| panic!(
                    "schema.toml: extra_copy source field \"{source_field}\" not found in record fields. \
                     Available fields: [{}]",
                    col_list.join(", ")
                ));
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
    writeln!(
        out,
        "impl sync_engine::UpsertableInTx for {record_name} {{"
    )
    .unwrap();
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

/// Public wrapper for tests — exercises the rule expression generator.
#[cfg(test)]
pub fn gen_rule_expr_pub(field: &str, src: &str, rule: &str) -> String {
    gen_rule_expr(field, src, rule)
}

// ── Public entry point ────────────────────────────────────────────────────

pub fn generate(schema_path: impl AsRef<Path>) {
    let path = schema_path.as_ref();
    println!("cargo:rerun-if-changed={}", path.display());

    let raw = fs::read_to_string(path)
        .unwrap_or_else(|_| panic!("Cannot read {}", path.display()));
    let schema: Schema =
        toml::from_str(&raw).unwrap_or_else(|e| panic!("Cannot parse {}: {e}", path.display()));

    let out = PathBuf::from(std::env::var("OUT_DIR").unwrap());

    let mut records   = String::from("// @generated\n\n");
    let mut envelopes = String::from("// @generated\n\n");
    let mut upserts   = String::from("// @generated\n\n");
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

    fs::write(out.join("records.rs"),   &records).unwrap();
    fs::write(out.join("envelopes.rs"), &envelopes).unwrap();
    fs::write(out.join("upserts.rs"),   &upserts).unwrap();
    fs::write(out.join("transforms.rs"), &transforms).unwrap();
}

// ── Pipeline skeleton generator ───────────────────────────────────────────

/// Generates a ready-to-use `pipeline.toml` skeleton with the correct
/// generated type names pre-filled. Called from build.rs alongside generate().
///
/// The skeleton uses case 1 (tx per window) as the default, with the other
/// three patterns commented out inline.
///
/// ```rust,no_run
/// // user-sync/build.rs
/// fn main() {
///     sync_engine::codegen::generate("schema.toml");
///     sync_engine::codegen::generate_pipeline_skeleton("schema.toml", "pipeline.toml");
///     sync_engine::codegen::generate_config_doc("config.toml", "schema.toml", "CONFIG.md");
/// }
/// ```
pub fn generate_pipeline_skeleton(
    schema_path: impl AsRef<Path>,
    out_path: impl AsRef<Path>,
) {
    let schema_path = schema_path.as_ref();
    let out_path    = out_path.as_ref();

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
    let mut envelope_name  = String::from("ApiResponse");
    let mut api_record     = String::from("ApiRecord");
    let mut db_record      = String::from("DbRecord");
    let mut transform_name = String::from("RecordTransform");
    let mut db_table       = String::from("records");

    for (name, def) in &schema.record {
        if def.fetcher.is_some() {
            api_record    = name.clone();
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

    doc.push_str("# ── Resources ─────────────────────────────────────────────────────────────\n\n");
    doc.push_str("[resources.pg]\ntype            = \"postgres\"\nurl             = { env = \"SINK__DATABASE_URL\" }\nmax_connections = 5\n\n");
    doc.push_str("[resources.auth]\ntype          = \"oauth2\"\ntoken_url     = { env = \"AUTH__TOKEN_URL\" }\nclient_id     = { env = \"AUTH__CLIENT_ID\" }\nclient_secret = { env = \"AUTH__CLIENT_SECRET\" }\n\n");
    doc.push_str("[resources.http]\ntype           = \"http_client\"\ntimeout_secs   = 620\nkeepalive_secs = 30\n\n");
    doc.push_str("[resources.svc]\ntype         = \"http_service\"\nhttp         = \"http\"\nauth         = \"auth\"\nendpoint     = { env = \"SOURCE__ENDPOINT\" }\nrealm_type   = { env = \"SOURCE__REALM_TYPE\", default = \"\" }\n# start_param = \"start_time\"   # default — override for APIs using different param names\n# end_param   = \"end_time\"     # default\n# date_format = \"%Y%m%d\"       # default — e.g. \"%Y-%m-%d\" or \"%Y-%m-%dT00:00:00Z\"\n# [[resources.svc.extra_params]]\n# key   = \"api_version\"\n# value = { env = \"SOURCE__API_VERSION\", default = \"v2\" }\n\n");

    doc.push_str("# ── Slots ─────────────────────────────────────────────────────────────────\n");
    doc.push_str("# scope = \"window\" (case 1/3/4) or \"job\" (case 2: accumulate across windows)\n\n");
    doc.push_str(&format!("[slots.api_rows]\ntype  = \"{api_record}\"\nscope = \"window\"\n\n"));
    doc.push_str(&format!("[slots.db_rows]\ntype  = \"{db_record}\"\nscope = \"window\"\n\n"));

    doc.push_str("# ── Pre-job ───────────────────────────────────────────────────────────────\n\n");
    doc.push_str("[pre_job]\ninit_resources = true\n\n");
    doc.push_str("# Case 3/4 — uncomment to spawn async consumer:\n");
    doc.push_str(&format!("# [[pre_job.steps]]\n# type        = \"spawn_consumer\"\n# queue       = \"db_rows\"\n# model       = \"{db_record}\"\n# commit_mode = \"per_batch\"\n\n"));

    doc.push_str("# ── Main job ──────────────────────────────────────────────────────────────\n\n");
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

    doc.push_str("# ── Post-job ──────────────────────────────────────────────────────────────\n\n");
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
    let out_path    = out_path.as_ref();

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
    doc.push_str("> Generated from `config.toml` and `schema.toml` — do not edit this file directly.\n");
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
                    let val     = &fields[key];
                    let default = toml_value_display(val);
                    let desc    = fields
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
    doc.push_str("`build.rs` generates all Rust structs and trait implementations from this file.\n\n");

    for (record_name, def) in &schema.record {
        let kind = if def.fetcher.is_some() { "API source record" } else { "DB target record" };
        doc.push_str(&format!("### `[record.{record_name}]`  _{kind}_\n\n"));
        if let Some(ref fh) = def.fetcher {
            doc.push_str(&format!("Response envelope field: `{}`  \n", fh.envelope_field));
            if !fh.envelope_meta.is_empty() {
                let names: Vec<&str> = fh.envelope_meta.iter().map(|m| m.name.as_str()).collect();
                doc.push_str(&format!("Envelope metadata fields: {}  \n", names.join(", ")));
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
    doc.push_str("| `epoch_ms_to_ts` | `i64` / `Option<i64>` | `DateTime<Utc>` | Epoch ms → timestamp |\n");
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
        toml::Value::String(s)  => if s.is_empty() { "\"\"".into() } else { s.clone() },
        toml::Value::Integer(n) => n.to_string(),
        toml::Value::Float(f)   => f.to_string(),
        toml::Value::Boolean(b) => b.to_string(),
        _                       => "—".into(),
    }
}

// ── Architecture SVG generator ────────────────────────────────────────────

/// Generates `ARCHITECTURE.md` — a Markdown file containing a self-contained
/// SVG diagram of the full pipeline architecture for this job. Called from
/// `build.rs` after `generate()`.
///
/// The SVG reflects the actual schema (record names, table, fields) and
/// pipeline config (job name, pattern, scheduler cron) so it stays in sync
/// with the code automatically.
///
/// ```rust,no_run
/// // user-sync/build.rs
/// fn main() {
///     sync_engine::codegen::generate("schema.toml");
///     // Write standalone .svg (viewable in browser / git diff)
///     sync_engine::codegen::generate_architecture_svg_file(
///         "schema.toml", "pipeline.toml", "ARCHITECTURE.svg"
///     );
///     // Write .md that embeds the .svg via a relative img tag
///     sync_engine::codegen::generate_architecture_svg(
///         "schema.toml", "pipeline.toml", "ARCHITECTURE.md", "ARCHITECTURE.svg"
///     );
/// }
/// ```
///
/// Legacy two-argument form (inline SVG blob in the `.md`) is no longer
/// the recommended path, but the four-argument form above is the new default.
///
/// If you only want the `.svg` file and no markdown wrapper, call
/// [`generate_architecture_svg_file`] directly.

/// Write raw SVG to `svg_out_path`.
/// Usable standalone — open in any browser or SVG viewer.
pub fn generate_architecture_svg_file(
    schema_path:   impl AsRef<Path>,
    pipeline_path: impl AsRef<Path>,
    svg_out_path:  impl AsRef<Path>,
) {
    let schema_path   = schema_path.as_ref();
    let pipeline_path = pipeline_path.as_ref();
    let svg_out_path  = svg_out_path.as_ref();

    println!("cargo:rerun-if-changed={}", schema_path.display());
    println!("cargo:rerun-if-changed={}", pipeline_path.display());

    let svg = build_svg(schema_path, pipeline_path);
    fs::write(svg_out_path, &svg)
        .unwrap_or_else(|e| panic!("Cannot write {}: {e}", svg_out_path.display()));
    println!("cargo:warning=Generated {}", svg_out_path.display());
}

/// Write a Markdown file that references `svg_ref_path` via a relative
/// `![architecture](…)` image tag, plus the pattern/field tables.
///
/// Call [`generate_architecture_svg_file`] first to emit the actual `.svg`.
pub fn generate_architecture_svg(
    schema_path:   impl AsRef<Path>,
    pipeline_path: impl AsRef<Path>,
    md_out_path:   impl AsRef<Path>,
    svg_ref_path:  impl AsRef<Path>,
) {
    let schema_path   = schema_path.as_ref();
    let pipeline_path = pipeline_path.as_ref();
    let md_out_path   = md_out_path.as_ref();
    let svg_ref_path  = svg_ref_path.as_ref();

    println!("cargo:rerun-if-changed={}", schema_path.display());
    println!("cargo:rerun-if-changed={}", pipeline_path.display());

    // Re-derive the metadata we need for the Markdown tables.
    let schema_raw = fs::read_to_string(schema_path)
        .unwrap_or_else(|_| panic!("Cannot read {}", schema_path.display()));
    let schema: Schema = toml::from_str(&schema_raw)
        .unwrap_or_else(|e| panic!("Cannot parse {}: {e}", schema_path.display()));

    let mut api_name   = "ApiRecord".to_owned();
    let mut db_name    = "DbRecord".to_owned();
    let mut table_name = "records".to_owned();
    let mut mapping_rules: Vec<(String, String)> = vec![];

    for (name, def) in &schema.record {
        if def.fetcher.is_some() { api_name = name.clone(); }
        if def.sink.is_some() {
            db_name = name.clone();
            if let Some(ref sh) = def.sink { table_name = sh.table.clone(); }
        }
    }
    for mapping in schema.mapping.values() {
        mapping_rules = mapping.rules.iter()
            .map(|r| (r.field.clone(), r.rule.clone()))
            .take(8)
            .collect();
    }

    let pipeline_val = fs::read_to_string(pipeline_path).ok()
        .and_then(|s| toml::from_str::<toml::Value>(&s).ok());

    let job_name = pipeline_val.as_ref()
        .and_then(|v| v.get("job")?.get("name")?.as_str().map(str::to_owned))
        .unwrap_or_else(|| table_name.replace('_', "-"));

    let cron = pipeline_val.as_ref()
        .and_then(|v| {
            let sch = v.get("job")?.get("scheduler")?;
            sch.get("cron")?.as_str()
                .or_else(|| sch.get("cron")?.get("default")?.as_str())
                .map(str::to_owned)
        })
        .unwrap_or_else(|| "0 */30 * * * *".to_owned());

    let has_queue = pipeline_val.as_ref()
        .map(|v| v.get("queues").is_some())
        .unwrap_or(false);

    let pattern_label = if has_queue { "case 3/4 — async queue" } else { "case 1 — tx per window" };

    // svg_ref_path is written as a relative path in the img tag.
    // If the .md and .svg sit in the same directory (the normal case) this
    // is just the filename; otherwise the caller controls the relative path
    // by passing e.g. "docs/ARCHITECTURE.svg".
    let svg_rel = svg_ref_path.file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| svg_ref_path.display().to_string());

    let md = format!(
        "# {job_name} — architecture\n\n\
         > Auto-generated by `build.rs`. Do not edit — regenerated on every build.\n\n\
         ![{job_name} architecture]({svg_rel})\n\n\
         ## Pattern: {pattern_label}\n\n\
         | Phase | Steps |\n\
         |-------|-------|\n\
         | pre\\_job | build resources (postgres pool, oauth2, http client) · declare slots/queues |\n\
         | main\\_job | composable step tree — see ARCHITECTURE.svg for the actual nesting |\n\
         | post\\_job | log\\_summary · raw\\_sql · drain\\_queue · custom hook |\n\
         | scheduler | cron `{cron}` · mutex-skip · pipeline-scope slots survive ticks |\n\
         \n\
         ## Source records\n\n\
         | Source (`{api_name}`) | Transform rule | Sink (`{db_name}`) |\n\
         |---|---|---|\n\
         {rule_rows}\n",
        rule_rows = mapping_rules.iter()
            .map(|(f, r)| format!("| `{f}` | `{r}` | `{f}` |"))
            .collect::<Vec<_>>()
            .join("\n")
    );

    fs::write(md_out_path, &md)
        .unwrap_or_else(|e| panic!("Cannot write {}: {e}", md_out_path.display()));
    println!("cargo:warning=Generated {}", md_out_path.display());
}

// ── Private helper ────────────────────────────────────────────────────────

// Step representation used only for SVG rendering.
#[derive(Clone)]
struct SvgStep {
    label:      String,
    sub:        String,
    is_wrapper: bool,
    children:   Vec<SvgStep>,
}

const LEAF_H:   i32 = 28;
const WRAP_HDR: i32 = 22;
const WRAP_PAD: i32 = 8;
const STEP_GAP: i32 = 4;
const INDENT:   i32 = 16;

fn svg_step_height(s: &SvgStep) -> i32 {
    if !s.is_wrapper || s.children.is_empty() { return LEAF_H; }
    let ch: i32 = s.children.iter().map(svg_step_height).sum::<i32>()
        + STEP_GAP * (s.children.len() as i32 - 1).max(0);
    WRAP_HDR + WRAP_PAD + ch + WRAP_PAD
}

fn svg_steps_total_height(steps: &[SvgStep]) -> i32 {
    if steps.is_empty() { return 0; }
    steps.iter().map(svg_step_height).sum::<i32>()
        + STEP_GAP * (steps.len() as i32 - 1).max(0)
}

fn svg_step_colors(label: &str, is_wrapper: bool) -> (&'static str, &'static str) {
    if label.starts_with("window_loop") { return ("hdr",  "ht");  }
    if label.starts_with("retry")       { return ("hdr",  "ht");  }
    if label.starts_with("tx ")         { return ("hdr3", "ht3"); }
    if label == "tx_upsert"             { return ("hdr2", "ht2"); }
    if label == "transform"             { return ("hdr4", "ht4"); }
    if label == "fetch"                 { return ("hdr",  "ht");  }
    if label == "sleep"                 { return ("hdr5", "ht5"); }
    if is_wrapper                       { return ("hdr",  "ht");  }
    ("hdr5", "ht5")
}

fn svg_step_subtitle(v: &toml::Value) -> String {
    let mut parts: Vec<String> = Vec::new();
    for key in &["envelope","transform","model","group","reads","writes","secs","max_attempts"] {
        if let Some(val) = v.get(key) {
            let s = match val {
                toml::Value::String(s)  => s.clone(),
                toml::Value::Integer(n) => n.to_string(),
                toml::Value::Table(t)   => t.get("default")
                    .and_then(|d| d.as_str()).unwrap_or("…").to_owned(),
                _ => continue,
            };
            parts.push(format!("{key}={s}"));
            if parts.len() >= 2 { break; }
        }
    }
    parts.join("  ")
}

fn resolve_svg_step(
    v:      &toml::Value,
    groups: &HashMap<String, toml::Value>,
    depth:  usize,
) -> SvgStep {
    if depth > 8 {
        return SvgStep { label: "…".into(), sub: "".into(), is_wrapper: false, children: vec![] };
    }
    let type_str   = v.get("type").and_then(|t| t.as_str()).unwrap_or("step");
    let is_wrapper = matches!(type_str, "window_loop" | "retry" | "tx");

    let children = if is_wrapper {
        let inline: Vec<SvgStep> = v.get("steps")
            .and_then(|s| s.as_array())
            .map(|arr| arr.iter().map(|c| resolve_svg_step(c, groups, depth + 1)).collect())
            .unwrap_or_default();
        if !inline.is_empty() {
            inline
        } else if let Some(gname) = v.get("group").and_then(|g| g.as_str()) {
            groups.get(gname)
                .and_then(|grp| grp.get("steps")?.as_array())
                .map(|arr| arr.iter().map(|c| resolve_svg_step(c, groups, depth + 1)).collect())
                .unwrap_or_default()
        } else { vec![] }
    } else { vec![] };

    let label = match type_str {
        "window_loop" => {
            let limit = v.get("interval_limit")
                .and_then(|x| x.as_integer()
                    .or_else(|| x.get("default")?.as_str()?.parse().ok()))
                .unwrap_or(7);
            format!("window_loop  (limit={limit}d)")
        }
        "retry" => {
            let n = v.get("max_attempts").and_then(|x| x.as_integer()).unwrap_or(5);
            format!("retry  (max={n})")
        }
        "tx"    => "tx  (postgres transaction)".into(),
        other   => other.to_owned(),
    };

    let sub = if !is_wrapper {
        v.get("group").and_then(|g| g.as_str())
            .map(|g| format!("group:{g}"))
            .unwrap_or_else(|| svg_step_subtitle(v))
    } else {
        svg_step_subtitle(v)
    };

    SvgStep { label, sub, is_wrapper, children }
}

fn render_steps_svg(steps: &[SvgStep], x: i32, y: i32, avail_w: i32, out: &mut String) {
    let mut cy = y;
    for (i, s) in steps.iter().enumerate() {
        let h = svg_step_height(s);
        let (hdr, tc) = svg_step_colors(&s.label, s.is_wrapper);

        if s.is_wrapper && !s.children.is_empty() {
            out.push_str(&format!(
                "<rect x=\"{x}\" y=\"{cy}\" width=\"{avail_w}\" height=\"{h}\" rx=\"5\" class=\"box\"/>\n\
                 <rect x=\"{x}\" y=\"{cy}\" width=\"{avail_w}\" height=\"{WRAP_HDR}\" rx=\"5\" class=\"{hdr}\"/>\n\
                 <rect x=\"{x}\" y=\"{}\" width=\"{avail_w}\" height=\"6\" class=\"{hdr}\"/>\n\
                 <text x=\"{}\" y=\"{}\" class=\"{tc}\" font-size=\"12\">{}</text>\n",
                cy + WRAP_HDR - 6,
                x + 8, cy + WRAP_HDR - 6,
                s.label
            ));
            if !s.sub.is_empty() {
                out.push_str(&format!(
                    "<text x=\"{}\" y=\"{}\" class=\"dim\" font-size=\"10\" text-anchor=\"end\">{}</text>\n",
                    x + avail_w - 8, cy + WRAP_HDR - 6, s.sub
                ));
            }
            render_steps_svg(&s.children, x + INDENT, cy + WRAP_HDR + WRAP_PAD, avail_w - INDENT * 2, out);
        } else {
            out.push_str(&format!(
                "<rect x=\"{x}\" y=\"{cy}\" width=\"{avail_w}\" height=\"{h}\" rx=\"4\" class=\"{hdr}\"/>\n\
                 <text x=\"{}\" y=\"{}\" class=\"{tc}\" font-size=\"12\">{}</text>\n",
                x + 8, cy + 18, s.label
            ));
            if !s.sub.is_empty() {
                out.push_str(&format!(
                    "<text x=\"{}\" y=\"{}\" class=\"dim\" font-size=\"10\" text-anchor=\"end\">{}</text>\n",
                    x + avail_w - 8, cy + 18, s.sub
                ));
            }
        }
        cy += h;
        if i + 1 < steps.len() { cy += STEP_GAP; }
    }
}

/// Parse schema + pipeline and return the SVG string.
/// All layout and rendering lives here; the public functions just decide
/// where to write the output.
fn build_svg(schema_path: &Path, pipeline_path: &Path) -> String {
    // ── Parse schema ──────────────────────────────────────────────────────
    let schema_raw = fs::read_to_string(schema_path)
        .unwrap_or_else(|_| panic!("Cannot read {}", schema_path.display()));
    let schema: Schema = toml::from_str(&schema_raw)
        .unwrap_or_else(|e| panic!("Cannot parse {}: {e}", schema_path.display()));

    let mut api_name      = "ApiRecord".to_owned();
    let mut db_name       = "DbRecord".to_owned();
    let mut table_name    = "records".to_owned();
    let mut api_fields: Vec<String> = vec![];
    let mut db_fields:  Vec<String> = vec![];
    let mut transform_name = "Transform".to_owned();
    let mut envelope_name  = "ApiResponse".to_owned();
    let mut mapping_rules: Vec<(String, String)> = vec![];

    for (name, def) in &schema.record {
        if def.fetcher.is_some() {
            api_name      = name.clone();
            envelope_name = format!("{name}Response");
            api_fields    = def.fields.iter().map(|f| {
                let ty = if f.ty.starts_with("Option") { "?" } else { "" };
                format!("{}{}", f.name, ty)
            }).collect();
        }
        if def.sink.is_some() {
            db_name = name.clone();
            if let Some(ref sh) = def.sink { table_name = sh.table.clone(); }
            db_fields = def.fields.iter().map(|f| f.name.clone()).collect();
        }
    }
    for mapping in schema.mapping.values() {
        transform_name = mapping.name.clone();
        mapping_rules  = mapping.rules.iter()
            .map(|r| (r.field.clone(), r.rule.clone()))
            .take(8)
            .collect();
    }

    // ── Parse pipeline (best-effort — file may not exist yet) ─────────────
    let pipeline_val = fs::read_to_string(pipeline_path).ok()
        .and_then(|s| toml::from_str::<toml::Value>(&s).ok());

    let job_name = pipeline_val.as_ref()
        .and_then(|v| v.get("job")?.get("name")?.as_str().map(str::to_owned))
        .unwrap_or_else(|| table_name.replace('_', "-"));

    let has_queue = pipeline_val.as_ref()
        .map(|v| v.get("queues").is_some())
        .unwrap_or(false);

    let sink_label    = if has_queue { "send_to_queue" } else { "tx_upsert" };
    let pattern_label = if has_queue { "case 3/4 — async queue" } else { "case 1 — tx per window" };

    // Trigger label shown in the scheduler box at the bottom of the diagram.
    let trigger_label: String = pipeline_val.as_ref()
        .and_then(|v| {
            if let Some(t) = v.get("job").and_then(|j| j.get("trigger")) {
                let ty = t.get("type").and_then(|x| x.as_str()).unwrap_or("cron");
                return Some(match ty {
                    "once"      => "trigger: once (run-and-exit)".to_owned(),
                    "webhook"   => {
                        let port = t.get("port").and_then(|p| p.as_integer()).unwrap_or(8080);
                        format!("trigger: webhook  POST :{port}  ·  mutex-skip")
                    }
                    "pg_notify" => {
                        let ch = t.get("channel")
                            .and_then(|c| c.as_str()
                                .or_else(|| c.get("default")?.as_str()))
                            .unwrap_or("sync_trigger");
                        format!("trigger: pg_notify  LISTEN \"{ch}\"  ·  mutex-skip")
                    }
                    _ => {
                        let c = t.get("cron")
                            .and_then(|cv| cv.as_str()
                                .or_else(|| cv.get("default")?.as_str()))
                            .unwrap_or("0 */30 * * * *");
                        format!("trigger: cron \"{c}\"  ·  mutex-skip")
                    }
                });
            }
            let sch = v.get("job")?.get("scheduler")?;
            let c = sch.get("cron")?.as_str()
                .or_else(|| sch.get("cron")?.get("default")?.as_str())
                .unwrap_or("0 */30 * * * *");
            Some(format!("trigger: cron \"{c}\"  ·  mutex-skip if previous tick running"))
        })
        .unwrap_or_else(|| "trigger: cron \"0 */30 * * * *\"  ·  mutex-skip".to_owned());

    // Detect [init_job] presence for SVG lifecycle section
    let has_init_job = pipeline_val.as_ref()
        .map(|v| v.get("init_job").is_some())
        .unwrap_or(false);

    let init_job_steps: Vec<String> = pipeline_val.as_ref()
        .and_then(|v| v.get("init_job")?.get("steps")?.as_array())
        .map(|arr| arr.iter()
            .filter_map(|s| s.get("type").and_then(|t| t.as_str()).map(str::to_owned))
            .collect())
        .unwrap_or_default();

    // ── Layout constants ──────────────────────────────────────────────────
    let w           = 900i32;
    let col_gap     = 40i32;
    let box_w       = (w - col_gap * 4) / 3;
    let row_h       = 36i32;
    let section_pad = 14i32;

    let max_fields = 10usize;
    let api_display: Vec<String> = api_fields.iter().take(max_fields).cloned()
        .chain(if api_fields.len() > max_fields {
            vec![format!("… +{}", api_fields.len() - max_fields)]
        } else { vec![] })
        .collect();
    let db_display: Vec<String> = db_fields.iter().take(max_fields).cloned()
        .chain(if db_fields.len() > max_fields {
            vec![format!("… +{}", db_fields.len() - max_fields)]
        } else { vec![] })
        .collect();
    let rule_display: Vec<String> = mapping_rules.iter()
        .map(|(f, r)| format!("{f}  →  {r}"))
        .collect();

    let api_box_h  = section_pad * 2 + row_h * api_display.len()  as i32 + 20;
    let db_box_h   = section_pad * 2 + row_h * db_display.len()   as i32 + 20;
    let rule_box_h = section_pad * 2 + row_h * rule_display.len() as i32 + 20;
    let schema_h   = api_box_h.max(db_box_h).max(rule_box_h) + 60;

    let c1x = col_gap;
    let c2x = col_gap * 2 + box_w;
    let c3x = col_gap * 3 + box_w * 2;

    // ── Build SVG string ──────────────────────────────────────────────────
    let mut svg = String::new();

    let init_h = if has_init_job { 80i32 + 26 * init_job_steps.len() as i32 } else { 0i32 };

    // Pre-compute pipe_h from the actual step tree so total_h is accurate.
    let pre_pipe_h: i32 = {
        let grps: HashMap<String, toml::Value> = pipeline_val.as_ref()
            .and_then(|v| v.get("step_groups")?.as_table())
            .map(|t| t.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();
        let steps_val: Vec<SvgStep> = pipeline_val.as_ref()
            .and_then(|v| v.get("main_job")?.get("steps")?.as_array())
            .filter(|arr| !arr.is_empty())
            .map(|arr| arr.iter().map(|s| resolve_svg_step(s, &grps, 0)).collect())
            .unwrap_or_else(|| vec![SvgStep {
                label: String::new(), sub: String::new(),
                is_wrapper: false, children: vec![],
            }; 5]);
        svg_steps_total_height(&steps_val) + 40
    };

    let total_h = 80 + 60 + schema_h + 40 + pre_pipe_h + 40 + init_h + 160 + 40 + 80;
    svg.push_str(&format!(
        "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"{w}\" height=\"{total_h}\" \
         viewBox=\"0 0 {w} {total_h}\" \
         font-family=\"ui-monospace,SFMono-Regular,'SF Mono',Menlo,Consolas,monospace\" \
         font-size=\"13\">\n"
    ));
    // Static CSS + defs — pushed directly to avoid Rust 2021 format! issues
    // with hex literals like #888780 and keywords like auto-start-reverse.
    svg.push_str(
        "<style>\n\
         .bg   { fill:#f8f8f6; }\n\
         .box  { fill:#fff; stroke:#d4d2c8; stroke-width:1; }\n\
         .hdr  { fill:#eeedfe; stroke:#afa9ec; stroke-width:1; }\n\
         .hdr2 { fill:#e1f5ee; stroke:#5dcaa5; stroke-width:1; }\n\
         .hdr3 { fill:#faeeda; stroke:#ef9f27; stroke-width:1; }\n\
         .hdr4 { fill:#faece7; stroke:#f0997b; stroke-width:1; }\n\
         .hdr5 { fill:#f1efe8; stroke:#b4b2a9; stroke-width:1; }\n\
         .ht   { fill:#3c3489; font-weight:600; font-size:14px; }\n\
         .ht2  { fill:#085041; font-weight:600; font-size:14px; }\n\
         .ht3  { fill:#633806; font-weight:600; font-size:14px; }\n\
         .ht4  { fill:#712b13; font-weight:600; font-size:14px; }\n\
         .ht5  { fill:#444441; font-weight:600; font-size:14px; }\n\
         .lbl  { fill:#2c2c2a; font-size:12px; }\n\
         .dim  { fill:#888780; font-size:11px; }\n\
         .arr  { stroke:#888780; stroke-width:1.5; fill:none; marker-end:url(#arr); }\n\
         .arr2 { stroke:#378add; stroke-width:1.5; fill:none; marker-end:url(#arr2); }\n\
         .dash { stroke:#b4b2a9; stroke-width:1; stroke-dasharray:4 3; fill:none; }\n\
         .title { fill:#2c2c2a; font-size:18px; font-weight:600; font-family:ui-sans-serif,system-ui,sans-serif; }\n\
         .sub   { fill:#888780; font-size:12px; font-family:ui-sans-serif,system-ui,sans-serif; }\n\
         </style>\n\
         <defs>\n\
         <marker id=\"arr\" viewBox=\"0 0 10 10\" refX=\"8\" refY=\"5\" markerWidth=\"6\" markerHeight=\"6\" orient=\"auto-start-reverse\">\n\
           <path d=\"M2 1L8 5L2 9\" fill=\"none\" stroke=\"#888780\" stroke-width=\"1.5\" stroke-linecap=\"round\" stroke-linejoin=\"round\"/>\n\
         </marker>\n\
         <marker id=\"arr2\" viewBox=\"0 0 10 10\" refX=\"8\" refY=\"5\" markerWidth=\"6\" markerHeight=\"6\" orient=\"auto-start-reverse\">\n\
           <path d=\"M2 1L8 5L2 9\" fill=\"none\" stroke=\"#378add\" stroke-width=\"1.5\" stroke-linecap=\"round\" stroke-linejoin=\"round\"/>\n\
         </marker>\n\
         </defs>\n"
    );
    svg.push_str(&format!("<rect width=\"{w}\" height=\"{total_h}\" class=\"bg\" rx=\"0\"/>\n"));

    // ── Title ──────────────────────────────────────────────────────────────
    svg.push_str(&format!(
        "<text x=\"40\" y=\"44\" class=\"title\">{job_name}  —  pipeline architecture</text>\n\
         <text x=\"40\" y=\"62\" class=\"sub\">pattern: {pattern_label}  ·  {trigger_label}  ·  sink: {table_name}</text>\n"
    ));

    // ── Lifecycle strip ────────────────────────────────────────────────────
    // Shows the full startup → trigger → tick sequence at a glance.
    let lifecycle_y = 74i32;
    let lc_items: &[(&str, &str)] = if has_init_job {
        &[("startup", "hdr5"), ("init_job", "hdr3"), ("trigger", "hdr"), ("pre_job", "hdr2"),
          ("main_job", "hdr2"), ("post_job", "hdr2")]
    } else {
        &[("startup", "hdr5"), ("trigger", "hdr"), ("pre_job", "hdr2"),
          ("main_job", "hdr2"), ("post_job", "hdr2")]
    };
    let lc_w = (w - 40) / lc_items.len() as i32;
    for (i, (label, hdr)) in lc_items.iter().enumerate() {
        let lx = 20 + i as i32 * lc_w;
        let tc = match *hdr { "hdr" => "ht", "hdr2" => "ht2", "hdr3" => "ht3", _ => "ht5" };
        svg.push_str(&format!(
            "<rect x=\"{lx}\" y=\"{lifecycle_y}\" width=\"{}\" height=\"20\" rx=\"0\" class=\"{hdr}\"/>\n\
             <text x=\"{}\" y=\"{}\" class=\"{tc}\" text-anchor=\"middle\" font-size=\"11\">{label}</text>\n",
            lc_w - 1,
            lx + lc_w / 2, lifecycle_y + 13
        ));
        if i + 1 < lc_items.len() {
            svg.push_str(&format!(
                "<text x=\"{}\" y=\"{}\" class=\"dim\" font-size=\"10\">→</text>\n",
                lx + lc_w - 6, lifecycle_y + 13
            ));
        }
    }

    // ── Schema section ────────────────────────────────────────────────────
    let sy = 80 + 30i32;  // shifted down to make room for lifecycle strip
    svg.push_str(&format!(
        "<rect x=\"20\" y=\"{sy}\" width=\"{}\" height=\"{schema_h}\" rx=\"10\" class=\"box\"/>\n\
         <text x=\"40\" y=\"{}\" class=\"dim\">schema.toml</text>\n",
        w - 40, sy + 18
    ));

    // Helper closure: draw a labelled record box
    let draw_box = |x: i32, y: i32, w_b: i32, hdr: &str, tc: &str,
                    title: &str, subtitle: &str, fields: &[String]| -> String {
        let h          = section_pad * 2 + row_h * fields.len() as i32 + 24;
        let hdr_fill_y = y + 24;
        let mut s = format!(
            "<rect x=\"{x}\" y=\"{y}\" width=\"{w_b}\" height=\"{h}\" rx=\"6\" class=\"box\"/>\n\
             <rect x=\"{x}\" y=\"{y}\" width=\"{w_b}\" height=\"36\" rx=\"6\" class=\"{hdr}\"/>\n\
             <rect x=\"{x}\" y=\"{hdr_fill_y}\" width=\"{w_b}\" height=\"12\" class=\"{hdr}\"/>\n\
             <text x=\"{}\" y=\"{}\" class=\"{tc}\">{title}</text>\n\
             <text x=\"{}\" y=\"{}\" class=\"dim\">{subtitle}</text>\n",
            x + 12, y + 20,
            x + 12, y + 34
        );
        for (i, f) in fields.iter().enumerate() {
            let fy = y + 36 + section_pad + i as i32 * row_h + row_h / 2;
            s.push_str(&format!("<text x=\"{}\" y=\"{fy}\" class=\"lbl\">{f}</text>\n", x + 16));
        }
        s
    };

    let box_y = sy + 30i32;
    svg.push_str(&draw_box(c1x, box_y, box_w, "hdr",  "ht",
        &envelope_name,  &format!("source: {api_name}"),      &api_display));
    svg.push_str(&draw_box(c2x, box_y, box_w, "hdr4", "ht4",
        &transform_name, &format!("{api_name} → {db_name}"),  &rule_display));
    svg.push_str(&draw_box(c3x, box_y, box_w, "hdr2", "ht2",
        &db_name,        &format!("sink: {table_name}"),       &db_display));

    let mid_y = box_y + 18;
    svg.push_str(&format!(
        "<line x1=\"{}\" y1=\"{mid_y}\" x2=\"{}\" y2=\"{mid_y}\" class=\"arr2\"/>\n\
         <line x1=\"{}\" y1=\"{mid_y}\" x2=\"{}\" y2=\"{mid_y}\" class=\"arr2\"/>\n",
        c1x + box_w, c2x - 2,
        c2x + box_w, c3x - 2
    ));

    // ── Init-job section (optional) ───────────────────────────────────────
    let init_section_h = init_h;
    let iy = sy + schema_h + 30i32;

    if has_init_job {
        svg.push_str(&format!(
            "<rect x=\"20\" y=\"{iy}\" width=\"{}\" height=\"{init_section_h}\" rx=\"10\" class=\"box\"/>\n\
             <text x=\"40\" y=\"{}\" class=\"dim\">pipeline.toml  ·  [init_job]  — runs once at startup</text>\n",
            w - 40, iy + 18
        ));

        let ij_step_h = 26i32;
        let ij_steps_label = if init_job_steps.is_empty() {
            vec!["(no steps declared)".to_owned()]
        } else {
            init_job_steps.clone()
        };
        for (i, step_type) in ij_steps_label.iter().enumerate() {
            let ssx = 40i32;
            let ssy = iy + 28 + i as i32 * ij_step_h;
            svg.push_str(&format!(
                "<rect x=\"{ssx}\" y=\"{ssy}\" width=\"{}\" height=\"{}\" rx=\"4\" class=\"hdr3\"/>\n\
                 <text x=\"{}\" y=\"{}\" class=\"ht3\" font-size=\"12\">{step_type}</text>\n",
                w - 80, ij_step_h - 2,
                ssx + 8, ssy + 14
            ));
        }
    }

    // ── Pipeline section — dynamic composable step tree ───────────────────

    // Collect steps for rendering: new-style (main_job.steps) or legacy flat list.
    let groups_val: HashMap<String, toml::Value> = pipeline_val.as_ref()
        .and_then(|v| v.get("step_groups"))
        .and_then(|g| g.as_table())
        .map(|t| t.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        .unwrap_or_default();

    let svg_steps: Vec<SvgStep> = pipeline_val.as_ref()
        .and_then(|v| v.get("main_job")?.get("steps")?.as_array())
        .filter(|arr| !arr.is_empty())
        .map(|arr| arr.iter().map(|s| resolve_svg_step(s, &groups_val, 0)).collect())
        .unwrap_or_else(|| vec![
            SvgStep { label: "pre_job".into(),  sub: "resources".into(),        is_wrapper: false, children: vec![] },
            SvgStep { label: "fetch".into(),     sub: format!("→ {api_name}"),   is_wrapper: false, children: vec![] },
            SvgStep { label: "transform".into(), sub: format!("→ {db_name}"),    is_wrapper: false, children: vec![] },
            SvgStep { label: sink_label.into(),  sub: format!("→ {table_name}"), is_wrapper: false, children: vec![] },
            SvgStep { label: "post_job".into(),  sub: "summary + hooks".into(),  is_wrapper: false, children: vec![] },
        ]);

    let steps_content_h = svg_steps_total_height(&svg_steps);
    let pipe_h = steps_content_h + 40i32;

    let py = iy + init_section_h + if has_init_job { 14i32 } else { 0i32 };
    svg.push_str(&format!(
        "<rect x=\"20\" y=\"{py}\" width=\"{}\" height=\"{pipe_h}\" rx=\"10\" class=\"box\"/>\n\
         <text x=\"40\" y=\"{}\" class=\"dim\">pipeline.toml  ·  main_job</text>\n",
        w - 40, py + 18
    ));

    let mut pipe_svg = String::new();
    render_steps_svg(&svg_steps, 40, py + 26, w - 80, &mut pipe_svg);
    svg.push_str(&pipe_svg);

    let slot_y = py + pipe_h + 8i32;
    svg.push_str(&format!("<text x=\"40\" y=\"{slot_y}\" class=\"dim\">data flow:</text>\n"));

    let flow_items: &[(&str, &str)] = &[
        ("HTTP response",                                "#378add"),
        ("api_rows slot",                               "#7f77dd"),
        (db_name.as_str(),                              "#378add"),
        (if has_queue { "queue / consumer" } else { "db_rows slot" }, "#7f77dd"),
        (table_name.as_str(),                           "#1d9e75"),
    ];
    let fi_w = (w - 80) / flow_items.len() as i32;
    for (i, (lbl, color)) in flow_items.iter().enumerate() {
        let fx = 40 + i as i32 * fi_w + fi_w / 2;
        let fy = slot_y + 24;
        let rx = 40 + i as i32 * fi_w + 4;
        let rw = fi_w - 8;
        svg.push_str(&format!(
            "<rect x=\"{rx}\" y=\"{fy}\" width=\"{rw}\" height=\"24\" rx=\"4\" \
             fill=\"{color}\" opacity=\"0.15\" stroke=\"{color}\" stroke-width=\"0.5\"/>\n\
             <text x=\"{fx}\" y=\"{}\" class=\"lbl\" text-anchor=\"middle\" fill=\"{color}\">{lbl}</text>\n",
            fy + 15
        ));
        if i + 1 < flow_items.len() {
            let lx1 = 40 + i as i32 * fi_w + fi_w - 4;
            let lx2 = 40 + (i + 1) as i32 * fi_w + 4;
            let ly  = fy + 12;
            svg.push_str(&format!(
                "<line x1=\"{lx1}\" y1=\"{ly}\" x2=\"{lx2}\" y2=\"{ly}\" \
                 stroke=\"{color}\" stroke-width=\"1\" marker-end=\"url(#arr2)\"/>\n"
            ));
        }
    }

    // ── Post-job / scheduler section ──────────────────────────────────────
    let qy = py + pipe_h + 24i32;
    let qh = 160i32;
    svg.push_str(&format!(
        "<rect x=\"20\" y=\"{qy}\" width=\"{}\" height=\"{qh}\" rx=\"10\" class=\"box\"/>\n\
         <text x=\"40\" y=\"{}\" class=\"dim\">post_job  +  scheduler</text>\n",
        w - 40, qy + 18
    ));

    let pj_items: &[(&str, &str, &str, &str)] = &[
        ("log_summary",                          "fetched · upserted · skipped", "hdr3", "ht3"),
        ("raw_sql",                              "post-sync SQL",                "hdr3", "ht3"),
        (if has_queue { "drain_queue" } else { "—" }, "await consumer",          "hdr3", "ht3"),
        ("custom hook",                          "registered in main.rs",        "hdr3", "ht3"),
    ];
    let pj_w = (w - 80 - 3 * 16) / 4;
    for (i, (lbl, sub, hdr, tc)) in pj_items.iter().enumerate() {
        let px2 = 40 + i as i32 * (pj_w + 16);
        let py2 = qy + 28i32;
        svg.push_str(&format!(
            "<rect x=\"{px2}\" y=\"{py2}\" width=\"{pj_w}\" height=\"50\" rx=\"6\" class=\"box\"/>\n\
             <rect x=\"{px2}\" y=\"{py2}\" width=\"{pj_w}\" height=\"26\" rx=\"6\" class=\"{hdr}\"/>\n\
             <rect x=\"{px2}\" y=\"{}\" width=\"{pj_w}\" height=\"12\" class=\"{hdr}\"/>\n\
             <text x=\"{}\" y=\"{}\" class=\"{tc}\" text-anchor=\"middle\">{lbl}</text>\n\
             <text x=\"{}\" y=\"{}\" class=\"dim\" text-anchor=\"middle\">{sub}</text>\n",
            py2 + 14,
            px2 + pj_w / 2, py2 + 17,
            px2 + pj_w / 2, py2 + 42
        ));
    }

    let sch_y = qy + 92i32;
    svg.push_str(&format!(
        "<rect x=\"40\" y=\"{sch_y}\" width=\"{}\" height=\"44\" rx=\"6\" class=\"box\"/>\n\
         <rect x=\"40\" y=\"{sch_y}\" width=\"{}\" height=\"22\" rx=\"6\" class=\"hdr5\"/>\n\
         <rect x=\"40\" y=\"{}\" width=\"{}\" height=\"10\" class=\"hdr5\"/>\n\
         <text x=\"{}\" y=\"{}\" class=\"ht5\" text-anchor=\"middle\">trigger</text>\n\
         <text x=\"{}\" y=\"{}\" class=\"dim\" text-anchor=\"middle\">\
         {trigger_label}  ·  pipeline slots survive ticks\
         </text>\n",
        w - 80, w - 80,
        sch_y + 12, w - 80,
        w / 2, sch_y + 15,
        w / 2, sch_y + 38
    ));

    // ── Footer ────────────────────────────────────────────────────────────
    let fy2 = py + pipe_h + qh + 48i32;
    svg.push_str(&format!(
        "<text x=\"{}\" y=\"{fy2}\" class=\"dim\" text-anchor=\"middle\">\
         generated by sync_engine::codegen::generate_architecture_svg_file  ·  do not edit\
         </text>\n\
         </svg>",
        w / 2
    ));

    svg
}


