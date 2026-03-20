// user-sync/src/config.rs
//
// All configuration is declared in config.toml.
// Environment variables override individual keys using the pattern:
//   SECTION__KEY  (double underscore as separator)
//
// Examples:
//   AUTH__CLIENT_SECRET=prod-secret
//   SINK__DATABASE_URL=postgres://...
//   SOURCE__USER_ENDPOINT=https://...
//   SCHEDULER__CRON="0 0 3 * * *"
//
// No env var string literals anywhere in Rust code.

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub scheduler: SchedulerConfig,
    pub source: SourceConfig,
    pub auth: AuthConfig,
    pub sink: SinkConfig,
    #[serde(default)]
    pub log: LogConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SchedulerConfig {
    pub cron: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SourceConfig {
    pub user_endpoint: String,
    pub start_interval: i64,
    pub end_interval: i64,
    pub interval_limit: i64,
    #[serde(default = "default_window_sleep")]
    pub window_sleep_secs: u64,
    #[serde(default)]
    pub include_realm_types: String,
}
fn default_window_sleep() -> u64 {
    60
}

#[derive(Debug, Clone, Deserialize)]
pub struct AuthConfig {
    pub token_url: String,
    pub client_id: String,
    pub client_secret: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SinkConfig {
    pub database_url: String,
    #[serde(default)]
    pub sync_sql: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct LogConfig {
    #[serde(default = "default_log")]
    pub rust_log: String,
}
fn default_log() -> String {
    "user_sync=info".into()
}

impl AppConfig {
    pub fn from_env() -> Result<Self> {
        dotenvy::dotenv().ok();

        config::Config::builder()
            // 1. base: config.toml defines all keys and defaults
            .add_source(
                config::File::with_name("config")
                    .format(config::FileFormat::Toml)
                    .required(true),
            )
            // 2. overlay: env vars using __ separator, e.g. AUTH__CLIENT_SECRET
            .add_source(
                config::Environment::default()
                    .separator("__")
                    .try_parsing(true),
            )
            .build()
            .context("Failed to build config")?
            .try_deserialize()
            .context("Failed to deserialize config")
    }
}
