// user-sync/src/config.rs
//
// Loaded via the `config` crate which merges:
//   1. config.toml  (defaults)
//   2. Environment variables using __ as the section separator
//      e.g. AUTH__CLIENT_SECRET overrides [auth] client_secret
//
// `dotenvy` in main.rs loads a .env file before this runs.

use anyhow::Result;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    pub log: LogConfig,
    pub auth: AuthConfig,
    pub source: SourceConfig,
    pub sink: SinkConfig,
    pub scheduler: SchedulerConfig,
}

#[derive(Debug, Deserialize)]
pub struct LogConfig {
    pub rust_log: String,
}

#[derive(Debug, Deserialize)]
pub struct AuthConfig {
    pub token_url: String,
    pub client_id: String,
    pub client_secret: String,
}

#[derive(Debug, Deserialize)]
pub struct SourceConfig {
    pub user_endpoint: String,
    pub start_interval: i64,
    pub end_interval: i64,
    pub interval_limit: i64,
    pub window_sleep_secs: u64,
    #[serde(default)]
    pub include_realm_types: String,
}

#[derive(Debug, Deserialize)]
pub struct SinkConfig {
    pub database_url: String,
    #[serde(default)]
    pub sync_sql: String,
}

#[derive(Debug, Deserialize)]
pub struct SchedulerConfig {
    pub cron: String,
}

impl AppConfig {
    pub fn from_env() -> Result<Self> {
        // Load .env file if present (silently ignored if missing)
        dotenvy::dotenv().ok();

        let cfg = config::Config::builder()
            .add_source(config::File::with_name("config").required(false))
            .add_source(
                config::Environment::default()
                    .separator("__")
                    .try_parsing(true),
            )
            .build()?
            .try_deserialize()?;

        Ok(cfg)
    }
}
