// user-sync/src/config.rs
//
// Single source of truth for all environment variables.
// Loaded once at startup and passed into each primitive constructor.
// No primitive reads std::env::var() directly — they all receive AppConfig.

use anyhow::{Context, Result};
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct AppConfig {
    // ── DateWindowChunker ─────────────────────────────────────────────
    pub start_interval: i64,
    pub end_interval: i64,
    pub interval_limit: i64,
    pub chunk_sleep: Duration,

    // ── OAuth2Auth ────────────────────────────────────────────────────
    pub token_url: String,
    pub client_id: String,
    pub client_secret: String,

    // ── HttpJsonFetcher ───────────────────────────────────────────────
    pub user_endpoint: String,
    pub include_realm_types: Option<String>,

    // ── PostgresWriter / RawSqlHook ───────────────────────────────────
    pub database_url: String,
    pub sync_sql: Option<String>,
}

impl AppConfig {
    pub fn from_env() -> Result<Self> {
        dotenvy::dotenv().ok();
        Ok(Self {
            start_interval: env_parse("START_INTERVAL").unwrap_or(30),
            end_interval: env_parse("END_INTERVAL").unwrap_or(0),
            interval_limit: env_parse("INTERVAL_LIMIT").unwrap_or(7),
            chunk_sleep: Duration::from_secs(60),

            token_url: env("TOKEN_URL")?,
            client_id: env("CLIENT_ID")?,
            client_secret: env("CLIENT_SECRET")?,

            user_endpoint: env("USER_ENDPOINT")?,
            include_realm_types: std::env::var("INCLUDE_REALM_TYPES")
                .ok()
                .filter(|s| !s.is_empty()),

            database_url: env("DATABASE_URL")?,
            sync_sql: std::env::var("SYNC_SQL")
                .ok()
                .filter(|s| !s.trim().is_empty()),
        })
    }
}

fn env(key: &str) -> Result<String> {
    std::env::var(key).with_context(|| format!("{key} not set"))
}

fn env_parse<T: std::str::FromStr>(key: &str) -> Option<T> {
    std::env::var(key).ok()?.parse().ok()
}
