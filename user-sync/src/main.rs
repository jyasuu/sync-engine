// user-sync/src/main.rs
//
// Loads all configuration once via AppConfig::from_env().
// Passes config values into each primitive constructor — no primitive
// reads env vars directly.

mod config;
mod generated;

use anyhow::Result;
use sync_engine::components::writer::{NoopHook, RawSqlHook, WriterAdapter};
use sync_engine::pipeline::component_registry::AnyPostHook;
use sync_engine::{
    AnyFetcher, AnyTransform, AnyWriter, DateWindowChunker, HttpJsonFetcher, OAuth2Auth,
    PostgresWriter, PrimitiveRegistry, TransformAdapter,
};
use tracing_subscriber::EnvFilter;

use config::AppConfig;
use generated::{envelopes::ApiUserResponse, records::DbUser, transforms::UserTransform};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    // ── Load all config from environment once ─────────────────────────
    let cfg = AppConfig::from_env()?;

    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(620))
        .tcp_keepalive(std::time::Duration::from_secs(30))
        .build()?;

    let mut reg = PrimitiveRegistry::new();

    // ── Chunkers ──────────────────────────────────────────────────────
    let (start, end, limit, sleep) = (
        cfg.start_interval,
        cfg.end_interval,
        cfg.interval_limit,
        cfg.chunk_sleep,
    );
    reg.register_chunker("date-window", move || async move {
        Ok(Box::new(DateWindowChunker::new(start, end, limit, sleep))
            as Box<dyn sync_engine::Chunker>)
    });

    // ── Auth ──────────────────────────────────────────────────────────
    let (h, url, id, secret) = (
        http.clone(),
        cfg.token_url.clone(),
        cfg.client_id.clone(),
        cfg.client_secret.clone(),
    );
    reg.register_auth("oauth2", move || {
        let (h, url, id, secret) = (h.clone(), url.clone(), id.clone(), secret.clone());
        async move {
            Ok(Box::new(OAuth2Auth::new(h, url, id, secret)) as Box<dyn sync_engine::Auth>)
        }
    });

    // ── Fetchers ──────────────────────────────────────────────────────
    let (h, endpoint, realm) = (
        http.clone(),
        cfg.user_endpoint.clone(),
        cfg.include_realm_types.clone(),
    );
    reg.register_fetcher("http-json", move || {
        let (h, endpoint, realm) = (h.clone(), endpoint.clone(), realm.clone());
        async move {
            Ok(
                Box::new(HttpJsonFetcher::<ApiUserResponse>::new(h, endpoint, realm))
                    as Box<dyn AnyFetcher>,
            )
        }
    });

    // ── Mappers (transforms) ──────────────────────────────────────────
    reg.register_mapper("user-fields", || async {
        Ok(Box::new(TransformAdapter(UserTransform)) as Box<dyn AnyTransform>)
    });

    // ── Writers ───────────────────────────────────────────────────────
    let db_url = cfg.database_url.clone();
    reg.register_writer("pg-upsert", move || {
        let db_url = db_url.clone();
        async move {
            Ok(
                Box::new(WriterAdapter(PostgresWriter::<DbUser>::new(&db_url).await?))
                    as Box<dyn AnyWriter>,
            )
        }
    });

    // ── Post-hooks ────────────────────────────────────────────────────
    let (db_url, sync_sql) = (cfg.database_url.clone(), cfg.sync_sql.clone());
    reg.register_post_hook("raw-sql", move || {
        let (db_url, sync_sql) = (db_url.clone(), sync_sql.clone());
        async move {
            let hook: Box<dyn AnyPostHook> = match sync_sql {
                Some(sql) => {
                    let pool = sqlx::postgres::PgPoolOptions::new()
                        .max_connections(1)
                        .connect(&db_url)
                        .await?;
                    Box::new(RawSqlHook::new(pool, sql))
                }
                None => Box::new(NoopHook),
            };
            Ok(hook)
        }
    });

    // ── Assemble from component.toml → schedule from pipeline.toml ────
    reg.build_pipeline_registry("component.toml")
        .await?
        .run_from_toml("pipeline.toml")
        .await
}
