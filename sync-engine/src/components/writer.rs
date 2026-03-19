// sync-engine/src/components/writer.rs
use anyhow::Result;
use async_trait::async_trait;
use sqlx::{postgres::PgPoolOptions, PgPool};
use tracing::{error, info, warn};

use crate::pipeline::component_registry::{AnyPostHook, AnyWriter};
use crate::pipeline::Sink;

/// Implemented by generated record types (via build.rs upserts.rs).
#[async_trait]
pub trait Upsertable: Send + Sync + 'static {
    async fn upsert(&self, pool: &PgPool) -> Result<()>;
}

pub struct PostgresWriter<T> {
    pool: PgPool,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: Upsertable> PostgresWriter<T> {
    pub async fn new(database_url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await?;
        Ok(Self {
            pool,
            _phantom: std::marker::PhantomData,
        })
    }
}

#[async_trait]
impl<T: Upsertable> Sink for PostgresWriter<T> {
    type Item = T;
    async fn write(&self, items: Vec<T>) {
        info!("Writing {} records", items.len());
        for item in items {
            if let Err(e) = item.upsert(&self.pool).await {
                error!(error = %e, "Upsert failed — skipping row");
            }
        }
    }
}

/// Wraps PostgresWriter so it can satisfy AnyWriter (type-erased).
pub struct WriterAdapter<T>(pub PostgresWriter<T>);

#[async_trait]
impl<T: Upsertable> AnyWriter for WriterAdapter<T> {
    async fn write_erased(&self, items: Vec<Box<dyn std::any::Any + Send>>) {
        let typed: Vec<T> = items
            .into_iter()
            .filter_map(|b| b.downcast::<T>().ok().map(|v| *v))
            .collect();
        self.0.write(typed).await;
    }
}

pub struct RawSqlHook {
    pub pool: PgPool,
    pub sql: String,
}

impl RawSqlHook {
    pub fn new(pool: PgPool, sql: String) -> Self {
        Self { pool, sql }
    }
}

#[async_trait]
impl AnyPostHook for RawSqlHook {
    async fn on_complete(&self) {
        info!("Running post-sync SQL");
        if let Err(e) = sqlx::raw_sql(&self.sql).execute(&self.pool).await {
            warn!(error = %e, "Post-sync SQL failed");
        }
    }
}

pub struct NoopHook;
#[async_trait]
impl AnyPostHook for NoopHook {
    async fn on_complete(&self) {}
}
