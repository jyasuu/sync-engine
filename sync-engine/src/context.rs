// sync-engine/src/context.rs
//
// JobContext is the single shared object passed to every Step.
// It holds:
//   - SlotMap     typed data shared between steps
//   - Connections db pool, auth client, http client
//   - QueueMap    named async channels for producer/consumer patterns
//   - Metadata    job name, current window, run-time config strings
//
// Steps never communicate through function arguments — only through the
// context. This makes each step independently testable and reusable.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, RwLock};

use crate::components::auth::OAuth2Auth;
use crate::slot::{SlotMap, SlotScope};

// ── Queue types ───────────────────────────────────────────────────────────

/// A named, bounded async channel registered in pre_job.
pub struct QueueEntry {
    /// Sender side — held by producer steps.
    pub tx: mpsc::Sender<Box<dyn std::any::Any + Send>>,
    /// Receiver side — consumed by the spawned consumer task.
    pub rx: Mutex<Option<mpsc::Receiver<Box<dyn std::any::Any + Send>>>>,
    pub capacity: usize,
}

impl QueueEntry {
    pub fn new(capacity: usize) -> Self {
        let (tx, rx) = mpsc::channel(capacity);
        Self {
            tx,
            rx: Mutex::new(Some(rx)),
            capacity,
        }
    }

    /// Take the receiver — can only be called once (by the consumer task).
    pub async fn take_rx(
        &self,
    ) -> Option<mpsc::Receiver<Box<dyn std::any::Any + Send>>> {
        self.rx.lock().await.take()
    }
}

// ── Connections ───────────────────────────────────────────────────────────

/// All external connections created in pre_job and shared read-only.
#[derive(Clone)]
pub struct Connections {
    #[cfg(feature = "postgres")]
    pub db:          sqlx::PgPool,
    pub auth:        Arc<OAuth2Auth>,
    pub http:        reqwest::Client,
    pub endpoint:    String,
    pub extra_query: Vec<(String, String)>,
    /// Query param name for window start (default: "start_time")
    pub start_param: String,
    /// Query param name for window end (default: "end_time")
    pub end_param:   String,
    /// strftime format string for date params (default: "%Y%m%d")
    pub date_format: String,
}

// ── WindowMeta ────────────────────────────────────────────────────────────

/// Set by the iterator at the start of each window tick.
/// Steps can read this to know which window they are processing.
#[derive(Debug, Clone, Default)]
pub struct WindowMeta {
    pub start_day: i64,   // days-ago value
    pub end_day:   i64,
    pub start_str: String, // formatted for API query
    pub end_str:   String,
    pub index:     usize,  // 0-based window number
}

// ── JobContext ────────────────────────────────────────────────────────────

pub struct JobContext {
    /// Typed data slots — shared between steps.
    pub slots: RwLock<SlotMap>,

    /// External connections.
    pub connections: Connections,

    /// Named async channels (for producer/consumer pattern).
    pub queues: RwLock<HashMap<String, Arc<QueueEntry>>>,

    /// Current window — updated by the iterator before each tick.
    pub window: RwLock<WindowMeta>,

    /// Arbitrary string config values accessible to steps.
    /// Populated from pipeline.toml env-var lookups at startup.
    pub config: HashMap<String, String>,

    /// Job-level name (for logging).
    pub job_name: String,
}

impl JobContext {
    pub fn new(
        connections: Connections,
        config: HashMap<String, String>,
        job_name: impl Into<String>,
    ) -> Self {
        Self {
            slots:       RwLock::new(SlotMap::new()),
            connections,
            queues:      RwLock::new(HashMap::new()),
            window:      RwLock::new(WindowMeta::default()),
            config,
            job_name:    job_name.into(),
        }
    }

    // ── Slot helpers ──────────────────────────────────────────────────────

    pub async fn slot_write<T: std::any::Any + Send + Sync + 'static>(
        &self,
        key: &str,
        value: T,
    ) -> anyhow::Result<()> {
        self.slots.write().await.write(key, value).await
    }

    pub async fn slot_read<T: std::any::Any + Send + Sync + Clone + 'static>(
        &self,
        key: &str,
    ) -> anyhow::Result<T> {
        self.slots.read().await.read(key).await
    }

    pub async fn slot_append<T: std::any::Any + Send + Sync + 'static>(
        &self,
        key: &str,
        items: Vec<T>,
    ) -> anyhow::Result<()> {
        self.slots.write().await.append(key, items).await
    }

    pub async fn slot_is_set(&self, key: &str) -> bool {
        self.slots.read().await.is_set(key)
    }

    pub async fn clear_window_slots(&self) {
        self.slots.write().await.clear_scope(SlotScope::Window).await;
    }

    pub async fn clear_job_slots(&self) {
        self.slots.write().await.clear_scope(SlotScope::Job).await;
    }

    // ── Queue helpers ─────────────────────────────────────────────────────

    pub async fn register_queue(&self, name: impl Into<String>, capacity: usize) {
        self.queues
            .write()
            .await
            .insert(name.into(), Arc::new(QueueEntry::new(capacity)));
    }

    pub async fn queue_send<T: std::any::Any + Send + 'static>(
        &self,
        name: &str,
        item: T,
    ) -> anyhow::Result<()> {
        let queues = self.queues.read().await;
        let entry = queues
            .get(name)
            .ok_or_else(|| anyhow::anyhow!("Queue \"{name}\" not registered"))?;
        entry
            .tx
            .send(Box::new(item))
            .await
            .map_err(|_| anyhow::anyhow!("Queue \"{name}\" receiver dropped"))
    }

    pub async fn get_queue(&self, name: &str) -> anyhow::Result<Arc<QueueEntry>> {
        self.queues
            .read()
            .await
            .get(name)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Queue \"{name}\" not registered"))
    }

    // ── Config helpers ────────────────────────────────────────────────────

    pub fn cfg(&self, key: &str) -> anyhow::Result<&str> {
        self.config
            .get(key)
            .map(|s| s.as_str())
            .ok_or_else(|| anyhow::anyhow!("Config key \"{key}\" not found in context"))
    }

    pub fn cfg_or<'a>(&'a self, key: &str, default: &'a str) -> &'a str {
        self.config.get(key).map(|s| s.as_str()).unwrap_or(default)
    }
}
