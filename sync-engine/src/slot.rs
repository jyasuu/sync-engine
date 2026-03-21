// sync-engine/src/slot.rs
//
// SlotMap: a type-erased key→value store shared across all job phases.
// Each slot has a scope that controls its lifetime:
//
//   Window   — cleared at the start of every iterator tick
//   Job      — lives for the entire job run; readable in post_job
//   Pipeline — survives across cron ticks (e.g. rate-limit state)
//
// All access goes through JobContext (see context.rs), not directly.

use anyhow::{anyhow, Context, Result};
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

// ── Scope ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotScope {
    /// Cleared at the start of each window iteration.
    Window,
    /// Accumulates across windows; carried into post_job.
    Job,
    /// Survives across cron ticks (lives in the scheduler).
    Pipeline,
}

// ── SlotEntry ─────────────────────────────────────────────────────────────

pub struct SlotEntry {
    pub scope: SlotScope,
    pub(crate) value: Option<Arc<RwLock<Box<dyn Any + Send + Sync>>>>,
}

impl SlotEntry {
    pub fn new(scope: SlotScope) -> Self {
        Self { scope, value: None }
    }
}

// ── SlotMap ───────────────────────────────────────────────────────────────

#[derive(Default)]
pub struct SlotMap {
    pub(crate) slots: HashMap<String, SlotEntry>,
}

impl SlotMap {
    pub fn new() -> Self {
        Self::default()
    }

    /// Declare a slot with a given scope. Must be called before first use.
    pub fn declare(&mut self, key: impl Into<String>, scope: SlotScope) {
        self.slots
            .entry(key.into())
            .or_insert_with(|| SlotEntry::new(scope));
    }

    /// Write a typed value into a slot.
    /// The slot must have been declared. Creates the inner lock on first write.
    pub async fn write<T: Any + Send + Sync + 'static>(
        &mut self,
        key: &str,
        value: T,
    ) -> Result<()> {
        let entry = self
            .slots
            .get_mut(key)
            .with_context(|| format!("Slot \"{key}\" not declared"))?;

        if let Some(ref arc) = entry.value {
            let mut guard = arc.write().await;
            *guard = Box::new(value);
        } else {
            entry.value = Some(Arc::new(RwLock::new(Box::new(value))));
        }
        Ok(())
    }

    /// Read a typed value from a slot, cloning it out.
    /// Returns `Err` if the slot is undeclared or not yet written.
    pub async fn read<T: Any + Send + Sync + Clone + 'static>(&self, key: &str) -> Result<T> {
        let entry = self
            .slots
            .get(key)
            .with_context(|| format!("Slot \"{key}\" not declared"))?;

        let arc = entry
            .value
            .as_ref()
            .with_context(|| format!("Slot \"{key}\" has not been written yet"))?;

        let guard = arc.read().await;
        guard
            .downcast_ref::<T>()
            .cloned()
            .ok_or_else(|| anyhow!("Slot \"{key}\" type mismatch on read"))
    }

    /// Take the value out of a slot, replacing it with None.
    /// Useful when the consumer wants to own the data (avoids a clone for Vec<T>).
    /// If the Arc has multiple holders (rare), falls back to a clone via read().
    pub async fn take<T: Any + Send + Sync + Clone + 'static>(&mut self, key: &str) -> Result<T> {
        let entry = self
            .slots
            .get_mut(key)
            .with_context(|| format!("Slot \"{key}\" not declared"))?;

        let arc = entry
            .value
            .take()
            .with_context(|| format!("Slot \"{key}\" has not been written yet"))?;

        // Fast path: we're the only holder — unwrap directly.
        match Arc::try_unwrap(arc) {
            Ok(lock) => lock
                .into_inner()
                .downcast::<T>()
                .map(|b| *b)
                .map_err(|_| anyhow!("Slot \"{key}\" type mismatch on take")),
            // Slow path: something else holds a reference — clone the value
            // and put the Arc back so the slot remains usable.
            Err(arc) => {
                entry.value = Some(arc.clone());
                let guard = arc.read().await;
                guard
                    .downcast_ref::<T>()
                    .cloned()
                    .ok_or_else(|| anyhow!("Slot \"{key}\" type mismatch on take"))
            }
        }
    }

    /// Append items to a Vec slot. Creates the Vec if the slot is empty.
    /// The slot's value type must be `Vec<T>`.
    pub async fn append<T: Any + Send + Sync + 'static>(
        &mut self,
        key: &str,
        mut items: Vec<T>,
    ) -> Result<()> {
        let entry = self
            .slots
            .get_mut(key)
            .with_context(|| format!("Slot \"{key}\" not declared"))?;

        if let Some(ref arc) = entry.value {
            let mut guard = arc.write().await;
            if let Some(vec) = guard.downcast_mut::<Vec<T>>() {
                vec.append(&mut items);
            } else {
                return Err(anyhow!("Slot \"{key}\" type mismatch on append"));
            }
        } else {
            entry.value = Some(Arc::new(RwLock::new(Box::new(items))));
        }
        Ok(())
    }

    /// Returns true if the slot has been written at least once.
    pub fn is_set(&self, key: &str) -> bool {
        self.slots.get(key).and_then(|e| e.value.as_ref()).is_some()
    }

    /// Clear all slots of a given scope (called by the engine, not by steps).
    pub async fn clear_scope(&mut self, scope: SlotScope) {
        for entry in self.slots.values_mut() {
            if entry.scope == scope {
                entry.value = None;
            }
        }
    }
}
