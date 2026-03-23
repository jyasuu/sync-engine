// sync-engine/src/components/chunker.rs
use async_trait::async_trait;
use chrono::Utc;
use std::time::Duration;
use tracing::info;

use crate::pipeline::primitives::{Chunker, FetchParams};

pub struct DateWindowChunker {
    cursor:         i64,
    end_interval:   i64,
    interval_limit: i64,
    chunk_sleep:    Duration,
    first:          bool,
}

impl DateWindowChunker {
    pub fn new(
        start_interval: i64,
        end_interval:   i64,
        interval_limit: i64,
        chunk_sleep:    Duration,
    ) -> Self {
        Self {
            cursor: start_interval,
            end_interval,
            interval_limit,
            chunk_sleep,
            first: true,
        }
    }
}

#[async_trait]
impl Chunker for DateWindowChunker {
    async fn next_window(&mut self) -> Option<FetchParams> {
        if self.cursor <= self.end_interval {
            return None;
        }
        if !self.first {
            info!("Sleeping {:?} between windows", self.chunk_sleep);
            tokio::time::sleep(self.chunk_sleep).await;
        }
        self.first = false;
        let now        = Utc::now();
        let window_end = (self.cursor - self.interval_limit).max(self.end_interval);
        let start_str  = (now - chrono::Duration::days(self.cursor))
            .format("%Y%m%d")
            .to_string();
        let end_str = (now - chrono::Duration::days(window_end))
            .format("%Y%m%d")
            .to_string();
        info!(start = %start_str, end = %end_str, "Next window");
        self.cursor = window_end;
        Some(
            FetchParams::new()
                .with("start_time", start_str)
                .with("end_time",   end_str),
        )
    }
}
