// user-sync/src/generated/mod.rs
//
// All impls (Upsertable, UpsertableInTx, Transform, HasEnvelope) are emitted
// by build.rs → codegen. Nothing hand-written here.
//
// The old manual `impl DbUser { pub async fn upsert_in_tx(...) }` block is
// gone — codegen now emits `impl UpsertableInTx for DbUser` automatically.

pub mod records {
    use chrono::{DateTime, Utc};
    sync_engine::include_schema!("records");
}

pub mod envelopes {
    use super::records::*;
    sync_engine::include_schema!("envelopes");
}

pub mod upserts {
    use super::records::*;
    sync_engine::include_schema!("upserts");
}

pub mod transforms {
    use super::records::*;
    sync_engine::include_schema!("transforms");
}

/// Rule helpers referenced by generated transform code.
pub mod rules {
    use anyhow::{Context, Result};
    use chrono::{DateTime, Utc};

    pub trait EpochConvert {
        type Output;
        fn convert(self) -> Result<Self::Output>;
    }

    impl EpochConvert for i64 {
        type Output = DateTime<Utc>;
        fn convert(self) -> Result<DateTime<Utc>> {
            DateTime::from_timestamp_millis(self).context("Invalid epoch ms")
        }
    }

    impl EpochConvert for Option<i64> {
        type Output = Option<DateTime<Utc>>;
        fn convert(self) -> Result<Option<DateTime<Utc>>> {
            self.map(|ms| DateTime::from_timestamp_millis(ms).context("Invalid epoch ms"))
                .transpose()
        }
    }

    pub fn epoch_ms_to_ts<T: EpochConvert>(v: T) -> Result<T::Output> {
        v.convert()
    }
}
