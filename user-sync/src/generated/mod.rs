// user-sync/src/generated/mod.rs
//
// Pulls build.rs output into the crate.
// Also contains the hand-written rule helper library used by generated transforms.

pub mod records {
    // chrono imports come from the generated file header
    include!(concat!(env!("OUT_DIR"), "/records.rs"));
}

pub mod envelopes {
    // super::records::* comes from the generated file header
    use super::records::*;
    include!(concat!(env!("OUT_DIR"), "/envelopes.rs"));
}

pub mod upserts {
    use super::records::*;
    include!(concat!(env!("OUT_DIR"), "/upserts.rs"));
}

pub mod transforms {
    use super::records::*;
    include!(concat!(env!("OUT_DIR"), "/transforms.rs"));
}

/// Rule helpers called by generated transform code.
/// One function per rule keyword defined in build.rs.
/// Add here only when introducing a new rule keyword.
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
