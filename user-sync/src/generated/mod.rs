// user-sync/src/generated/mod.rs
//
// Uses sync_engine::include_schema! — identical to tonic::include_proto!
// Each call expands to: include!(concat!(env!("OUT_DIR"), "/{name}.rs"))

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

impl records::DbUser {
    pub async fn upsert_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> anyhow::Result<()> {
        sqlx::query(
            "INSERT INTO global_users (
                pccuid, sso_acct, fact_no, local_fact_no, chinese_nm, local_pnl_nm,
                english_nm, contact_mail, sex, lo_posi_nm, disabled, disabled_date,
                update_date, lo_dept_nm, tel, leave_mk, acct_type, id
             ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$1)
             ON CONFLICT (pccuid) DO UPDATE SET
                sso_acct=EXCLUDED.sso_acct, fact_no=EXCLUDED.fact_no,
                local_fact_no=EXCLUDED.local_fact_no, chinese_nm=EXCLUDED.chinese_nm,
                local_pnl_nm=EXCLUDED.local_pnl_nm, english_nm=EXCLUDED.english_nm,
                contact_mail=EXCLUDED.contact_mail, sex=EXCLUDED.sex,
                lo_posi_nm=EXCLUDED.lo_posi_nm, disabled=EXCLUDED.disabled,
                disabled_date=EXCLUDED.disabled_date, update_date=EXCLUDED.update_date,
                lo_dept_nm=EXCLUDED.lo_dept_nm, tel=EXCLUDED.tel,
                leave_mk=EXCLUDED.leave_mk, acct_type=EXCLUDED.acct_type",
        )
        .bind(&self.pccuid)
        .bind(&self.sso_acct)
        .bind(&self.fact_no)
        .bind(&self.local_fact_no)
        .bind(&self.chinese_nm)
        .bind(&self.local_pnl_nm)
        .bind(&self.english_nm)
        .bind(&self.contact_mail)
        .bind(&self.sex)
        .bind(&self.lo_posi_nm)
        .bind(&self.disabled)
        .bind(&self.disabled_date)
        .bind(&self.update_date)
        .bind(&self.lo_dept_nm)
        .bind(&self.tel)
        .bind(&self.leave_mk)
        .bind(&self.acct_type)
        .execute(&mut **tx)
        .await
        .map(|_| ())
        .map_err(Into::into)
    }
}
