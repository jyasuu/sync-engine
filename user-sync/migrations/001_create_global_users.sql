-- user-sync/migrations/001_create_global_users.sql
-- Mounted into docker-entrypoint-initdb.d so it runs on first `docker compose up`.

CREATE TABLE IF NOT EXISTS global_users (
    id             TEXT        NOT NULL,
    pccuid         TEXT        NOT NULL,
    sso_acct       TEXT        NOT NULL DEFAULT '',
    fact_no        TEXT        NOT NULL DEFAULT '',
    local_fact_no  TEXT        NOT NULL DEFAULT '',
    chinese_nm     TEXT        NOT NULL DEFAULT '',
    local_pnl_nm   TEXT        NOT NULL DEFAULT '',
    english_nm     TEXT        NOT NULL DEFAULT '',
    contact_mail   TEXT        NOT NULL DEFAULT '',
    sex            TEXT        NOT NULL DEFAULT '',
    lo_posi_nm     TEXT        NOT NULL DEFAULT '',
    disabled       TEXT        NOT NULL DEFAULT '',
    disabled_date  TIMESTAMPTZ,
    update_date    TIMESTAMPTZ NOT NULL DEFAULT now(),
    lo_dept_nm     TEXT        NOT NULL DEFAULT '',
    tel            TEXT        NOT NULL DEFAULT '',
    leave_mk       TEXT        NOT NULL DEFAULT '',
    acct_type      TEXT        NOT NULL DEFAULT '',

    CONSTRAINT global_users_pkey PRIMARY KEY (pccuid)
);

CREATE INDEX IF NOT EXISTS idx_global_users_sso_acct ON global_users (sso_acct);
CREATE INDEX IF NOT EXISTS idx_global_users_update_date ON global_users (update_date);
