# sync-engine / user-sync

A scheduled data sync service that pulls users from an HTTP API and upserts them into PostgreSQL. Built on a Talend-style **pre / main / post job** model — connections created once, pipeline steps explicit, configuration declared in TOML.

---

## How it works

Every cron tick runs three phases in sequence:

```
pre_job   → create PgPool, OAuth2Client, UserServiceClient
main_job  → iterate date windows → retry(fetch → transform → begin tx → upsert → commit)
post_job  → log summary, run optional post-sync SQL
```

The pipeline inside `main_job` looks like this:

```
DateWindowIter(start=30d, end=0d, step=7d)
  └─ for each window, sleep window_sleep_secs between iterations
      └─ with_retry(5×, exponential 2s→30s)
          ├─ OAuth2Client.get_token()         bearer token (cached, refreshes near expiry)
          ├─ GET /api/users?start=...&end=... on 401 → invalidate token, retry
          ├─ parse ApiUserResponse JSON
          ├─ UserTransform: ApiUser → DbUser  (generated from schema.toml)
          ├─ PgPool.begin()
          ├─ DbUser.upsert_in_tx() × N        INSERT … ON CONFLICT DO UPDATE
          └─ tx.commit()
```

---

## Workspace layout

```
workspace/
├── sync-engine/              reusable library — traits, job phases, generic components
│   └── src/
│       ├── job.rs            PreJob / MainJob / PostJob traits, DateWindowIter, with_retry
│       ├── codegen.rs        build-time code generator (called from user-sync/build.rs)
│       ├── components/
│       │   ├── auth.rs       OAuth2Client
│       │   ├── chunker.rs    DateWindowChunker
│       │   ├── fetcher.rs    HttpJsonFetcher<T>
│       │   └── writer.rs     PostgresWriter<T>, Upsertable trait
│       └── pipeline/         registry, adapters (legacy, kept for future use)
│
└── user-sync/                business crate — everything domain-specific
    ├── schema.toml           API shape, DB shape, field mapping rules
    ├── config.toml           runtime values (URLs, credentials, intervals)
    ├── build.rs              calls sync_engine::codegen::generate()
    └── src/
        ├── config.rs         AppConfig — typed, loaded from config.toml + env overlay
        ├── connections.rs    JobConnections — PgPool + OAuth2Client + UserServiceClient
        ├── job.rs            UserSyncJob — PreJob / MainJob / PostJob implementations
        ├── main.rs           cron scheduler, mutex guard, calls run_job
        └── generated/
            └── mod.rs        include_schema! calls + upsert_in_tx helper + rule helpers
```

---

## Quick start

**Prerequisites:** Rust 1.75+, PostgreSQL

### 1. Fill in config.toml

```toml
[auth]
token_url     = "https://your-auth-server/oauth2/token"
client_id     = "your-client-id"
client_secret = ""                   # set via AUTH__CLIENT_SECRET env var

[source]
user_endpoint = "https://your-user-api/api/users"

[sink]
database_url  = ""                   # set via SINK__DATABASE_URL env var
```

Secrets should be provided as environment variables — every config key can be
overridden with `SECTION__KEY`:

```bash
export AUTH__CLIENT_SECRET=prod-secret
export SINK__DATABASE_URL=postgres://user:pass@host:5432/db
```

### 2. Create the database table

```sql
CREATE TABLE IF NOT EXISTS global_users (
    pccuid        BIGINT       NOT NULL,
    id            BIGINT       NOT NULL,
    sso_acct      VARCHAR(50)  NOT NULL,
    fact_no       VARCHAR(10),
    local_fact_no VARCHAR(10),
    chinese_nm    VARCHAR(300),
    local_pnl_nm  VARCHAR(300),
    english_nm    VARCHAR(200),
    contact_mail  VARCHAR(200) NOT NULL DEFAULT '',
    sex           VARCHAR(1),
    lo_posi_nm    VARCHAR(60),
    disabled      VARCHAR(1)   NOT NULL,
    disabled_date TIMESTAMPTZ,
    update_date   TIMESTAMPTZ  NOT NULL,
    lo_dept_nm    VARCHAR(60),
    tel           VARCHAR(100) NOT NULL DEFAULT '',
    leave_mk      VARCHAR(20),
    acct_type     VARCHAR(1),
    CONSTRAINT global_users_pkey PRIMARY KEY (pccuid)
);
```

### 3. Build and run

```bash
cargo build
cargo run --bin user-sync
```

---

## Configuration

All configuration lives in `user-sync/config.toml`. Every key can be overridden
by an environment variable using `SECTION__KEY` format.

### `[scheduler]`

| Key | Default | Description |
|-----|---------|-------------|
| `cron` | `0 0 2 * * *` | 6-field cron (seconds field first). Daily at 02:00. Use `*/5 * * * * *` for every 5 s during testing. |

### `[source]`

| Key | Default | Description |
|-----|---------|-------------|
| `user_endpoint` | — | Base URL of the user API |
| `start_interval` | `30` | Fetch users updated up to this many days ago (outer window boundary) |
| `end_interval` | `0` | Inner boundary in days ago. `0` = today |
| `interval_limit` | `7` | Maximum day-range per single API call |
| `window_sleep_secs` | `60` | Pause between windows to avoid overloading the upstream API. Set to `0` for testing |
| `include_realm_types` | `""` | Comma-separated realm filter. Empty = all realms |

### `[auth]`

| Key | Default | Description |
|-----|---------|-------------|
| `token_url` | — | OAuth2 token endpoint (`client_credentials` grant) |
| `client_id` | — | OAuth2 client identifier |
| `client_secret` | — | Use `AUTH__CLIENT_SECRET` env var — never commit this value |

### `[sink]`

| Key | Default | Description |
|-----|---------|-------------|
| `database_url` | — | PostgreSQL connection string. Use `SINK__DATABASE_URL` env var in production |
| `sync_sql` | `""` | Optional SQL executed once after every completed cycle, e.g. `REFRESH MATERIALIZED VIEW mv_active_users;` |

### `[log]`

| Key | Default | Description |
|-----|---------|-------------|
| `rust_log` | `user_sync=info` | `tracing` filter. Use `user_sync=debug` to see per-step HTTP and database logs |

---

## Schema

`schema.toml` is the single source of truth for the API contract and DB shape.
`build.rs` reads it at compile time and generates four Rust source files:

| Generated file | Contents |
|----------------|----------|
| `records.rs` | `ApiUser` and `DbUser` structs with serde derives |
| `envelopes.rs` | `ApiUserResponse` + `impl HasEnvelope` |
| `upserts.rs` | `impl Upsertable for DbUser` with the full upsert SQL |
| `transforms.rs` | `UserTransform` + `impl Transform<Input=ApiUser, Output=DbUser>` |

**To add a field to the sync:**

1. Add `[[record.ApiUser.fields]]` entry with the API field name and type
2. Add `[[record.DbUser.fields]]` entry with the DB column name and type
3. Add `[[mapping.user.rules]]` entry with the conversion rule
4. `cargo build` — done, no Rust to write

**Available mapping rules:**

| Rule | Input | Output | When to use |
|------|-------|--------|-------------|
| `copy` | any | same type | Types already match |
| `null_to_empty` | `Option<String>` | `String` | DB column is NOT NULL |
| `bool_to_yn` | `bool` | `String` | DB stores `"Y"`/`"N"` |
| `epoch_ms_to_ts` | `i64` / `Option<i64>` | `DateTime<Utc>` | API sends Unix ms timestamps |
| `to_string` | any `Display` | `String` | Numeric code stored as string |

**To add envelope metadata fields** (fields present in the JSON response wrapper
but not in the `data` array):

```toml
[record.ApiUser.fetcher]
envelope_field = "data"

[[record.ApiUser.fetcher.envelope_meta]]
name = "timestamp"
type = "i64"
```

---

## Logging reference

| Level | Emitted when |
|-------|-------------|
| `INFO` | Job starts/completes, each window opens and commits, token acquired, fetch byte count, upsert totals |
| `DEBUG` | HTTP request URL, response status, body byte count, transform count, tx open/commit |
| `WARN` | Token refresh, 401 invalidation, transform skipped, previous job tick skipped |
| `ERROR` | All retries exhausted, upsert failure, post-sync SQL failure |

Set `[log] rust_log = "user_sync=debug"` for verbose output or `user_sync=warn` for quiet production logs.

---

## Extending

### Add a field to the sync

Edit `schema.toml` only — no Rust changes needed. See the Schema section above.

### Change the sync schedule

Edit `[scheduler] cron` in `config.toml` and restart. No rebuild needed.

### Add a post-sync step

Set `[sink] sync_sql` in `config.toml`:

```toml
[sink]
sync_sql = "REFRESH MATERIALIZED VIEW mv_active_users;"
```

### Add a second sync job for a different API

Create a new workspace member crate with its own `schema.toml`, `config.toml`,
and job phase implementations. Add it to `workspace/Cargo.toml`:

```toml
[workspace]
members = ["sync-engine", "user-sync", "partner-sync"]
```

`sync-engine` is shared and unchanged — only `schema.toml`, `config.toml`, and
the three job phase implementations differ between integrations.

### Change retry behaviour

`with_retry(5, ...)` in `user-sync/src/job.rs` — change the first argument to
adjust max attempts. Back-off starts at 2 s and doubles each attempt, capped at 30 s.

### Change the inter-window pause

`[source] window_sleep_secs` in `config.toml`. Set to `0` for testing, raise
it if the upstream API has rate limits.
