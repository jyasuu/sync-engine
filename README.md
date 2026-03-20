# sync-engine workspace

A zero-boilerplate data sync framework for Rust.  
Business crates write **TOML files** — the engine handles auth, pagination, retry, transactions, and scheduling.

```
sync-engine/          # reusable library crate
user-sync/            # example business crate (user directory sync)
Cargo.toml            # workspace root
docker-compose.yml    # local Postgres for development
```

---

## Quick start

```bash
# 1. Start local Postgres
docker compose up -d

# 2. Copy and fill in env vars
cp user-sync/.env.example user-sync/.env
$EDITOR user-sync/.env

# 3. Run
cd user-sync
cargo run
```

---

## How a new sync job works

For a brand-new sync job you write **two TOML files** and **one Rust file**:

| File | Purpose |
|------|---------|
| `schema.toml` | API record shape, DB target shape, field mapping rules |
| `pipeline.toml` | Connections, window iterator, retry, post-job steps, cron |
| `src/job.rs` | `PostJob` impl only — custom summary / post-SQL logic |

Everything else — structs, SQL, transforms, auth, retry loop, scheduler — is generated or provided by the engine.

---

## Workspace crates

### `sync-engine`

The reusable library. Key public surface:

| Item | Description |
|------|-------------|
| `codegen::generate(path)` | Call from `build.rs`; reads `schema.toml`, emits 4 `.rs` files |
| `codegen::generate_config_doc(…)` | Emits `CONFIG.md` from annotated `config.toml` |
| `StandardJob<Env, DbM, Xfm>` | Generic `MainJob`: fetch → transform → tx-upsert loop |
| `StandardConnections` | Builds `PgPool + OAuth2Auth + reqwest::Client` from `pipeline.toml` env vars |
| `run_from_pipeline_toml(path, factory)` | Reads `pipeline.toml`, wires cron + mutex-skip, runs the job |
| `TxWriter<T>` | Opens a Postgres transaction, upserts a batch, commits |
| `OAuth2Auth` | Token caching with auto-refresh and invalidation |
| `DateWindowChunker` | Emits `(start_time, end_time)` date-range windows |
| `HttpJsonFetcher<T>` | HTTP GET with bearer auth, deserialises into `HasEnvelope` |

#### Generated code (per `schema.toml`)

| File | Content |
|------|---------|
| `records.rs` | `ApiUser`, `DbUser` structs with serde derives |
| `envelopes.rs` | `ApiUserResponse` + `HasEnvelope` impl |
| `upserts.rs` | `Upsertable` (pool) **and** `UpsertableInTx` (transaction) impls |
| `transforms.rs` | `UserTransform` struct + `Transform` impl |

#### Supported mapping rules

| Rule | Input | Output | Effect |
|------|-------|--------|--------|
| `copy` | any | same | Direct field copy |
| `null_to_empty` | `Option<String>` | `String` | `None` → `""` |
| `bool_to_yn` | `bool` | `String` | `true` → `"Y"` |
| `option_bool_to_yn` | `Option<bool>` | `String` | `Some(true)` → `"Y"`, `None` → `""` |
| `epoch_ms_to_ts` | `i64` / `Option<i64>` | `DateTime<Utc>` | Epoch ms → timestamp |
| `to_string` | any `Display` | `String` | `.to_string()` |

Each rule entry in `schema.toml` supports an optional `source` key for when the API field name differs from the DB field name (e.g. camelCase API → snake_case DB):

```toml
[[mapping.user.rules]]
field  = "sso_acct"    # DbUser target field (snake_case)
source = "ssoAcct"     # ApiUser source field (camelCase) — omit if identical
rule   = "copy"
```

---

### `user-sync`

Concrete sync job: pulls users from an HTTP API and upserts them into Postgres.

```
user-sync/
├── src/
│   ├── main.rs           # scheduler wiring (~40 lines)
│   ├── config.rs         # AppConfig (config crate + env vars)
│   ├── job.rs            # PreJob + MainJob (delegates) + PostJob
│   └── generated/
│       └── mod.rs        # include_schema! — no hand-written code
├── schema.toml           # record shapes + mapping rules
├── config.toml           # default runtime config + __desc annotations
├── pipeline.toml         # fully declarative pipeline config
├── .env.example          # env var template
└── migrations/
    └── 001_create_global_users.sql
```

#### Configuration

All values in `config.toml` can be overridden with environment variables using `__` as the section separator:

```bash
AUTH__CLIENT_SECRET=prod-secret          # overrides [auth] client_secret
SOURCE__USER_ENDPOINT=https://api/users  # overrides [source] user_endpoint
SINK__DATABASE_URL=postgres://...        # overrides [sink] database_url
```

A generated `CONFIG.md` (via `build.rs`) documents every key with defaults, descriptions, and env-var names.

---

## Pipeline lifecycle

```
pre_job
  └─ build PgPool + OAuth2Auth + reqwest::Client

main_job
  └─ DateWindowIter (start → end in interval_limit steps)
       └─ per window (with retry + backoff)
            ├─ get_token()          OAuth2, cached, auto-refresh
            ├─ HTTP GET             bearer auth + date params
            ├─ parse envelope       serde_json → ApiUser[]
            ├─ transform            ApiUser → DbUser (generated)
            ├─ BEGIN transaction
            ├─ upsert each row      generated INSERT … ON CONFLICT
            └─ COMMIT
       └─ sleep between windows

post_job
  ├─ log_summary            windows / fetched / upserted / skipped / errors
  └─ raw_sql (optional)     arbitrary SQL after job completes
```

---

## Adding a new sync job

1. Create a new crate under the workspace, e.g. `order-sync/`
2. Copy `user-sync/Cargo.toml` and update the name
3. Copy `user-sync/build.rs` unchanged
4. Write `schema.toml` for your API + DB shapes
5. Write `pipeline.toml` pointing at your env vars
6. Write `src/job.rs` — only `PostJob` needs custom logic
7. Copy `src/main.rs` and `src/config.rs` — minimal changes needed
8. Add `src/generated/mod.rs` with the four `include_schema!` calls
9. Add `"order-sync"` to the workspace `members` in root `Cargo.toml`

---

## Development tips

```bash
# Watch logs at debug level for a single run
RUST_LOG=user_sync=debug,sync_engine=debug cargo run

# Check generated code
cargo build 2>&1 | grep "warning=Generated"
cat target/debug/build/user-sync-*/out/records.rs

# Run with a .env file
cd user-sync && cargo run   # dotenvy picks up .env automatically
```
