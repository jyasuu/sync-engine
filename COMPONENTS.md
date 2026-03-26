# Component guide

> This document describes every component in the `user-sync` workspace —
> **what** it is, **why** it exists, **when** it runs, **who** owns it,
> **where** it lives, and **how** to extend or replace it.

---

## Workspace layout

```
workspace/
├── sync-engine/          reusable library — no domain types, no env vars
└── user-sync/            business crate  — schema, config, job phases
```

The boundary is strict: `sync-engine` knows nothing about users, postgres
tables, or OAuth2 endpoints. `user-sync` contains everything that is specific
to this integration.

---

## sync-engine components

### PreJob / MainJob / PostJob  _(traits)_

| | |
|---|---|
| **What** | Three traits that define the lifecycle of one sync execution |
| **Why** | Mirrors Talend Studio's pre/main/post job model — connections are created once, passed explicitly, cleaned up after |
| **When** | `PreJob` runs first, `MainJob` receives its output, `PostJob` always runs even if `MainJob` fails |
| **Who** | Defined in `sync-engine`; implemented in `user-sync/src/job.rs` |
| **Where** | `sync-engine/src/job.rs` |
| **How to extend** | Implement all three traits on a unit struct, then call `run_job::<YourJob, YourConnections, YourConfig>(cfg)` |

```rust
// Minimal new job
struct MyJob;
impl PreJob  for MyJob { type Cx = MyCx; type Cfg = MyCfg; async fn run(cfg) -> MyCx }
impl MainJob for MyJob { async fn run(cx, cfg) -> JobSummary }
impl PostJob for MyJob { async fn run(summary, cx, cfg) }
```

---

### run_job  _(function)_

| | |
|---|---|
| **What** | Orchestrates `pre → main → post` in order |
| **Why** | Guarantees `PostJob` runs even when `MainJob` returns an error, so connections are always logged and cleaned up |
| **When** | Called once per cron tick from `main.rs` |
| **Who** | `sync-engine`; never modified by business code |
| **Where** | `sync-engine/src/job.rs` |
| **How to extend** | No extension needed — swap the job type parameter |

---

### DateWindowIter  _(struct)_

| | |
|---|---|
| **What** | Async iterator that yields `(start_days_ago, end_days_ago)` windows |
| **Why** | Breaks a large date range into smaller chunks to avoid overloading the upstream API |
| **When** | Created at the start of `MainJob::run`, advances once per chunk |
| **Who** | `sync-engine`; configured by `[source]` values in `config.toml` |
| **Where** | `sync-engine/src/job.rs` |
| **How to extend** | Set `iter.sleep` after construction, or call `.next_window_with_sleep(false)` to skip the inter-window pause |

**Config keys that control it:**

| Key | Effect |
|-----|--------|
| `source.start_interval` | How many days back to start (outer boundary) |
| `source.end_interval` | How many days back to stop (0 = today) |
| `source.interval_limit` | Maximum day-range per API call |
| `source.window_sleep_secs` | Pause between windows (set to 0 for testing) |

---

### with_retry  _(function)_

| | |
|---|---|
| **What** | Exponential back-off retry wrapper — calls a closure up to N times |
| **Why** | Transient network errors and token expiry should not abort a window; the retry loop handles them transparently |
| **When** | Wraps the entire fetch → transform → upsert → commit pipeline inside `MainJob` |
| **Who** | `sync-engine`; used directly in `user-sync/src/job.rs` |
| **Where** | `sync-engine/src/job.rs` |
| **How to extend** | Change the first argument (`max_attempts`); back-off is hardcoded at 2 s → 4 s → 8 s … capped at 30 s |

---

### OAuth2Client  _(struct)_

| | |
|---|---|
| **What** | Caches a client-credentials access token and refreshes it when it expires |
| **Why** | Token acquisition is expensive; caching avoids a round-trip on every window |
| **When** | Created in `PreJob`; `get_token()` called at the start of every retry attempt |
| **Who** | `user-sync/src/connections.rs` |
| **Where** | `user-sync/src/connections.rs` |
| **How to extend** | Replace with `ApiKeyAuth` or any struct that exposes `get_token() → String` and `invalidate()` |

**Config keys:**

| Key | Description |
|-----|-------------|
| `auth.token_url` | OAuth2 token endpoint |
| `auth.client_id` | Client identifier |
| `auth.client_secret` | Client secret — use `AUTH__CLIENT_SECRET` env var in production |

**Token lifecycle:**

```
first call          → fetch from token_url, cache result
subsequent calls    → return cached token
60 s before expiry  → automatically refresh
HTTP 401 received   → invalidate(), next retry fetches fresh token
```

---

### UserServiceClient  _(struct)_

| | |
|---|---|
| **What** | Holds the HTTP client and endpoint URL for the user API |
| **Why** | Separates connection configuration from the fetch logic in `MainJob` |
| **When** | Created in `PreJob`; `http` and `endpoint` cloned into each retry closure |
| **Who** | `user-sync/src/connections.rs` |
| **Where** | `user-sync/src/connections.rs` |
| **How to extend** | Add headers, mTLS config, or proxy settings to the `reqwest::Client` builder here |

**Config keys:**

| Key | Description |
|-----|-------------|
| `source.user_endpoint` | Base URL of the user API |
| `source.include_realm_types` | Comma-separated realm filter; empty = all |

---

### JobConnections  _(struct)_

| | |
|---|---|
| **What** | Bundle of all live connections — `PgPool`, `OAuth2Client`, `UserServiceClient` |
| **Why** | Connections are expensive to create; making them explicit and first-class prevents hidden re-creation inside the pipeline |
| **When** | Created once in `PreJob`, passed by reference to `MainJob` and `PostJob` |
| **Who** | `user-sync/src/connections.rs` |
| **Where** | `user-sync/src/connections.rs` |
| **How to extend** | Add more connection fields (e.g. Redis cache, S3 client) — they will be available in all three job phases |

---

## user-sync components

### UserSyncJob  _(struct)_

| | |
|---|---|
| **What** | Unit struct that ties together the three job phase implementations |
| **Why** | Provides a single type to pass to `run_job` without coupling the engine to business logic |
| **When** | Referenced only in `main.rs` as a type parameter |
| **Who** | `user-sync/src/job.rs` |
| **Where** | `user-sync/src/job.rs` |
| **How to extend** | Add a second job struct for a different API; register it in a second `tokio-cron-scheduler` job |

---

### UserTransform  _(generated struct)_

| | |
|---|---|
| **What** | Pure function mapping `ApiUser → DbUser` — no I/O, no side effects |
| **Why** | Separates data shape conversion from fetch and persistence logic |
| **When** | Applied once per API record inside the retry closure, after the HTTP response is parsed |
| **Who** | Generated by `build.rs` from `[mapping.user]` in `schema.toml` |
| **Where** | `target/.../out/transforms.rs` (generated), declared in `schema.toml` |
| **How to extend** | Add a `[[mapping.user.rules]]` entry in `schema.toml` — no Rust to write |

**Available mapping rules:**

| Rule | Input | Output | When to use |
|------|-------|--------|-------------|
| `copy` | any | same type | Field names differ or types match exactly |
| `null_to_empty` | `Option<String>` | `String` | DB column is NOT NULL but API may omit the field |
| `bool_to_yn` | `bool` | `String` | DB stores `"Y"`/`"N"` flags |
| `epoch_ms_to_ts` | `i64` / `Option<i64>` | `DateTime<Utc>` | API returns Unix epoch milliseconds |
| `to_string` | any `Display` | `String` | Numeric codes stored as strings in DB |

---

### ApiUserResponse  _(generated struct)_

| | |
|---|---|
| **What** | Deserialisation target for the HTTP response envelope |
| **Why** | Decouples the API wire format from the domain model; the engine never sees raw JSON |
| **When** | Instantiated by `serde_json::from_slice` inside the retry closure |
| **Who** | Generated by `build.rs` from `[record.ApiUser.fetcher]` in `schema.toml` |
| **Where** | `target/.../out/envelopes.rs` (generated), declared in `schema.toml` |
| **How to extend** | Add envelope metadata fields under `[[record.ApiUser.fetcher.envelope_meta]]` in `schema.toml` |

---

### PostgresWriter / upsert_in_tx  _(generated + generic)_

| | |
|---|---|
| **What** | Executes `INSERT … ON CONFLICT DO UPDATE` inside a caller-supplied transaction |
| **Why** | The transaction boundary is owned by `MainJob`, not the writer — this matches the Talend model where tx begin/commit are explicit steps |
| **When** | Called per row inside the retry closure, between `db.begin()` and `tx.commit()` |
| **Who** | SQL generated by `build.rs` from `[record.DbUser.sink]` in `schema.toml`; `upsert_in_tx` hand-written in `generated/mod.rs` |
| **Where** | `schema.toml` (SQL shape), `user-sync/src/generated/mod.rs` (tx helper) |
| **How to extend** | Change `schema.toml` to add/remove columns — the SQL regenerates on next build |

---

### AppConfig  _(struct)_

| | |
|---|---|
| **What** | Typed configuration loaded from `config.toml` with environment variable overlay |
| **Why** | Single source of truth — no `std::env::var` scattered through business code |
| **When** | Loaded once at startup in `main.rs`, passed as `&AppConfig` to all three job phases |
| **Who** | `user-sync/src/config.rs` |
| **Where** | `user-sync/src/config.rs` + `user-sync/config.toml` |
| **How to extend** | Add a field to the appropriate section struct in `config.rs` and a matching key in `config.toml` |

**Override precedence:** `config.toml` < environment variable  
**Env var format:** `SECTION__KEY` (double underscore)  
**Examples:** `AUTH__CLIENT_SECRET=x`, `SOURCE__WINDOW_SLEEP_SECS=0`, `SINK__DATABASE_URL=postgres://...`

---

### build.rs + codegen  _(build-time pipeline)_

| | |
|---|---|
| **What** | Compile-time code generator — reads `schema.toml`, emits Rust source into `OUT_DIR` |
| **Why** | Domain structs and SQL are declarations, not hand-written code; changing the API contract requires only editing `schema.toml` |
| **When** | Runs before the Rust compiler every time `schema.toml` or `config.toml` changes |
| **Who** | `user-sync/build.rs` calls `sync_engine::codegen::generate()` |
| **Where** | `sync-engine/src/codegen.rs` (the generator), `user-sync/build.rs` (the entry point) |
| **How to extend** | Add a new rule keyword: (1) add a `match` arm in `gen_rule_expr`, (2) add a helper in `generated/rules.rs` |

**What gets generated:**

| File | Contains |
|------|---------|
| `records.rs` | `ApiUser` and `DbUser` structs with serde derives |
| `envelopes.rs` | `ApiUserResponse` + `impl HasEnvelope` |
| `upserts.rs` | `impl Upsertable for DbUser` with runtime SQL |
| `transforms.rs` | `UserTransform` + `impl Transform` |
| `CONFIG.md` | This document (config reference + schema reference) |

---

## Execution flow

```
cron tick
│
├─ skip if previous job still holds the mutex
│
└─ run_job::<UserSyncJob>
   │
   ├─ PreJob::run(cfg)
   │   ├─ PgPool::connect        → cfg.sink.database_url
   │   ├─ OAuth2Client::new      → cfg.auth.*
   │   └─ UserServiceClient::new → cfg.source.user_endpoint
   │
   ├─ MainJob::run(cx, cfg)
   │   └─ DateWindowIter (start_interval → end_interval, step = interval_limit)
   │       └─ per window: sleep window_sleep_secs (skipped on first + after errors)
   │           └─ with_retry(5, exponential 2s→30s)
   │               ├─ OAuth2Client.get_token()        → bearer token
   │               ├─ HTTP GET user_endpoint          → 200 OK / 401 → invalidate + retry
   │               ├─ serde_json::from_slice          → ApiUserResponse
   │               ├─ UserTransform.apply()           → Vec<DbUser>
   │               ├─ PgPool.begin()                  → Transaction
   │               ├─ DbUser.upsert_in_tx() × N       → INSERT ON CONFLICT
   │               └─ Transaction.commit()
   │
   └─ PostJob::run(summary, cx, cfg)
       ├─ log totals (windows, fetched, upserted, skipped, errors)
       └─ optional: sqlx::raw_sql(cfg.sink.sync_sql)
```

---

## Adding a second integration

1. Create a new workspace member: `cargo new --bin partner-sync`
2. Add `sync-engine` to its dependencies and build-dependencies
3. Write `partner-sync/schema.toml` with the new record shapes
4. Write `partner-sync/config.toml` with connection values
5. Implement `PreJob`, `MainJob`, `PostJob` on a new unit struct
6. Call `run_job::<PartnerSyncJob, _, _>(cfg)` from `main.rs`

`sync-engine` is shared and unchanged.
