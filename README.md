# sync-engine

A config-driven, code-generation-backed data sync framework for Rust.

Business teams declare **what** to sync in TOML files. The engine handles **how**.

---

## Architecture overview

The workspace contains two crates and is driven by three config files.

```
workspace/
├── sync-engine/   ← reusable library: traits, generic components, runtime registry
└── user-sync/     ← business crate: schema.toml, config.toml, build.rs, main.rs
```

### Three-layer config system

Each layer has a distinct owner and change cadence.

![](docs/three-layer-config-system.svg)

### Source decomposition

A `Source` is assembled from three independent primitives. Each is registered by a string key and can be swapped in `component.toml` without any code change.

![](docs/source-decomposition.svg)

### Build-time code generation

`schema.toml` is the single source of truth for all domain types. `build.rs` generates Rust from it at compile time — no hand-written structs or mapping logic.

![](docs/build-time-codegen.svg)

---

## Project layout

```
workspace/
├── Cargo.toml                           # workspace root
│
├── sync-engine/                         # reusable library crate (no domain types)
│   └── src/
│       ├── lib.rs                       # public re-exports
│       ├── pipeline/
│       │   ├── mod.rs                   # Source, Transform, Sink traits
│       │   ├── primitives.rs            # Chunker, Auth, Fetcher, FetchParams
│       │   ├── registry.rs              # Registry  +  run_from_toml()
│       │   ├── component_registry.rs    # PrimitiveRegistry, erased trait objects
│       │   ├── adapters.rs              # SourceAdapter, TransformAdapter, SinkAdapter
│       │   └── composed_source.rs       # ComposedSource<C, A, F>
│       └── components/
│           ├── auth.rs                  # OAuth2Auth
│           ├── chunker.rs               # DateWindowChunker
│           ├── fetcher.rs               # HttpJsonFetcher<T>
│           └── writer.rs                # PostgresWriter<T>, WriterAdapter, RawSqlHook
│
└── user-sync/                           # business crate (bin)
    ├── schema.toml                      # ← edit to change domain model
    ├── component.toml                   # ← edit to rewire primitives
    ├── pipeline.toml                    # ← edit to change schedule / topology
    ├── config.toml                      # ← edit runtime values (overlaid by env vars)
    ├── build.rs                         # compile-time codegen from schema.toml
    └── src/
        ├── config.rs                    # AppConfig: loads config.toml + env overlay
        ├── main.rs                      # register primitives, run
        └── generated/
            └── mod.rs                   # include! glue + epoch rule helpers
```

---

## Getting started

### Prerequisites

- Rust 1.75+
- PostgreSQL (or any compatible server)

### 1. Configure

```bash
cd user-sync
cp config.toml config.local.toml   # optional: keep secrets out of version control
```

Edit `config.toml` with your connection details:

```toml
[auth]
token_url     = "https://your-auth-server/oauth2/token"
client_id     = "your-client-id"
client_secret = "your-client-secret"

[sink]
database_url = "postgres://user:pass@localhost:5432/mydb"

[source]
user_endpoint = "https://your-user-api/api/users"
```

Environment variables override any config.toml value using `__` as the section separator:

```bash
export AUTH__CLIENT_SECRET=prod-secret
export SINK__DATABASE_URL=postgres://...
```

### 2. Create the database table

```sql
CREATE TABLE IF NOT EXISTS global_users (
    pccuid         BIGINT        NOT NULL,
    id             BIGINT        NOT NULL,
    sso_acct       VARCHAR(50)   NOT NULL,
    fact_no        VARCHAR(10),
    local_fact_no  VARCHAR(10),
    chinese_nm     VARCHAR(300),
    local_pnl_nm   VARCHAR(300),
    english_nm     VARCHAR(200),
    contact_mail   VARCHAR(200)  NOT NULL DEFAULT '',
    sex            VARCHAR(1),
    lo_posi_nm     VARCHAR(60),
    disabled       VARCHAR(1)    NOT NULL,
    disabled_date  TIMESTAMPTZ,
    update_date    TIMESTAMPTZ   NOT NULL,
    lo_dept_nm     VARCHAR(60),
    tel            VARCHAR(100)  NOT NULL DEFAULT '',
    leave_mk       VARCHAR(20),
    acct_type      VARCHAR(1),
    CONSTRAINT global_users_pkey PRIMARY KEY (pccuid)
);
```

### 3. Build and run

```bash
cargo build
cargo run --bin user-sync
```

---

## Configuration reference

### `config.toml`

| Section | Key | Default | Description |
|---|---|---|---|
| `[scheduler]` | `cron` | `"0 0 2 * * *"` | 6-field cron (seconds first). Runs daily at 02:00. |
| `[source]` | `user_endpoint` | — | Base URL of the user API. |
| `[source]` | `start_interval` | `30` | Fetch users updated up to N days ago (outer window). |
| `[source]` | `end_interval` | `0` | Stop at N days ago (0 = today). |
| `[source]` | `interval_limit` | `7` | Max day-range per API call. |
| `[source]` | `include_realm_types` | `""` | Comma-separated realm filter. Empty = all. |
| `[auth]` | `token_url` | — | OAuth2 token endpoint URL. |
| `[auth]` | `client_id` | — | OAuth2 client identifier. |
| `[auth]` | `client_secret` | — | OAuth2 client secret. |
| `[sink]` | `database_url` | — | PostgreSQL connection string. |
| `[sink]` | `sync_sql` | `""` | Optional SQL run after each full cycle (e.g. `REFRESH MATERIALIZED VIEW …`). |
| `[log]` | `rust_log` | `"user_sync=info"` | `tracing` filter string. |

### `component.toml`

Wires named components from registered primitives. No recompile — just restart.

```toml
[source.user-api]
chunker = "date-window"   # registered in main.rs
auth    = "oauth2"
fetcher = "http-json"

[transform.user]
mapper = "user-fields"

[sink.postgres]
writer    = "pg-upsert"
post_hook = "raw-sql"
```

### `pipeline.toml`

Declares which components run on which cron. Add a block for a second pipeline — no code change.

```toml
[[pipeline]]
name      = "global-user-sync"
source    = "user-api"
transform = "user"
sink      = "postgres"
cron      = "0 0 2 * * *"
```

### `schema.toml`

Declares the API record shape, DB target shape, and field mapping rules. `build.rs` generates all Rust structs and trait implementations from this file at compile time.

**Available mapping rules:**

| Rule | Input type | Output type | Effect |
|---|---|---|---|
| `copy` | any | same | Direct field assignment |
| `null_to_empty` | `Option<String>` | `String` | `None` → `""` |
| `bool_to_yn` | `bool` | `String` | `true` → `"Y"`, `false` → `"N"` |
| `epoch_ms_to_ts` | `i64` / `Option<i64>` | `DateTime<Utc>` / `Option<DateTime<Utc>>` | Epoch milliseconds → timestamp |
| `to_string` | any `Display` | `String` | `.to_string()` |

To add a field to the sync: add one `[[record.ApiUser.fields]]` entry, one `[[record.DbUser.fields]]` entry, and one `[[mapping.user.rules]]` entry. Run `cargo build`. Done.

---

## Adding a second sync job

Create a new workspace member:

```
partner-sync/
├── schema.toml          # partner API shape + mapping
├── component.toml       # which primitives to use
├── pipeline.toml        # schedule
├── config.toml          # connection values
├── build.rs             # same codegen script
└── src/
    ├── config.rs
    ├── main.rs
    └── generated/mod.rs
```

Add to `workspace/Cargo.toml`:

```toml
[workspace]
members = ["sync-engine", "user-sync", "partner-sync"]
```

`sync-engine` is shared unchanged.

---

## How the sync cycle works

```
cron tick
  │
  ├─ DateWindowChunker.next_window()  →  { start_time, end_time }
  │     sleeps 60 s between windows
  │
  ├─ OAuth2Auth.get_token()           →  bearer token
  │     refreshes 60 s before expiry
  │     invalidates on HTTP 401
  │
  ├─ HttpJsonFetcher.fetch()          →  Vec<ApiUser>
  │     retries 5× with exponential back-off (2 s → 30 s)
  │
  ├─ UserTransform.apply()            →  Vec<DbUser>  (generated)
  │
  └─ PostgresWriter.write()
        INSERT … ON CONFLICT (pccuid) DO UPDATE SET …  (generated)
        per-row errors logged and skipped, batch continues
        on_complete() → optional post-sync SQL
```

## Logging

Set `[log] rust_log` in `config.toml` or the `LOG__RUST_LOG` env var.

| Level | Emitted when |
|---|---|
| `INFO` | Cycle start/end, each window, token acquired, batch size |
| `DEBUG` | Per-row upsert |
| `WARN` | Token refresh, 401 invalidation, transform skip, post-sync failure |
| `ERROR` | All retries exhausted, upsert failure |
