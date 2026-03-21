# sync-engine workspace

A zero-boilerplate data sync framework for Rust.
Write `schema.toml` + `pipeline.toml` — the engine handles auth, pagination,
retry, transactions, scheduling, and async transport.

```
sync-engine/          reusable library crate
user-sync/            example business crate (user directory sync)
docs/                 tutorial and four-pattern example configs
Cargo.toml            workspace root
docker-compose.yml    local Postgres for development
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
cd user-sync && cargo run
```

---

## What a new sync job requires

| File | Purpose | Hand-written? |
|------|---------|---------------|
| `schema.toml` | API + DB record shapes, field mapping rules | Yes — data model |
| `pipeline.toml` | Resources, slots/queues, steps, scheduler | Yes — wiring only |
| `src/main.rs` | Type registration + `run()` | Yes — ~15 lines |
| `src/generated/mod.rs` | `include_schema!` macros | Copy from template |

Everything else — structs, SQL, auth, retry, transactions, scheduler,
cron, RabbitMQ — is provided by the engine.

`user-sync/src/main.rs` (the only Rust you write for a new job):

```rust
mod generated;
use generated::{envelopes::ApiUserResponse, records::{ApiUser, DbUser}, transforms::UserTransform};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt().with_env_filter(...).init();

    let mut registry = sync_engine::TypeRegistry::new();
    registry.register_envelope::<ApiUserResponse>("ApiUserResponse");
    registry.register_transform::<ApiUser, DbUser, UserTransform>("UserTransform");
    registry.register_model::<DbUser>("DbUser");

    sync_engine::run("pipeline.toml", registry).await
}
```

---

## Four pipeline patterns

All four patterns share the same `main.rs`. Switch between them by editing
`pipeline.toml` only.

| | Case 1 | Case 2 | Case 3 | Case 4 |
|---|---|---|---|---|
| `db_rows` | slot, window | slot, job | tokio queue | rabbitmq queue |
| Sink | tx per window | bulk tx after loop | tokio consumer | amqp consumer |
| Best for | simplicity | fewer commits | throughput | multi-service |

See `docs/pipeline-patterns.md` for full annotated examples of each case.

**Minimal diff between cases** — the only lines that change:

```toml
# 1 → 2: accumulate then bulk tx
[slots.db_rows]  scope = "job"        # was "window"
# move tx_upsert from retry_steps to post_loop_steps, set transform append=true

# 1 → 3: async tokio producer/consumer
[queues.db_rows]  type = "tokio"      # replaces [slots.db_rows]
[[pre_job.steps]]  type = "spawn_consumer"  model = "DbUser"  queue = "db_rows"
# swap tx_upsert → send_to_queue in retry_steps
# add drain_queue to post_job

# 3 → 4: rabbitmq instead of tokio
[queues.db_rows]  type = "rabbitmq"   # was "tokio"
url         = { env = "RABBITMQ_URL" }
exchange    = { env = "RABBITMQ_EXCHANGE" }
routing_key = { env = "RABBITMQ_ROUTING_KEY" }
```

---

## `pipeline.toml` structure

```toml
[job]
name = "my-sync"

[job.scheduler]
cron = { env = "SCHEDULER__CRON", default = "0 */30 * * * *" }

# ── Named connections — built in dependency order ──────────────────────────
[resources.pg]    type = "postgres"     url  = { env = "SINK__DATABASE_URL" }
[resources.auth]  type = "oauth2"       ...
[resources.http]  type = "http_client"  timeout_secs = 620
[resources.svc]   type = "http_service" http = "http"  auth = "auth"
                                        endpoint = { env = "SOURCE__ENDPOINT" }

# ── Typed data channels ────────────────────────────────────────────────────
[slots.api_rows]  type = "ApiUser"   scope = "window"  # or "job" / "pipeline"
[slots.db_rows]   type = "DbUser"    scope = "window"
# [queues.db_rows]  type = "tokio"   capacity = 256
# [queues.db_rows]  type = "rabbitmq"  url = ...  exchange = ...

# ── Phases ─────────────────────────────────────────────────────────────────
[pre_job]
init_resources = true
# [[pre_job.steps]]  type = "spawn_consumer"  model = "DbUser"  queue = "db_rows"

[main_job.iterator]
type = "date_window"  start_interval = ...  end_interval = ...

[main_job.retry]
max_attempts = 5  backoff_secs = 2

[[main_job.retry_steps]]
type = "fetch"      envelope = "ApiUserResponse"  writes = "api_rows"

[[main_job.retry_steps]]
type = "transform"  transform = "UserTransform"   reads = "api_rows"  writes = "db_rows"

[[main_job.retry_steps]]
type = "tx_upsert"  model = "DbUser"  reads = "db_rows"
# type = "send_to_queue"  model = "DbUser"  reads = "db_rows"  queue = "db_rows"

[[main_job.post_window_steps]]
type = "sleep"  secs = { env = "SOURCE__WINDOW_SLEEP_SECS", default = "60" }

[[post_job.steps]]  type = "log_summary"
[[post_job.steps]]  type = "raw_sql"  sql = { env = "SINK__SYNC_SQL", default = "" }  skip_if_empty = true
```

### Config value syntax

Any TOML string value can be written three ways:

```toml
timeout_secs = 620                                    # literal — used as-is
url          = { env = "SINK__DATABASE_URL" }         # required env var
cron         = { env = "SCHEDULER__CRON", default = "0 */30 * * * *" }
```

`pipeline.toml` is the single source of truth for every config knob. The env
key names are the documentation.

---

## `schema.toml` structure

```toml
# API source record — what the HTTP endpoint returns
[record.ApiUser]
serde_rename = "camelCase"          # JSON field naming

[record.ApiUser.fetcher]
envelope_field = "data"             # JSON key that holds the array

[[record.ApiUser.fields]]
name = "ssoAcct"  type = "String"

# DB target record — what gets upserted
[record.DbUser]

[record.DbUser.sink]
table       = "global_users"
primary_key = "pccuid"
upsert      = true

[[record.DbUser.fields]]
name = "sso_acct"  type = "String"

# Field mapping rules
[mapping.user]
name = "UserTransform"  from = "ApiUser"  to = "DbUser"

[[mapping.user.rules]]
field  = "sso_acct"   # target (snake_case)
source = "ssoAcct"    # source (camelCase) — omit if identical
rule   = "copy"       # copy | null_to_empty | bool_to_yn | option_bool_to_yn
                      # epoch_ms_to_ts | to_string
```

`build.rs` runs `sync_engine::codegen::generate("schema.toml")` and emits:

| File | Content |
|------|---------|
| `records.rs` | `ApiUser`, `DbUser` structs |
| `envelopes.rs` | `ApiUserResponse` + `HasEnvelope` impl |
| `upserts.rs` | `Upsertable` + `UpsertableInTx` impls for `DbUser` |
| `transforms.rs` | `UserTransform` struct + `Transform` impl |

---

## Adding a new sync job

1. Create `order-sync/` with `Cargo.toml` referencing `sync-engine`
2. Copy `user-sync/build.rs` unchanged — points to `schema.toml`
3. Write `schema.toml` for your API + DB shapes
4. Run `cargo build` once — `pipeline.toml` skeleton is generated automatically
5. Fill in env-var names in the skeleton
6. Write `src/main.rs` — three `registry.register_*` calls + `run()`
7. Add to workspace `members` in root `Cargo.toml`

---

## Startup validation

The engine validates `pipeline.toml` against the registry before the scheduler
starts. If you typo a type name or forget a `register_model` call, you get:

```
pipeline.toml validation failed:
  step type="tx_upsert": model "DbUsr" not registered
  — add registry.register_model::<DbUsr>("DbUsr") in main.rs
```

---

## Development tips

```bash
# Debug logging
RUST_LOG=user_sync=debug,sync_engine=debug cargo run

# See generated code
cargo build 2>&1 | grep "warning=Generated"
cat target/debug/build/user-sync-*/out/records.rs

# Run with .env
cd user-sync && cargo run   # dotenvy picks up .env automatically

# Test with a single window
SOURCE__START_INTERVAL=1 SOURCE__END_INTERVAL=0 SOURCE__INTERVAL_LIMIT=1 cargo run
```
