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

---

## Trigger types

All patterns share the same `run_one_tick()` core. Switch with `[job.trigger]` only — no Rust changes needed. The legacy `[job.scheduler]` key is still accepted as a cron alias.

| Type | Config | Best for |
|------|--------|----------|
| `cron` | `cron = "0 */30 * * * *"` | Scheduled sync (default) |
| `once` | _(no extra fields)_ | CI, k8s Job, manual backfill |
| `webhook` | `host`, `port` | Event-driven via HTTP POST |
| `pg_notify` | `channel` | Postgres LISTEN/NOTIFY |
| `kafka` | `topic` | Kafka message-driven |

```toml
[job.trigger]
type = "pg_notify"
channel = { env = "TRIGGER__PG_CHANNEL", default = "sync_trigger" }
```

See `docs/examples/case5` through `case7` for full working examples.

---

## `[init_job]` — one-time startup stage

`[init_job]` runs once after `build_context` and `validate`, before the trigger loop. Use it for migrations, connectivity checks, and pre-seeding pipeline-scoped slots that every tick can read.

```toml
[[init_job.steps]]
type          = "raw_sql"
sql           = { env = "INIT__MIGRATION_SQL", default = "" }
skip_if_empty = true

[[init_job.steps]]
type = "custom"
hook = "WarmLookupCache"   # registered in main.rs
```

Pipeline-scoped slots written in `init_job` survive all ticks. See `docs/examples/case11-init-job.toml`.

**Lifecycle:**
```
program start → build_context → validate → [init_job] → trigger loop
                                                              ↓ (per tick)
                                              pre_job → main_job → post_job
```

---

## Complex transform patterns

### `split_transform` — fan-out

Apply multiple registered transforms to the same source slot, each writing to a different output slot. Use when one API record maps to multiple DB tables.

```toml
[[main_job.retry_steps]]
type   = "split_transform"
reads  = "api_rows"
transforms = [
  { transform = "UserTransform",     writes = "db_users" },
  { transform = "UserRoleTransform", writes = "db_roles" },
]
```

Register both transforms in `main.rs`:
```rust
registry.register_transform::<ApiUser, DbUser, UserTransform>("UserTransform");
registry.register_transform::<ApiUser, DbUserRole, UserRoleTransform>("UserRoleTransform");
```

### `merge_slots` — fan-in

Concatenate multiple same-typed slots into one before a single sink. Use when data arrives from multiple upstream sources.

```toml
[[main_job.retry_steps]]
type        = "merge_slots"
model       = "DbUser"
merge_reads = ["db_rows_primary", "db_rows_legacy"]
writes      = "db_rows_all"
```

### `slot_to_json` / `json_to_slot` — JSON passthrough

Serialize any typed slot to `Vec<serde_json::Value>` and back. Enables ES indexing without `EsIndexable`, Kafka JSON bridging, and JSONB audit columns.

```toml
[[main_job.retry_steps]]
type   = "slot_to_json"
model  = "DbUser"
reads  = "db_rows"
writes = "db_rows_json"
```

---

## Backend components (optional features)

Add to `Cargo.toml` as needed:
```toml
sync-engine = { features = ["elasticsearch", "kafka"] }
```

### Postgres commit strategies

Set `commit_strategy` on any `tx_upsert` step:

| Strategy | Behaviour |
|----------|-----------|
| `tx_scope` | One transaction per window (default) |
| `autocommit` | One transaction per row — partial success OK |
| `bulk_tx` | Accumulate all windows, commit once in `post_loop_steps` |

### RabbitMQ ack strategies

Set `ack_strategy` on `send_to_queue` steps backed by a RabbitMQ queue:

| Strategy | Behaviour |
|----------|-----------|
| `ack_on_commit` | Ack after Postgres tx commits — at-least-once (default) |
| `ack_on_receive` | Ack immediately — at-most-once, maximum throughput |

### Elasticsearch (`feature = "elasticsearch"`)

Sink (`es_index`) or source (`es_fetch`) via the ES REST API over reqwest — no extra crate needed.

```toml
[resources.es]
type = "elasticsearch"
url  = { env = "ES_URL", default = "http://localhost:9200" }

[[main_job.retry_steps]]
type  = "es_index"
model = "DbUser"     # implement EsIndexable: fn es_id(&self) -> String
index = "users"
reads = "db_rows"
op    = "upsert"     # upsert | delete | create
```

Register with `registry.register_model_es::<DbUser>("DbUser")` instead of `register_model`.

### Kafka (`feature = "kafka"`)

Produce (sink), consume (source), or trigger:

```toml
[resources.kafka]
type    = "kafka"
brokers = { env = "KAFKA_BROKERS", default = "localhost:9092" }

# Sink
[[main_job.retry_steps]]
type  = "kafka_produce"
model = "DbUser"
topic = { env = "KAFKA_SINK_TOPIC" }

# Trigger
[job.trigger]
type  = "kafka"
topic = { env = "TRIGGER__KAFKA_TOPIC" }
```

---

## Architecture diagram

`build.rs` generates `ARCHITECTURE.svg` (standalone, viewable in browser, diffable in git) and `ARCHITECTURE.md` (embeds the SVG). The diagram shows the full lifecycle — startup → `[init_job]` → trigger type → per-tick phases — alongside the schema and pipeline sections.

```rust
// user-sync/build.rs
fn main() {
    sync_engine::codegen::generate("schema.toml");
    sync_engine::codegen::generate_pipeline_skeleton("schema.toml", "pipeline.toml");
    sync_engine::codegen::generate_architecture_svg_file("schema.toml", "pipeline.toml", "ARCHITECTURE.svg");
    sync_engine::codegen::generate_architecture_svg("schema.toml", "pipeline.toml", "ARCHITECTURE.md", "ARCHITECTURE.svg");
}
```
