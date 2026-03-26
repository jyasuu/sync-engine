# Pipeline patterns

> This document shows how the same sync job can be expressed in four
> progressively more sophisticated patterns — all by changing `pipeline.toml`
> only. The Rust `main.rs` stays identical across every case.
>
> Each case has two example files: a legacy flat version (`case*-*.toml`) and
> a composable version (`case*-composable.toml`). Both compile to the same
> runtime behaviour. The composable versions are recommended for new pipelines.

---

## Step style comparison

The same case 1 pipeline expressed in both styles:

**Legacy flat** — uses `[main_job.iterator]`, `[main_job.retry]`, and
`[[main_job.retry_steps]]` as separate fixed sections:

```toml
[main_job.iterator]
type = "date_window"  start_interval = ...

[main_job.retry]
max_attempts = 5

[[main_job.retry_steps]]
type = "fetch"  ...

[[main_job.retry_steps]]
type = "tx_upsert"  ...
```

**Composable** — uses named `[step_groups]` and `[[main_job.steps]]` with
wrapper step types (`window_loop`, `retry`, `tx`):

```toml
[step_groups.commit_users]
steps = [{ type = "tx_upsert", model = "DbUser", reads = "db_rows" }]

[step_groups.fetch_and_transform]
steps = [
  { type = "fetch",     envelope = "ApiUserResponse", writes = "api_rows" },
  { type = "transform", transform = "UserTransform",  reads  = "api_rows", writes = "db_rows" },
  { type = "tx", group = "commit_users" },
]

[[main_job.steps]]
type           = "window_loop"
start_interval = { env = "SOURCE__START_INTERVAL", default = "30" }
end_interval   = 0
interval_limit = 7
steps = [
  { type = "retry", max_attempts = 5, group = "fetch_and_transform" },
  { type = "sleep", secs = 60 },
]
```

The composable style makes the nesting explicit and visible. `ARCHITECTURE.svg`
renders the actual step tree — wrappers appear as container boxes with children
nested inside.

---

## How to read this document

Every pattern shares the same **pre-job resources** (Postgres, OAuth2, HTTP)
and the same **retry loop** (fetch → transform). The only differences are:

| Axis | Case 1 | Case 2 | Case 3 | Case 4 |
|------|--------|--------|--------|--------|
| `db_rows` slot scope | `window` | `job` | replaced by queue | replaced by queue |
| Sink inside retry | `tx_upsert` | `slot_append` | `send_to_queue` | `rmq_publish` |
| Tx location | inside retry | after loop | inside consumer task | inside consumer task |
| Transport | none | none | tokio mpsc | RabbitMQ |
| Post-loop step | — | `tx_upsert` | — | — |
| Post-job step | `log_summary` | `slot_read` + `log_summary` | `drain_queue` + `log_summary` | `drain_queue` + `log_summary` |

The `main.rs` for every case:

```rust
// user-sync/src/main.rs  (identical for all four cases)
mod generated;
use generated::{envelopes::ApiUserResponse, records::{ApiUser, DbUser}, transforms::UserTransform};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let mut registry = sync_engine::TypeRegistry::new();
    registry.register_envelope::<ApiUserResponse>("ApiUserResponse");
    registry.register_transform::<ApiUser, DbUser, UserTransform>("UserTransform");
    registry.register_model::<DbUser>("DbUser");

    sync_engine::run("pipeline.toml", registry).await
}
```

---

## Case 1 — transactional upsert per window

**Pattern:** Each window's data is fetched, transformed, and committed in a
single Postgres transaction before moving to the next window. The simplest and
safest option — if a window fails, only that window is retried; all previous
windows are already committed.

```
pre_job
  ├── pg          (postgres connection)
  ├── auth        (oauth2 client — token cache + auto-refresh)
  ├── http        (http client)
  ├── svc         (http service — embeds auth)
  ├── slot api_rows  scope=window   ← cleared each retry attempt
  └── slot db_rows   scope=window   ← cleared each retry attempt

main_job
  └── date_window iterator
        └── retry(max=5, backoff=2s)
              ├── fetch svc          → api_rows   (HTTP GET with bearer token)
              ├── transform          api_rows → db_rows   (UserTransform)
              └── tx_upsert          db_rows     (BEGIN; upsert each row; COMMIT)
        └── sleep(window_sleep_secs)

post_job
  ├── log_summary   (windows / fetched / upserted / skipped / errors)
  └── raw_sql       (optional post-sync SQL, skipped if empty)
```

```toml
# pipeline.toml — Case 1: tx per window

[job]
name = "user-sync"

[job.trigger]
type = "cron"
cron = { env = "SCHEDULER__CRON", default = "0 */30 * * * *" }

# ── Resources ─────────────────────────────────────────────────────────────

[resources.pg]
type            = "postgres"
url             = { env = "SINK__DATABASE_URL" }
max_connections = 5

[resources.auth]
type          = "oauth2"
token_url     = { env = "AUTH__TOKEN_URL" }
client_id     = { env = "AUTH__CLIENT_ID" }
client_secret = { env = "AUTH__CLIENT_SECRET" }

[resources.http]
type           = "http_client"
timeout_secs   = 620
keepalive_secs = 30

[resources.svc]
type       = "http_service"
http       = "http"
auth       = "auth"
endpoint   = { env = "SOURCE__USER_ENDPOINT" }
realm_type = { env = "SOURCE__INCLUDE_REALM_TYPES", default = "" }

# ── Slots ─────────────────────────────────────────────────────────────────
# Both slots are window-scoped: cleared before each retry attempt.

[slots.api_rows]
type  = "ApiUser"
scope = "window"

[slots.db_rows]
type  = "DbUser"
scope = "window"        # ← case 1: window scope

# ── Pre-job ───────────────────────────────────────────────────────────────

[pre_job]
init_resources = true   # builds all [resources] in dependency order

# ── Main job ──────────────────────────────────────────────────────────────

[main_job.iterator]
type           = "date_window"
start_interval = { env = "SOURCE__START_INTERVAL",    default = "30" }
end_interval   = { env = "SOURCE__END_INTERVAL",      default = "0" }
interval_limit = { env = "SOURCE__INTERVAL_LIMIT",    default = "7" }
sleep_secs     = { env = "SOURCE__WINDOW_SLEEP_SECS", default = "60" }

[main_job.retry]
max_attempts = 5
backoff_secs = 2

[[main_job.retry_steps]]
type     = "fetch"
envelope = "ApiUserResponse"
writes   = "api_rows"
append   = false

[[main_job.retry_steps]]
type      = "transform"
transform = "UserTransform"
reads     = "api_rows"
writes    = "db_rows"
append    = false           # ← case 1: replace each retry

[[main_job.retry_steps]]
type  = "tx_upsert"         # ← case 1: commit inside retry loop
model = "DbUser"
reads = "db_rows"

[[main_job.post_window_steps]]
type = "sleep"
secs = { env = "SOURCE__WINDOW_SLEEP_SECS", default = "60" }

# ── Post-job ──────────────────────────────────────────────────────────────

[[post_job.steps]]
type = "log_summary"

[[post_job.steps]]
type          = "raw_sql"
sql           = { env = "SINK__SYNC_SQL", default = "" }
skip_if_empty = true
```

> **Composable version:** `docs/examples/case1-composable.toml`

---

## Case 2 — accumulate all windows, then one bulk transaction

**Pattern:** The transform step *appends* to a job-scoped `db_rows` slot across
all windows. After the iterator is exhausted, a single `tx_upsert` in
`post_loop_steps` commits everything in one transaction. Fewer round-trips to
Postgres; the tradeoff is higher memory use and an all-or-nothing commit.

```
pre_job
  ├── pg / auth / http / svc   (same as case 1)
  ├── slot api_rows  scope=window
  └── slot db_rows   scope=job     ← accumulates across ALL windows

main_job
  └── date_window iterator
        └── retry(max=5, backoff=2s)
              ├── fetch svc       → api_rows
              ├── transform       api_rows → db_rows  (append=true)
              └── (no tx here)
        └── sleep(window_sleep_secs)
  └── [post_loop] tx_upsert db_rows   ← ONE commit after all windows

post_job
  ├── slot_read db_rows  (available because scope=job)
  ├── log_summary
  └── raw_sql
```

```toml
# pipeline.toml — Case 2: bulk tx after all windows
# Diff from Case 1:
#   [slots.db_rows]  scope = "job"          (was "window")
#   transform        append = true          (was false)
#   retry_steps      no tx_upsert           (removed)
#   post_loop_steps  tx_upsert added        (new)
#   post_job         log_summary reads accumulated slot

[job]
name = "user-sync"

[job.trigger]
type = "cron"
cron = { env = "SCHEDULER__CRON", default = "0 */30 * * * *" }

[resources.pg]
type            = "postgres"
url             = { env = "SINK__DATABASE_URL" }
max_connections = 5

[resources.auth]
type          = "oauth2"
token_url     = { env = "AUTH__TOKEN_URL" }
client_id     = { env = "AUTH__CLIENT_ID" }
client_secret = { env = "AUTH__CLIENT_SECRET" }

[resources.http]
type           = "http_client"
timeout_secs   = 620
keepalive_secs = 30

[resources.svc]
type       = "http_service"
http       = "http"
auth       = "auth"
endpoint   = { env = "SOURCE__USER_ENDPOINT" }
realm_type = { env = "SOURCE__INCLUDE_REALM_TYPES", default = "" }

[slots.api_rows]
type  = "ApiUser"
scope = "window"

[slots.db_rows]
type  = "DbUser"
scope = "job"           # ← CHANGED: accumulates across all windows

[pre_job]
init_resources = true

[main_job.iterator]
type           = "date_window"
start_interval = { env = "SOURCE__START_INTERVAL",    default = "30" }
end_interval   = { env = "SOURCE__END_INTERVAL",      default = "0" }
interval_limit = { env = "SOURCE__INTERVAL_LIMIT",    default = "7" }
sleep_secs     = { env = "SOURCE__WINDOW_SLEEP_SECS", default = "60" }

[main_job.retry]
max_attempts = 5
backoff_secs = 2

[[main_job.retry_steps]]
type     = "fetch"
envelope = "ApiUserResponse"
writes   = "api_rows"
append   = false

[[main_job.retry_steps]]
type      = "transform"
transform = "UserTransform"
reads     = "api_rows"
writes    = "db_rows"
append    = true            # ← CHANGED: append across windows

# tx_upsert removed from retry_steps

[[main_job.post_window_steps]]
type = "sleep"
secs = { env = "SOURCE__WINDOW_SLEEP_SECS", default = "60" }

# ← ADDED: one bulk commit after iterator exhausted
[[main_job.post_loop_steps]]
type  = "tx_upsert"
model = "DbUser"
reads = "db_rows"

[[post_job.steps]]
type = "log_summary"        # reads job-scoped summary slots

[[post_job.steps]]
type          = "raw_sql"
sql           = { env = "SINK__SYNC_SQL", default = "" }
skip_if_empty = true
```

> **Composable version:** `docs/examples/case2-composable.toml`

---

## Case 3 — async producer / consumer (tokio channel)

**Pattern:** A consumer task is spawned in `pre_job` and runs concurrently for
the entire job. The main loop fetches and transforms as before, but instead of
writing directly to Postgres, it sends each batch of `DbUser` records to a
tokio `mpsc` channel. The consumer task receives batches and commits them
independently, decoupling fetch throughput from write throughput.

```
pre_job
  ├── pg / auth / http / svc   (same)
  ├── slot api_rows  scope=window
  ├── queue db_rows  type=tokio   ← replaces the slot
  └── spawn_consumer              ← spawned task: receive → tx_upsert

main_job
  └── date_window iterator
        └── retry(max=5, backoff=2s)
              ├── fetch svc       → api_rows
              ├── transform       api_rows → db_rows (window slot, temp)
              └── send_to_queue   db_rows → queue "db_rows"
        └── sleep(window_sleep_secs)

post_job
  ├── drain_queue "db_rows"   ← closes channel, awaits consumer task
  ├── log_summary
  └── raw_sql
```

```toml
# pipeline.toml — Case 3: async tokio producer/consumer
# Diff from Case 1:
#   [slots.db_rows]          removed entirely
#   [queues.db_rows]         added (tokio mpsc)
#   [[pre_job.steps]]        spawn_consumer added
#   retry_steps tx_upsert    replaced with send_to_queue
#   [[post_job.steps]]       drain_queue added before log_summary

[job]
name = "user-sync"

[job.trigger]
type = "cron"
cron = { env = "SCHEDULER__CRON", default = "0 */30 * * * *" }

[resources.pg]
type            = "postgres"
url             = { env = "SINK__DATABASE_URL" }
max_connections = 5

[resources.auth]
type          = "oauth2"
token_url     = { env = "AUTH__TOKEN_URL" }
client_id     = { env = "AUTH__CLIENT_ID" }
client_secret = { env = "AUTH__CLIENT_SECRET" }

[resources.http]
type           = "http_client"
timeout_secs   = 620
keepalive_secs = 30

[resources.svc]
type       = "http_service"
http       = "http"
auth       = "auth"
endpoint   = { env = "SOURCE__USER_ENDPOINT" }
realm_type = { env = "SOURCE__INCLUDE_REALM_TYPES", default = "" }

[slots.api_rows]
type  = "ApiUser"
scope = "window"

# ← CHANGED: db_rows is now a queue, not a slot
[queues.db_rows]
type     = "tokio"
capacity = 256

[pre_job]
init_resources = true

# ← ADDED: spawn the consumer task before main_job starts
[[pre_job.steps]]
type        = "spawn_consumer"
queue       = "db_rows"
model       = "DbUser"
commit_mode = "per_batch"   # commit each received batch immediately

[main_job.iterator]
type           = "date_window"
start_interval = { env = "SOURCE__START_INTERVAL",    default = "30" }
end_interval   = { env = "SOURCE__END_INTERVAL",      default = "0" }
interval_limit = { env = "SOURCE__INTERVAL_LIMIT",    default = "7" }
sleep_secs     = { env = "SOURCE__WINDOW_SLEEP_SECS", default = "60" }

[main_job.retry]
max_attempts = 5
backoff_secs = 2

[[main_job.retry_steps]]
type     = "fetch"
envelope = "ApiUserResponse"
writes   = "api_rows"
append   = false

[[main_job.retry_steps]]
type      = "transform"
transform = "UserTransform"
reads     = "api_rows"
writes    = "db_rows_tmp"   # temp window slot for the transformed batch
append    = false

# ← CHANGED: send to queue instead of tx_upsert
[[main_job.retry_steps]]
type  = "send_to_queue"
reads = "db_rows_tmp"
queue = "db_rows"

[[main_job.post_window_steps]]
type = "sleep"
secs = { env = "SOURCE__WINDOW_SLEEP_SECS", default = "60" }

# ← ADDED: drain queue in post_job (closes channel, awaits consumer)
[[post_job.steps]]
type  = "drain_queue"
queue = "db_rows"

[[post_job.steps]]
type = "log_summary"

[[post_job.steps]]
type          = "raw_sql"
sql           = { env = "SINK__SYNC_SQL", default = "" }
skip_if_empty = true
```

> **Composable version:** `docs/examples/case3-composable.toml`

---

## Case 4 — async producer / consumer (RabbitMQ)

**Pattern:** Identical to case 3 in structure. The only difference is the
transport layer: instead of an in-process tokio channel, `db_rows` is a
RabbitMQ exchange. The producer publishes each batch as a message; the consumer
is a separate subscriber. This pattern enables multi-process or multi-service
fan-out, and survives process restarts.

```
pre_job
  ├── pg / auth / http / svc   (same)
  ├── slot api_rows  scope=window
  ├── queue db_rows  type=rabbitmq   ← replaces tokio queue
  └── spawn_consumer                  ← subscribes to exchange

main_job  (identical to case 3)
  └── ...send_to_queue db_rows...

post_job  (identical to case 3)
  └── drain_queue → log_summary → raw_sql
```

```toml
# pipeline.toml — Case 4: RabbitMQ producer/consumer
# Diff from Case 3:
#   [queues.db_rows]   type = "rabbitmq" + connection config  (was "tokio")
#   Everything else is identical.

[job]
name = "user-sync"

[job.trigger]
type = "cron"
cron = { env = "SCHEDULER__CRON", default = "0 */30 * * * *" }

[resources.pg]
type            = "postgres"
url             = { env = "SINK__DATABASE_URL" }
max_connections = 5

[resources.auth]
type          = "oauth2"
token_url     = { env = "AUTH__TOKEN_URL" }
client_id     = { env = "AUTH__CLIENT_ID" }
client_secret = { env = "AUTH__CLIENT_SECRET" }

[resources.http]
type           = "http_client"
timeout_secs   = 620
keepalive_secs = 30

[resources.svc]
type       = "http_service"
http       = "http"
auth       = "auth"
endpoint   = { env = "SOURCE__USER_ENDPOINT" }
realm_type = { env = "SOURCE__INCLUDE_REALM_TYPES", default = "" }

[slots.api_rows]
type  = "ApiUser"
scope = "window"

# ← CHANGED: RabbitMQ transport instead of tokio channel
[queues.db_rows]
type        = "rabbitmq"
url         = { env = "RABBITMQ_URL" }
exchange    = { env = "RABBITMQ_EXCHANGE",    default = "sync" }
routing_key = { env = "RABBITMQ_ROUTING_KEY", default = "db_rows" }

[pre_job]
init_resources = true

[[pre_job.steps]]
type        = "spawn_consumer"
queue       = "db_rows"
model       = "DbUser"
commit_mode = "per_batch"

[main_job.iterator]
type           = "date_window"
start_interval = { env = "SOURCE__START_INTERVAL",    default = "30" }
end_interval   = { env = "SOURCE__END_INTERVAL",      default = "0" }
interval_limit = { env = "SOURCE__INTERVAL_LIMIT",    default = "7" }
sleep_secs     = { env = "SOURCE__WINDOW_SLEEP_SECS", default = "60" }

[main_job.retry]
max_attempts = 5
backoff_secs = 2

[[main_job.retry_steps]]
type     = "fetch"
envelope = "ApiUserResponse"
writes   = "api_rows"
append   = false

[[main_job.retry_steps]]
type      = "transform"
transform = "UserTransform"
reads     = "api_rows"
writes    = "db_rows_tmp"
append    = false

[[main_job.retry_steps]]
type  = "send_to_queue"
reads = "db_rows_tmp"
queue = "db_rows"

[[main_job.post_window_steps]]
type = "sleep"
secs = { env = "SOURCE__WINDOW_SLEEP_SECS", default = "60" }

[[post_job.steps]]
type  = "drain_queue"
queue = "db_rows"

[[post_job.steps]]
type = "log_summary"

[[post_job.steps]]
type          = "raw_sql"
sql           = { env = "SINK__SYNC_SQL", default = "" }
skip_if_empty = true
```

> **Composable version:** `docs/examples/case4-composable.toml`

---

## Minimal diff summary

The four cases share ~80% of their TOML. Here are the exact lines that differ:

```toml
# ── The three change points ───────────────────────────────────────────────

# 1. Data channel declaration (pick one)
[slots.db_rows]     scope = "window"          # case 1: tx per window
[slots.db_rows]     scope = "job"             # case 2: bulk tx
[queues.db_rows]    type  = "tokio"           # case 3: tokio channel
[queues.db_rows]    type  = "rabbitmq"        # case 4: RabbitMQ

# 2. Sink step inside [[main_job.retry_steps]] (pick one)
type = "tx_upsert"                            # case 1
# (removed — transform uses append=true)     # case 2
type = "send_to_queue"                        # case 3 & 4

# 3. When the write happens (pick one)
# inside retry_steps                          # case 1: per window
[[main_job.post_loop_steps]] type="tx_upsert" # case 2: after all windows
[[pre_job.steps]] type="spawn_consumer"       # case 3 & 4: concurrent
[[post_job.steps]] type="drain_queue"         # case 3 & 4: await consumer
```

---

## Adding a new sync job from scratch

1. **Define your data model** in `schema.toml`:
   - `[record.ApiXxx]` — API response shape (camelCase field names)
   - `[record.DbXxx]` — database target shape (snake_case)
   - `[mapping.xxx]` — field-by-field transformation rules

2. **Choose your pattern** and copy the matching `pipeline.toml` above.
   Replace `ApiUserResponse`, `UserTransform`, `DbUser` with your generated names.

3. **Write `main.rs`** — three `registry.register_*` calls + `sync_engine::run()`.

4. **Set environment variables** from `.env.example`.

5. `cargo run` — the engine does the rest.

---

## Trigger types

All four patterns share the same `run_one_tick()` core. Switch the trigger by changing `[job.trigger]` only — no Rust changes needed.

| Type | Config | Best for |
|------|--------|----------|
| `cron` | `cron = "0 */30 * * * *"` | Scheduled sync (default) |
| `once` | _(no extra fields)_ | CI, k8s Job, manual backfill |
| `webhook` | `host`, `port` | Event-driven via HTTP POST |
| `pg_notify` | `channel = "sync_trigger"` | Postgres LISTEN/NOTIFY |
| `kafka` | `topic = { env = "..." }` | Kafka message-driven |

```toml
# cron (default)
[job.trigger]
type = "cron"
cron = { env = "SCHEDULER__CRON", default = "0 */30 * * * *" }

# once — run and exit
[job.trigger]
type = "once"

# webhook — POST :8080 to fire
[job.trigger]
type = "webhook"
port = 8080

# pg_notify — LISTEN on a channel
[job.trigger]
type    = "pg_notify"
channel = { env = "TRIGGER__PG_CHANNEL", default = "sync_trigger" }

# kafka — each message fires a tick
[job.trigger]
type  = "kafka"
topic = { env = "TRIGGER__KAFKA_TOPIC" }
```

---

## Backend components (Idea 3)

### Postgres commit strategies

Set `commit_strategy` on any `tx_upsert` step:

| Strategy | Behaviour | Use when |
|----------|-----------|----------|
| `tx_scope` | One tx per window (default) | Atomicity per window |
| `autocommit` | One tx per row | Partial success OK, long windows |
| `bulk_tx` | Accumulate; commit in `post_loop_steps` | Fewest total commits |

```toml
# autocommit — each row independently committed
[[main_job.retry_steps]]
type            = "tx_upsert"
model           = "DbUser"
reads           = "db_rows"
commit_strategy = "autocommit"
```

### RabbitMQ ack strategies

Set `ack_strategy` on `send_to_queue` steps backed by a RabbitMQ queue:

| Strategy | Behaviour |
|----------|-----------|
| `ack_on_commit` | Ack after Postgres tx commits — at-least-once (default) |
| `ack_on_receive` | Ack immediately — at-most-once, maximum throughput |
| `ack_per_batch` | Alias for `ack_on_commit` |

```toml
[[main_job.retry_steps]]
type         = "send_to_queue"
reads        = "db_rows"
queue        = "db_rows"
ack_strategy = "ack_on_receive"   # fast path
```

### Elasticsearch

Requires feature `elasticsearch`. Add `[resources.es]`, then use `es_index` (sink) or `es_fetch` (source).

```toml
[resources.es]
type = "elasticsearch"
url  = { env = "ES_URL", default = "http://localhost:9200" }

# Sink — index documents
[[main_job.retry_steps]]
type  = "es_index"
model = "DbUser"     # implement EsIndexable: fn es_id(&self) -> String
index = "users"
reads = "db_rows"
op    = "upsert"     # upsert | delete | create

# Source — scroll query
[[main_job.retry_steps]]
type   = "es_fetch"
model  = "ApiUser"
index  = "source-users"
writes = "api_rows"
```

### Kafka

Requires feature `kafka`. Add `[resources.kafka]`, then use `kafka_produce` (sink), `kafka_consume` (source), or `type = "kafka"` trigger.

```toml
[resources.kafka]
type     = "kafka"
brokers  = { env = "KAFKA_BROKERS", default = "localhost:9092" }
group_id = { env = "KAFKA_GROUP_ID", default = "sync-engine" }
topic    = { env = "KAFKA_TOPIC" }   # required for consume source/trigger

# Sink — produce records
[[main_job.retry_steps]]
type      = "kafka_produce"
model     = "DbUser"
reads     = "db_rows"
topic     = { env = "KAFKA_SINK_TOPIC" }
key_field = "pccuid"   # optional

# Source — consume batch per window
[[main_job.retry_steps]]
type         = "kafka_consume"
model        = "ApiUser"
writes       = "api_rows"
topic        = { env = "KAFKA_SOURCE_TOPIC" }
max_messages = 500
```
