# user-sync/config.toml
#
# Default values only. Override any key with an environment variable:
#   AUTH__CLIENT_SECRET=prod-secret
#   SOURCE__USER_ENDPOINT=https://api.example.com/users
#
# Sensitive values (passwords, secrets, tokens) must be set via env vars —
# never committed to version control.
#
# Keys ending in __desc / __example are annotations used by generate_config_doc
# to produce CONFIG.md and are ignored at runtime.

[log]
rust_log = "info"
rust_log__desc = "Tracing filter string passed to EnvFilter"
rust_log__example = "user_sync=debug,sync_engine=debug"

[auth]
token_url     = ""
client_id     = ""
client_secret = ""
token_url__desc     = "OAuth2 token endpoint"
client_id__desc     = "OAuth2 client ID"
client_secret__desc = "OAuth2 client secret — set via AUTH__CLIENT_SECRET env var"
token_url__example  = "https://auth.example.com/oauth2/token"

[source]
user_endpoint        = ""
start_interval       = 30
end_interval         = 0
interval_limit       = 7
window_sleep_secs    = 60
include_realm_types  = ""
user_endpoint__desc       = "Base URL of the user API"
start_interval__desc      = "Days ago to start the first window"
end_interval__desc        = "Days ago to end the last window (0 = today)"
interval_limit__desc      = "Days covered per window"
window_sleep_secs__desc   = "Sleep between windows in seconds"
include_realm_types__desc = "Optional realm_type query param value; leave empty to omit"

[sink]
database_url  = ""
sync_sql      = ""
database_url__desc = "Postgres connection string — set via SINK__DATABASE_URL env var"
sync_sql__desc     = "Optional SQL to run after each job completes"
sync_sql__example  = "REFRESH MATERIALIZED VIEW user_summary"

[scheduler]
cron = "0 */30 * * * *"
cron__desc    = "Cron expression for how often to run the job"
cron__example = "0 0 2 * * *"
