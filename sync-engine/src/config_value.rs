// sync-engine/src/config_value.rs
//
// ConfigValue — a TOML value that is either:
//   - a plain string literal:               "some value"
//   - an env-var reference:                 { env = "MY_VAR" }
//   - an env-var with a fallback:           { env = "MY_VAR", default = "fallback" }
//
// This lets pipeline.toml be the single source of truth for all configuration
// knobs. The env key is the documentation — you can see exactly which
// environment variable controls each setting without consulting a separate doc.
//
// Usage in TOML:
//
//   url      = { env = "SINK__DATABASE_URL" }
//   cron     = { env = "SCHEDULER__CRON", default = "0 */30 * * * *" }
//   timeout  = "620"                         # literal — no env lookup
//   label    = { env = "JOB_LABEL", default = "unnamed" }

use anyhow::Result;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum ConfigValue {
    /// A plain TOML string — used as-is.
    Literal(String),
    /// An environment variable reference, optionally with a default.
    Env(EnvRef),
}

#[derive(Debug, Clone, Deserialize)]
pub struct EnvRef {
    pub env:     String,
    pub default: Option<String>,
}

impl ConfigValue {
    /// Resolve this value to a String.
    /// Returns an error if the env var is missing and no default is set.
    pub fn resolve(&self) -> Result<String> {
        match self {
            ConfigValue::Literal(s) => Ok(s.clone()),
            ConfigValue::Env(r)     => {
                match std::env::var(&r.env) {
                    Ok(v) => Ok(v),
                    Err(_) => {
                        r.default.clone().ok_or_else(|| {
                            anyhow::anyhow!(
                                "Required env var \"{}\" is not set and has no default",
                                r.env
                            )
                        })
                    }
                }
            }
        }
    }

    /// Resolve and parse to a specific type.
    pub fn resolve_as<T: std::str::FromStr>(&self) -> Result<T>
    where
        T::Err: std::fmt::Display,
    {
        let s = self.resolve()?;
        s.parse::<T>().map_err(|e| {
            anyhow::anyhow!("Failed to parse config value \"{s}\": {e}")
        })
    }

    /// Resolve, returning a default T if the value is empty.
    pub fn resolve_or_default<T>(&self) -> Result<T>
    where
        T: std::str::FromStr + Default,
        T::Err: std::fmt::Display,
    {
        let s = self.resolve()?;
        if s.trim().is_empty() {
            return Ok(T::default());
        }
        s.parse::<T>()
            .map_err(|e| anyhow::anyhow!("Failed to parse \"{s}\": {e}"))
    }
}

// Allow ConfigValue where a plain String was expected in older TOML schemas.
impl From<String> for ConfigValue {
    fn from(s: String) -> Self { ConfigValue::Literal(s) }
}
impl From<&str> for ConfigValue {
    fn from(s: &str) -> Self { ConfigValue::Literal(s.to_owned()) }
}
