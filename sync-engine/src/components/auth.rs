// sync-engine/src/components/auth.rs
use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::Utc;
use reqwest::Client;
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::pipeline::primitives::Auth;

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: i64,
}

#[derive(Clone)]
struct Cached {
    token: String,
    expires_at: i64,
}

impl Cached {
    fn is_expired(&self) -> bool {
        Utc::now().timestamp() >= self.expires_at - 60
    }
}

pub struct OAuth2Auth {
    client: Client,
    token_url: String,
    client_id: String,
    client_secret: String,
    cached: Arc<Mutex<Option<Cached>>>,
}

impl OAuth2Auth {
    pub fn new(
        client: Client,
        token_url: impl Into<String>,
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
    ) -> Self {
        Self {
            client,
            token_url: token_url.into(),
            client_id: client_id.into(),
            client_secret: client_secret.into(),
            cached: Arc::new(Mutex::new(None)),
        }
    }
}

#[async_trait]
impl Auth for OAuth2Auth {
    async fn get_token(&self) -> Result<String> {
        let mut guard = self.cached.lock().await;
        if let Some(ref c) = *guard {
            if !c.is_expired() {
                return Ok(c.token.clone());
            }
            warn!("Token expired — refreshing");
        }
        let resp = self
            .client
            .post(&self.token_url)
            .form(&[
                ("grant_type", "client_credentials"),
                ("client_id", &self.client_id),
                ("client_secret", &self.client_secret),
            ])
            .send()
            .await
            .context("Token request failed")?
            .error_for_status()
            .context("Token endpoint error")?
            .json::<TokenResponse>()
            .await
            .context("Token parse failed")?;
        info!("Token acquired; valid for {} s", resp.expires_in);
        *guard = Some(Cached {
            token: resp.access_token.clone(),
            expires_at: Utc::now().timestamp() + resp.expires_in,
        });
        Ok(resp.access_token)
    }

    async fn invalidate(&self) {
        *self.cached.lock().await = None;
        warn!("Token cache cleared");
    }
}
