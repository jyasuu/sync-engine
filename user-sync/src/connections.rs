// user-sync/src/connections.rs
use anyhow::Result;
use chrono::Utc;
use reqwest::Client;
use serde::Deserialize;
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::config::AppConfig;
use sync_engine::Connections;

#[derive(Clone)]
pub struct TokenState {
    pub access_token: String,
    pub expires_at: i64,
}
impl TokenState {
    pub fn is_expired(&self) -> bool {
        Utc::now().timestamp() >= self.expires_at - 60
    }
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: i64,
}

#[derive(Clone)]
pub struct OAuth2Client {
    http: Client,
    token_url: String,
    client_id: String,
    client_secret: String,
    pub cached: Arc<Mutex<Option<TokenState>>>,
}

impl OAuth2Client {
    pub fn new(http: Client, token_url: &str, client_id: &str, client_secret: &str) -> Self {
        Self {
            http,
            token_url: token_url.to_owned(),
            client_id: client_id.to_owned(),
            client_secret: client_secret.to_owned(),
            cached: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn get_token(&self) -> Result<String> {
        let mut guard = self.cached.lock().await;
        if let Some(ref t) = *guard {
            if !t.is_expired() {
                return Ok(t.access_token.clone());
            }
            tracing::warn!("Token expired — refreshing");
        }
        let resp = self
            .http
            .post(&self.token_url)
            .form(&[
                ("grant_type", "client_credentials"),
                ("client_id", &self.client_id),
                ("client_secret", &self.client_secret),
            ])
            .send()
            .await?
            .error_for_status()?
            .json::<TokenResponse>()
            .await?;
        tracing::info!("Token acquired; valid for {} s", resp.expires_in);
        *guard = Some(TokenState {
            access_token: resp.access_token.clone(),
            expires_at: Utc::now().timestamp() + resp.expires_in,
        });
        Ok(resp.access_token)
    }

    pub async fn invalidate(&self) {
        *self.cached.lock().await = None;
        tracing::warn!("Token invalidated");
    }
}

pub struct UserServiceClient {
    pub http: Client,
    pub endpoint: String,
    pub realm_type: Option<String>,
}

impl UserServiceClient {
    pub fn new(http: Client, endpoint: &str, realm_type: Option<String>) -> Self {
        Self {
            http,
            endpoint: endpoint.to_owned(),
            realm_type,
        }
    }
}

pub struct JobConnections {
    pub db: PgPool,
    pub auth: OAuth2Client,
    pub user_service: UserServiceClient,
}

impl Connections for JobConnections {}

impl JobConnections {
    pub async fn create(cfg: &AppConfig) -> Result<Self> {
        tracing::info!("pre_job: creating postgres connection pool");
        let db = PgPoolOptions::new()
            .max_connections(5)
            .connect(&cfg.sink.database_url) // nested: cfg.sink.database_url
            .await?;

        let http = Client::builder()
            .timeout(std::time::Duration::from_secs(620))
            .tcp_keepalive(std::time::Duration::from_secs(30))
            .build()?;

        tracing::info!("pre_job: creating oauth2 client");
        let auth = OAuth2Client::new(
            http.clone(),
            &cfg.auth.token_url, // nested: cfg.auth.*
            &cfg.auth.client_id,
            &cfg.auth.client_secret,
        );

        tracing::info!("pre_job: creating user service client");
        let realm = if cfg.source.include_realm_types.is_empty() {
            None
        } else {
            Some(cfg.source.include_realm_types.clone())
        };
        let user_service = UserServiceClient::new(
            http,
            &cfg.source.user_endpoint, // nested: cfg.source.*
            realm,
        );

        Ok(Self {
            db,
            auth,
            user_service,
        })
    }
}
