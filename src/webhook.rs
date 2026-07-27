use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use reqwest::{
    Client, StatusCode,
    header::{AUTHORIZATION, CONTENT_TYPE, HeaderValue},
    redirect::Policy,
};
use serde_json::json;
use tokio::sync::{Mutex, watch};

use crate::{
    USER_AGENT,
    config::{WebhookConfig, validate_webhook_url},
    domain::NowPlayingMessage,
};

#[derive(Debug, Clone, Default)]
pub struct WebhookStatus {
    pub delivered: bool,
    pub detail: String,
}

pub struct WebhookPublisher {
    latest: watch::Sender<Option<NowPlayingMessage>>,
    status: Arc<Mutex<WebhookStatus>>,
}

impl WebhookPublisher {
    #[must_use]
    pub fn spawn(config: WebhookConfig) -> Self {
        let (latest, receiver) = watch::channel(None);
        let status = Arc::new(Mutex::new(WebhookStatus {
            delivered: false,
            detail: "waiting for a track".to_owned(),
        }));
        tokio::spawn(run_webhook(config, receiver, Arc::clone(&status)));
        Self { latest, status }
    }

    pub fn publish_latest(&self, message: NowPlayingMessage) {
        self.latest.send_replace(Some(message));
    }

    pub fn clear_if_idle(&self) {
        self.latest.send_replace(None);
    }

    pub async fn status(&self) -> WebhookStatus {
        self.status.lock().await.clone()
    }
}

async fn run_webhook(
    config: WebhookConfig,
    mut latest: watch::Receiver<Option<NowPlayingMessage>>,
    status: Arc<Mutex<WebhookStatus>>,
) {
    let client = match webhook_client() {
        Ok(client) => client,
        Err(error) => {
            status.lock().await.detail = format!("{error:#}");
            return;
        }
    };
    if let Err(error) = validate_webhook_url(&config.url) {
        status.lock().await.detail = format!("{error:#}");
        return;
    }

    loop {
        let current = latest.borrow_and_update().clone();
        let Some(message) = current else {
            let mut current_status = status.lock().await;
            current_status.delivered = false;
            "waiting for a track".clone_into(&mut current_status.detail);
            drop(current_status);
            if latest.changed().await.is_err() {
                return;
            }
            continue;
        };

        let mut backoff = 1;
        loop {
            match post_message(&client, &config, &message).await {
                Ok(()) => {
                    let mut current_status = status.lock().await;
                    current_status.delivered = true;
                    "delivered".clone_into(&mut current_status.detail);
                    drop(current_status);
                    if latest.changed().await.is_err() {
                        return;
                    }
                    break;
                }
                Err(error) => {
                    let mut current_status = status.lock().await;
                    current_status.delivered = false;
                    current_status.detail = format!("{error:#}");
                    drop(current_status);
                    tokio::select! {
                        () = tokio::time::sleep(Duration::from_secs(backoff)) => {
                            backoff = (backoff * 2).min(60);
                        }
                        changed = latest.changed() => {
                            if changed.is_err() {
                                return;
                            }
                            break;
                        }
                    }
                }
            }
        }
    }
}

fn webhook_client() -> Result<Client> {
    Client::builder()
        .user_agent(USER_AGENT)
        .timeout(Duration::from_secs(10))
        // Never forward a bearer token across a redirect.
        .redirect(Policy::none())
        .build()
        .context("failed to create webhook HTTP client")
}

fn request(
    client: &Client,
    config: &WebhookConfig,
    body: Vec<u8>,
) -> Result<reqwest::RequestBuilder> {
    validate_webhook_url(&config.url)?;
    let mut request = client
        .post(config.url.trim())
        .header(CONTENT_TYPE, "application/json")
        .body(body);
    if let Some(token) = config
        .bearer_token
        .as_deref()
        .map(str::trim)
        .filter(|token| !token.is_empty())
    {
        let value = HeaderValue::from_str(&format!("Bearer {token}"))
            .context("webhook bearer token contains invalid characters")?;
        request = request.header(AUTHORIZATION, value);
    }
    Ok(request)
}

async fn post_message(
    client: &Client,
    config: &WebhookConfig,
    message: &NowPlayingMessage,
) -> Result<()> {
    let body = serde_json::to_vec(message).context("failed to encode webhook JSON")?;
    let response = request(client, config, body)?
        .send()
        .await
        .context("webhook request failed")?;
    require_success(response.status())
}

fn require_success(status: StatusCode) -> Result<()> {
    if status.is_success() {
        Ok(())
    } else {
        bail!("webhook returned HTTP {status}")
    }
}

/// POST a small diagnostic event to verify the configured endpoint and
/// credentials.
///
/// # Errors
///
/// Returns an error for invalid settings, transport failures, or non-2xx
/// responses.
pub async fn test_connection(config: &WebhookConfig) -> Result<()> {
    let client = webhook_client()?;
    let body = serde_json::to_vec(&json!({
        "schema_version": 1,
        "event": "test",
        "source": "tunebeacon"
    }))?;
    let response = request(&client, config, body)?
        .send()
        .await
        .context("webhook test request failed")?;
    require_success(response.status())
}
