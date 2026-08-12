use std::{fmt, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use reqwest::{
    Client, StatusCode,
    header::{AUTHORIZATION, CONTENT_TYPE, HeaderValue},
    redirect::Policy,
};
use serde::Serialize;
use serde_json::Value;
use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;

use crate::{
    USER_AGENT,
    config::{ListenBrainzConfig, validate_listenbrainz},
    domain::NowPlayingMessage,
    lastfm::ScrobbleAction,
};

const API_ROOT: &str = "https://api.listenbrainz.org";
const SUBMIT_ENDPOINT: &str = "/1/submit-listens";
const VALIDATE_ENDPOINT: &str = "/1/validate-token";

#[derive(Debug, Clone, Default)]
pub struct ListenBrainzStatus {
    pub authenticated: bool,
    pub detail: String,
}

pub struct ListenBrainzPublisher {
    sender: mpsc::UnboundedSender<ScrobbleAction>,
    status: Arc<Mutex<ListenBrainzStatus>>,
    shutdown: CancellationToken,
}

impl ListenBrainzPublisher {
    #[must_use]
    pub fn spawn(config: ListenBrainzConfig) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        let status = Arc::new(Mutex::new(ListenBrainzStatus {
            authenticated: false,
            detail: "waiting for a track".to_owned(),
        }));
        let shutdown = CancellationToken::new();
        tokio::spawn(run_submissions(
            config,
            receiver,
            Arc::clone(&status),
            shutdown.clone(),
        ));
        Self {
            sender,
            status,
            shutdown,
        }
    }

    pub(crate) fn submit(&self, action: ScrobbleAction) {
        let _ = self.sender.send(action);
    }

    pub async fn status(&self) -> ListenBrainzStatus {
        self.status.lock().await.clone()
    }
}

impl Drop for ListenBrainzPublisher {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

#[derive(Debug)]
enum FailureKind {
    Retryable,
    Authentication,
    Permanent,
}

#[derive(Debug)]
struct ApiFailure {
    kind: FailureKind,
    message: String,
}

impl fmt::Display for ApiFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ApiFailure {}

struct ListenBrainzApi {
    client: Client,
    endpoint: String,
    token: String,
}

impl ListenBrainzApi {
    fn new(config: &ListenBrainzConfig) -> std::result::Result<Self, ApiFailure> {
        validate_listenbrainz(config).map_err(|error| ApiFailure {
            kind: FailureKind::Permanent,
            message: format!("{error:#}"),
        })?;
        let client = api_client().map_err(|error| ApiFailure {
            kind: FailureKind::Permanent,
            message: format!("{error:#}"),
        })?;
        Ok(Self {
            client,
            endpoint: format!("{API_ROOT}{SUBMIT_ENDPOINT}"),
            token: config.token.trim().to_owned(),
        })
    }

    async fn now_playing(
        &self,
        message: &NowPlayingMessage,
    ) -> std::result::Result<(), ApiFailure> {
        self.submit(&submission("playing_now", message, None)).await
    }

    async fn listen(
        &self,
        message: &NowPlayingMessage,
        started_at: i64,
    ) -> std::result::Result<(), ApiFailure> {
        self.submit(&submission("single", message, Some(started_at)))
            .await
    }

    async fn submit(&self, body: &Submission<'_>) -> std::result::Result<(), ApiFailure> {
        let body = serde_json::to_vec(body).map_err(|error| ApiFailure {
            kind: FailureKind::Permanent,
            message: format!("failed to encode ListenBrainz JSON: {error}"),
        })?;
        let response = authorized_request(&self.client, &self.endpoint, &self.token)
            .map_err(|error| ApiFailure {
                kind: FailureKind::Permanent,
                message: format!("{error:#}"),
            })?
            .header(CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .map_err(|error| ApiFailure {
                kind: FailureKind::Retryable,
                message: format!("ListenBrainz request failed: {error}"),
            })?;
        response_result(response).await
    }
}

async fn run_submissions(
    config: ListenBrainzConfig,
    mut receiver: mpsc::UnboundedReceiver<ScrobbleAction>,
    status: Arc<Mutex<ListenBrainzStatus>>,
    shutdown: CancellationToken,
) {
    let api = match ListenBrainzApi::new(&config) {
        Ok(api) => api,
        Err(error) => {
            status.lock().await.detail = error.to_string();
            return;
        }
    };
    loop {
        let action = tokio::select! {
            () = shutdown.cancelled() => return,
            action = receiver.recv() => {
                let Some(action) = action else { return };
                action
            }
        };
        match action {
            ScrobbleAction::NowPlaying(message) => {
                let result = tokio::select! {
                    () = shutdown.cancelled() => return,
                    result = api.now_playing(&message) => result,
                };
                set_status(&status, result, "now playing sent").await;
            }
            ScrobbleAction::Scrobble {
                message,
                started_at,
            } => {
                let mut backoff = Duration::from_secs(1);
                loop {
                    let result = tokio::select! {
                        () = shutdown.cancelled() => return,
                        result = api.listen(&message, started_at) => result,
                    };
                    match result {
                        Ok(()) => {
                            set_status(&status, Ok(()), "listen submitted").await;
                            break;
                        }
                        Err(error) if matches!(error.kind, FailureKind::Retryable) => {
                            status.lock().await.detail = format!("retrying listen: {error}");
                            tokio::select! {
                                () = shutdown.cancelled() => return,
                                () = tokio::time::sleep(backoff) => {}
                            }
                            backoff = (backoff * 2).min(Duration::from_mins(1));
                        }
                        Err(error) => {
                            set_status(&status, Err(error), "").await;
                            break;
                        }
                    }
                }
            }
        }
    }
}

async fn set_status(
    status: &Mutex<ListenBrainzStatus>,
    result: std::result::Result<(), ApiFailure>,
    success: &str,
) {
    let mut current = status.lock().await;
    match result {
        Ok(()) => {
            current.authenticated = true;
            success.clone_into(&mut current.detail);
        }
        Err(error) => {
            if matches!(error.kind, FailureKind::Authentication) {
                current.authenticated = false;
            }
            current.detail = error.to_string();
        }
    }
}

#[derive(Debug, Serialize)]
struct Submission<'a> {
    listen_type: &'static str,
    payload: [Listen<'a>; 1],
}

#[derive(Debug, Serialize)]
struct Listen<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    listened_at: Option<i64>,
    track_metadata: TrackMetadata<'a>,
}

#[derive(Debug, Serialize)]
struct TrackMetadata<'a> {
    artist_name: String,
    track_name: &'a str,
    #[serde(skip_serializing_if = "str::is_empty")]
    release_name: &'a str,
    additional_info: AdditionalInfo<'a>,
}

#[derive(Debug, Serialize)]
struct AdditionalInfo<'a> {
    submission_client: &'static str,
    submission_client_version: &'static str,
    media_player: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    duration_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    recording_mbid: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    release_mbid: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    release_group_mbid: Option<&'a str>,
}

fn submission<'a>(
    listen_type: &'static str,
    message: &'a NowPlayingMessage,
    listened_at: Option<i64>,
) -> Submission<'a> {
    Submission {
        listen_type,
        payload: [Listen {
            listened_at,
            track_metadata: TrackMetadata {
                artist_name: message.artists.join(", "),
                track_name: &message.track,
                release_name: &message.album,
                additional_info: AdditionalInfo {
                    submission_client: crate::APP_NAME,
                    submission_client_version: env!("CARGO_PKG_VERSION"),
                    media_player: &message.player.identity,
                    duration_ms: message.duration_ms,
                    recording_mbid: message.verification.recording_id.as_deref(),
                    release_mbid: message.verification.release_id.as_deref(),
                    release_group_mbid: message.verification.release_group_id.as_deref(),
                },
            },
        }],
    }
}

fn api_client() -> Result<Client> {
    Client::builder()
        .user_agent(USER_AGENT)
        .timeout(Duration::from_secs(10))
        .redirect(Policy::none())
        .build()
        .context("failed to create ListenBrainz HTTP client")
}

fn authorized_request(
    client: &Client,
    endpoint: &str,
    token: &str,
) -> Result<reqwest::RequestBuilder> {
    let authorization = HeaderValue::from_str(&format!("Token {}", token.trim()))
        .context("ListenBrainz token contains invalid characters")?;
    Ok(client.post(endpoint).header(AUTHORIZATION, authorization))
}

async fn response_result(response: reqwest::Response) -> std::result::Result<(), ApiFailure> {
    let status = response.status();
    let body = response.text().await.map_err(|error| ApiFailure {
        kind: FailureKind::Retryable,
        message: format!("failed to read ListenBrainz response: {error}"),
    })?;
    if status.is_success() {
        return Ok(());
    }
    let detail = serde_json::from_str::<Value>(&body)
        .ok()
        .and_then(|value| {
            value
                .get("error")
                .or_else(|| value.get("message"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .filter(|value| !value.is_empty());
    let kind = match status {
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => FailureKind::Authentication,
        StatusCode::TOO_MANY_REQUESTS => FailureKind::Retryable,
        _ if status.is_server_error() => FailureKind::Retryable,
        _ => FailureKind::Permanent,
    };
    Err(ApiFailure {
        kind,
        message: detail.map_or_else(
            || format!("ListenBrainz returned HTTP {status}"),
            |detail| format!("ListenBrainz returned HTTP {status}: {detail}"),
        ),
    })
}

/// Validate a configured user token and return its `MusicBrainz` username.
///
/// # Errors
///
/// Returns an error for incomplete settings, transport failures, malformed
/// responses, or an invalid token.
pub async fn validate_token(config: &ListenBrainzConfig) -> Result<String> {
    validate_listenbrainz(config)?;
    let client = api_client()?;
    let endpoint = format!("{API_ROOT}{VALIDATE_ENDPOINT}");
    let authorization = HeaderValue::from_str(&format!("Token {}", config.token.trim()))
        .context("ListenBrainz token contains invalid characters")?;
    let response = client
        .get(endpoint)
        .header(AUTHORIZATION, authorization)
        .send()
        .await
        .context("ListenBrainz token validation failed")?;
    let status = response.status();
    let response_body = response
        .text()
        .await
        .context("failed to read ListenBrainz validation response")?;
    let body: Value = serde_json::from_str(&response_body)
        .with_context(|| format!("invalid ListenBrainz validation response (HTTP {status})"))?;
    if !status.is_success() {
        anyhow::bail!("ListenBrainz token validation returned HTTP {status}");
    }
    if !body.get("valid").and_then(Value::as_bool).unwrap_or(false) {
        anyhow::bail!("ListenBrainz token is invalid");
    }
    body.get("user_name")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .context("ListenBrainz validation response omitted the username")
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::*;
    use crate::domain::{PublishedPlayer, Verification, VerificationStatus};

    fn message() -> NowPlayingMessage {
        NowPlayingMessage {
            schema_version: 1,
            observed_at: Utc.with_ymd_and_hms(2026, 8, 12, 1, 2, 3).unwrap(),
            track: "A Track".to_owned(),
            artists: vec!["First Artist".to_owned(), "Second Artist".to_owned()],
            album: "An Album".to_owned(),
            duration_ms: Some(180_000),
            art_url: None,
            player: PublishedPlayer {
                key: "player".to_owned(),
                identity: "Music Player".to_owned(),
            },
            verification: Verification {
                status: VerificationStatus::Verified,
                score: Some(100),
                recording_id: Some("recording-id".to_owned()),
                release_id: Some("release-id".to_owned()),
                release_group_id: Some("release-group-id".to_owned()),
            },
        }
    }

    #[test]
    fn playing_now_omits_timestamp_and_includes_enrichment() {
        let value = serde_json::to_value(submission("playing_now", &message(), None)).unwrap();
        assert_eq!(value["listen_type"], "playing_now");
        assert!(value["payload"][0].get("listened_at").is_none());
        assert_eq!(
            value["payload"][0]["track_metadata"]["artist_name"],
            "First Artist, Second Artist"
        );
        assert_eq!(
            value["payload"][0]["track_metadata"]["additional_info"]["recording_mbid"],
            "recording-id"
        );
    }

    #[test]
    fn single_listen_uses_playback_start_timestamp() {
        let value =
            serde_json::to_value(submission("single", &message(), Some(1_700_000_000))).unwrap();
        assert_eq!(value["listen_type"], "single");
        assert_eq!(value["payload"][0]["listened_at"], 1_700_000_000);
    }
}
