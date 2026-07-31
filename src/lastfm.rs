use std::{
    collections::BTreeMap,
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use reqwest::{Client, StatusCode, redirect::Policy};
use serde_json::Value;
use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;

use crate::{
    USER_AGENT,
    config::{LastFmConfig, validate_lastfm},
    domain::{NowPlayingMessage, Player, TrackIdentity, Verification},
};

const API_ENDPOINT: &str = "https://ws.audioscrobbler.com/2.0/";
const AUTH_ENDPOINT: &str = "https://www.last.fm/api/auth/";
const MAX_SCROBBLE_DELAY: Duration = Duration::from_mins(4);

#[derive(Debug, Clone, Default)]
pub struct LastFmStatus {
    pub authenticated: bool,
    pub detail: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Authorization {
    pub token: String,
    pub url: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AuthorizedSession {
    pub username: String,
    pub session_key: String,
}

#[derive(Debug, Clone)]
pub(crate) enum ScrobbleAction {
    NowPlaying(NowPlayingMessage),
    Scrobble {
        message: NowPlayingMessage,
        started_at: i64,
    },
}

pub struct LastFmPublisher {
    sender: mpsc::UnboundedSender<ScrobbleAction>,
    status: Arc<Mutex<LastFmStatus>>,
    shutdown: CancellationToken,
}

impl LastFmPublisher {
    #[must_use]
    pub fn spawn(config: LastFmConfig) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        let status = Arc::new(Mutex::new(LastFmStatus {
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

    pub async fn status(&self) -> LastFmStatus {
        self.status.lock().await.clone()
    }
}

impl Drop for LastFmPublisher {
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

struct LastFmApi {
    client: Client,
    endpoint: String,
    config: LastFmConfig,
}

impl LastFmApi {
    fn new(config: LastFmConfig) -> std::result::Result<Self, ApiFailure> {
        validate_lastfm(&config).map_err(|error| ApiFailure {
            kind: FailureKind::Permanent,
            message: format!("{error:#}"),
        })?;
        let client = api_client().map_err(|error| ApiFailure {
            kind: FailureKind::Permanent,
            message: format!("{error:#}"),
        })?;
        Ok(Self {
            client,
            endpoint: API_ENDPOINT.to_owned(),
            config,
        })
    }

    async fn now_playing(
        &self,
        message: &NowPlayingMessage,
    ) -> std::result::Result<(), ApiFailure> {
        let mut fields = track_fields(message);
        fields.push(("method".to_owned(), "track.updateNowPlaying".to_owned()));
        self.authenticated_call(fields).await.map(|_| ())
    }

    async fn scrobble(
        &self,
        message: &NowPlayingMessage,
        started_at: i64,
    ) -> std::result::Result<(), ApiFailure> {
        let mut fields = track_fields(message);
        fields.push(("method".to_owned(), "track.scrobble".to_owned()));
        fields.push(("timestamp".to_owned(), started_at.to_string()));
        let response = self.authenticated_call(fields).await?;
        let accepted = response
            .pointer("/scrobbles/@attr/accepted")
            .and_then(|value| {
                value
                    .as_u64()
                    .or_else(|| value.as_str().and_then(|value| value.parse::<u64>().ok()))
            })
            .unwrap_or(0);
        if accepted == 0 {
            let ignored = response
                .pointer("/scrobbles/scrobble/ignoredMessage/#text")
                .or_else(|| response.pointer("/scrobbles/scrobble/ignoredmessage/#text"))
                .and_then(Value::as_str)
                .unwrap_or("Last.fm ignored the scrobble");
            return Err(ApiFailure {
                kind: FailureKind::Permanent,
                message: ignored.to_owned(),
            });
        }
        Ok(())
    }

    async fn authenticated_call(
        &self,
        mut fields: Vec<(String, String)>,
    ) -> std::result::Result<Value, ApiFailure> {
        fields.push(("api_key".to_owned(), self.config.api_key.clone()));
        fields.push(("sk".to_owned(), self.config.session_key.clone()));
        let signature = api_signature(&fields, &self.config.shared_secret);
        fields.push(("api_sig".to_owned(), signature));
        fields.push(("format".to_owned(), "json".to_owned()));
        call(&self.client, &self.endpoint, &fields).await
    }
}

async fn run_submissions(
    config: LastFmConfig,
    mut receiver: mpsc::UnboundedReceiver<ScrobbleAction>,
    status: Arc<Mutex<LastFmStatus>>,
    shutdown: CancellationToken,
) {
    let api = match LastFmApi::new(config) {
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
                let Some(action) = action else {
                    return;
                };
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
                        result = api.scrobble(&message, started_at) => result,
                    };
                    match result {
                        Ok(()) => {
                            set_status(&status, Ok(()), "scrobbled").await;
                            break;
                        }
                        Err(error) if matches!(error.kind, FailureKind::Retryable) => {
                            let mut current = status.lock().await;
                            current.detail = format!("retrying scrobble: {error}");
                            drop(current);
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
    status: &Mutex<LastFmStatus>,
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

fn track_fields(message: &NowPlayingMessage) -> Vec<(String, String)> {
    let mut fields = vec![
        (
            "artist".to_owned(),
            message.artists.first().cloned().unwrap_or_default(),
        ),
        ("track".to_owned(), message.track.clone()),
    ];
    if !message.album.trim().is_empty() {
        fields.push(("album".to_owned(), message.album.clone()));
    }
    if let Some(duration_ms) = message.duration_ms {
        fields.push(("duration".to_owned(), (duration_ms / 1_000).to_string()));
    }
    if let Some(recording_id) = &message.verification.recording_id {
        fields.push(("mbid".to_owned(), recording_id.clone()));
    }
    fields
}

fn api_client() -> Result<Client> {
    Client::builder()
        .user_agent(USER_AGENT)
        .timeout(Duration::from_secs(10))
        .redirect(Policy::none())
        .build()
        .context("failed to create Last.fm HTTP client")
}

async fn call(
    client: &Client,
    endpoint: &str,
    fields: &[(String, String)],
) -> std::result::Result<Value, ApiFailure> {
    let response = client
        .post(endpoint)
        .form(fields)
        .send()
        .await
        .map_err(|error| ApiFailure {
            kind: FailureKind::Retryable,
            message: format!("Last.fm request failed: {error}"),
        })?;
    let status = response.status();
    let body = response.text().await.map_err(|error| ApiFailure {
        kind: FailureKind::Retryable,
        message: format!("failed to read Last.fm response: {error}"),
    })?;
    let value: Value = serde_json::from_str(&body).map_err(|error| ApiFailure {
        kind: if status.is_server_error() {
            FailureKind::Retryable
        } else {
            FailureKind::Permanent
        },
        message: format!("invalid Last.fm response (HTTP {status}): {error}"),
    })?;
    if let Some(code) = value.get("error").and_then(Value::as_i64) {
        let message = value
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("Last.fm API error");
        let kind = match code {
            9 => FailureKind::Authentication,
            11 | 16 => FailureKind::Retryable,
            _ => FailureKind::Permanent,
        };
        return Err(ApiFailure {
            kind,
            message: format!("Last.fm error {code}: {message}"),
        });
    }
    if status != StatusCode::OK {
        return Err(ApiFailure {
            kind: if status.is_server_error() {
                FailureKind::Retryable
            } else {
                FailureKind::Permanent
            },
            message: format!("Last.fm returned HTTP {status}"),
        });
    }
    Ok(value)
}

fn api_signature(fields: &[(String, String)], shared_secret: &str) -> String {
    let sorted: BTreeMap<&str, &str> = fields
        .iter()
        .filter(|(name, _)| !matches!(name.as_str(), "api_sig" | "callback" | "format"))
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect();
    let mut input = String::new();
    for (name, value) in sorted {
        input.push_str(name);
        input.push_str(value);
    }
    input.push_str(shared_secret);
    format!("{:x}", md5::compute(input))
}

/// Request a short-lived desktop authorization token and its browser URL.
///
/// # Errors
///
/// Returns an error when application credentials are missing or Last.fm cannot
/// issue a token.
pub async fn begin_authorization(config: &LastFmConfig) -> Result<Authorization> {
    if config.api_key.trim().is_empty() || config.shared_secret.trim().is_empty() {
        bail!("enter the Last.fm API key and shared secret first");
    }
    let client = api_client()?;
    let mut fields = vec![
        ("api_key".to_owned(), config.api_key.clone()),
        ("method".to_owned(), "auth.getToken".to_owned()),
    ];
    let signature = api_signature(&fields, &config.shared_secret);
    fields.push(("api_sig".to_owned(), signature));
    fields.push(("format".to_owned(), "json".to_owned()));
    let response = call(&client, API_ENDPOINT, &fields)
        .await
        .map_err(anyhow::Error::new)?;
    let token = response
        .get("token")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .context("Last.fm token response was incomplete")?
        .to_owned();
    let mut url = url::Url::parse(AUTH_ENDPOINT)?;
    url.query_pairs_mut()
        .append_pair("api_key", config.api_key.trim())
        .append_pair("token", &token);
    Ok(Authorization {
        token,
        url: url.into(),
    })
}

/// Exchange a user-approved desktop token for a durable session.
///
/// # Errors
///
/// Returns an error when Last.fm has not approved the token or the response is
/// incomplete.
pub async fn complete_authorization(
    config: &LastFmConfig,
    token: &str,
) -> Result<AuthorizedSession> {
    let client = api_client()?;
    let mut fields = vec![
        ("api_key".to_owned(), config.api_key.clone()),
        ("method".to_owned(), "auth.getSession".to_owned()),
        ("token".to_owned(), token.to_owned()),
    ];
    let signature = api_signature(&fields, &config.shared_secret);
    fields.push(("api_sig".to_owned(), signature));
    fields.push(("format".to_owned(), "json".to_owned()));
    let response = call(&client, API_ENDPOINT, &fields)
        .await
        .map_err(anyhow::Error::new)?;
    let session = response
        .get("session")
        .context("Last.fm session response was incomplete")?;
    let username = session
        .get("name")
        .and_then(Value::as_str)
        .context("Last.fm session response did not include an account name")?
        .to_owned();
    let session_key = session
        .get("key")
        .and_then(Value::as_str)
        .context("Last.fm session response did not include a session key")?
        .to_owned();
    Ok(AuthorizedSession {
        username,
        session_key,
    })
}

#[derive(Debug, Default)]
pub(crate) struct PlaybackTracker {
    active: Option<TrackedPlayback>,
}

#[derive(Debug)]
struct TrackedPlayback {
    player_key: String,
    identity: TrackIdentity,
    started_at: i64,
    accumulated: Duration,
    playing_since: Option<Instant>,
    message: Option<NowPlayingMessage>,
    now_playing_sent: bool,
    scrobbled: bool,
}

impl PlaybackTracker {
    pub(crate) fn reset(&mut self) {
        self.active = None;
    }

    pub(crate) fn observe(&mut self, player: Option<&Player>, now: Instant) -> Vec<ScrobbleAction> {
        let next = player.and_then(|player| {
            player
                .track
                .as_ref()
                .map(|track| (player, track.identity()))
        });
        let same = next.as_ref().is_some_and(|(player, identity)| {
            self.active.as_ref().is_some_and(|active| {
                active.player_key == player.key && active.identity == *identity
            })
        });
        if same {
            let active = self.active.as_mut().expect("active playback checked above");
            if active.playing_since.is_none() {
                active.playing_since = Some(now);
            }
            active.update_elapsed(now);
            return active.take_due_scrobble().into_iter().collect();
        }

        let mut actions = Vec::new();
        if let Some(mut active) = self.active.take() {
            active.update_elapsed(now);
            if let Some(action) = active.take_due_scrobble() {
                actions.push(action);
            }
        }
        if let Some((player, identity)) = next
            && let Some(track) = &player.track
        {
            self.active = Some(TrackedPlayback {
                player_key: player.key.clone(),
                identity,
                started_at: track.observed_at.timestamp(),
                accumulated: Duration::ZERO,
                playing_since: Some(now),
                message: None,
                now_playing_sent: false,
                scrobbled: false,
            });
        }
        actions
    }

    pub(crate) fn pause(&mut self, now: Instant) -> Vec<ScrobbleAction> {
        let Some(active) = &mut self.active else {
            return Vec::new();
        };
        active.update_elapsed(now);
        active.playing_since = None;
        active.take_due_scrobble().into_iter().collect()
    }

    pub(crate) fn authorize(
        &mut self,
        player: &Player,
        verification: Verification,
        now: Instant,
    ) -> Vec<ScrobbleAction> {
        let Some(track) = &player.track else {
            return Vec::new();
        };
        let Some(active) = &mut self.active else {
            return Vec::new();
        };
        if active.player_key != player.key || active.identity != track.identity() {
            return Vec::new();
        }
        active.update_elapsed(now);
        if active.message.is_none() {
            active.message = Some(NowPlayingMessage::new(player, track, verification));
        }
        let mut actions = Vec::new();
        if !active.now_playing_sent {
            active.now_playing_sent = true;
            if let Some(message) = &active.message {
                actions.push(ScrobbleAction::NowPlaying(message.clone()));
            }
        }
        if let Some(action) = active.take_due_scrobble() {
            actions.push(action);
        }
        actions
    }
}

impl TrackedPlayback {
    fn update_elapsed(&mut self, now: Instant) {
        if let Some(since) = self.playing_since {
            self.accumulated += now.saturating_duration_since(since);
            self.playing_since = Some(now);
        }
    }

    fn take_due_scrobble(&mut self) -> Option<ScrobbleAction> {
        if self.scrobbled {
            return None;
        }
        let message = self.message.as_ref()?;
        let duration = Duration::from_millis(message.duration_ms?);
        if duration <= Duration::from_secs(30) {
            return None;
        }
        let threshold = (duration / 2).min(MAX_SCROBBLE_DELAY);
        if self.accumulated < threshold {
            return None;
        }
        self.scrobbled = true;
        Some(ScrobbleAction::Scrobble {
            message: message.clone(),
            started_at: self.started_at,
        })
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::*;
    use crate::domain::{PlaybackStatus, RawTrack, VerificationStatus};

    fn player(duration_ms: u64) -> Player {
        Player {
            key: "player".to_owned(),
            identity: "Player".to_owned(),
            bus_name: "org.mpris.MediaPlayer2.player".to_owned(),
            desktop_entry: None,
            status: PlaybackStatus::Playing,
            track: Some(RawTrack {
                title: "Track".to_owned(),
                artists: vec!["Artist".to_owned()],
                album: "Album".to_owned(),
                duration_ms: Some(duration_ms),
                art_url: None,
                track_id: Some("one".to_owned()),
                observed_at: Utc.with_ymd_and_hms(2026, 7, 31, 1, 2, 3).unwrap(),
            }),
        }
    }

    fn verification() -> Verification {
        Verification {
            status: VerificationStatus::Verified,
            score: Some(100),
            recording_id: Some("recording-id".to_owned()),
            release_id: None,
            release_group_id: None,
        }
    }

    #[test]
    fn signature_sorts_parameters_and_omits_format() {
        let fields = vec![
            ("token".to_owned(), "xxxxxxx".to_owned()),
            ("format".to_owned(), "json".to_owned()),
            ("method".to_owned(), "auth.getSession".to_owned()),
            ("api_key".to_owned(), "xxxxxxxx".to_owned()),
        ];
        assert_eq!(
            api_signature(&fields, "mysecret"),
            format!(
                "{:x}",
                md5::compute("api_keyxxxxxxxxmethodauth.getSessiontokenxxxxxxxmysecret")
            )
        );
    }

    #[test]
    fn tracker_scrobbles_after_half_the_track_and_counts_only_playing_time() {
        let player = player(180_000);
        let start = Instant::now();
        let mut tracker = PlaybackTracker::default();
        assert!(tracker.observe(Some(&player), start).is_empty());
        let actions = tracker.authorize(&player, verification(), start + Duration::from_secs(10));
        assert!(matches!(
            actions.as_slice(),
            [ScrobbleAction::NowPlaying(_)]
        ));
        assert!(tracker.pause(start + Duration::from_secs(45)).is_empty());
        assert!(
            tracker
                .observe(Some(&player), start + Duration::from_mins(2))
                .is_empty()
        );
        let actions = tracker.observe(Some(&player), start + Duration::from_secs(165));
        assert!(matches!(
            actions.as_slice(),
            [ScrobbleAction::Scrobble { .. }]
        ));
        assert!(
            tracker
                .observe(Some(&player), start + Duration::from_mins(5))
                .is_empty()
        );
    }

    #[test]
    fn tracker_never_scrobbles_tracks_that_are_not_longer_than_thirty_seconds() {
        let player = player(30_000);
        let start = Instant::now();
        let mut tracker = PlaybackTracker::default();
        tracker.observe(Some(&player), start);
        tracker.authorize(&player, verification(), start);
        assert!(
            tracker
                .observe(Some(&player), start + Duration::from_mins(1))
                .is_empty()
        );
    }

    #[test]
    fn four_minutes_caps_the_scrobble_threshold() {
        let player = player(900_000);
        let start = Instant::now();
        let mut tracker = PlaybackTracker::default();
        tracker.observe(Some(&player), start);
        tracker.authorize(&player, verification(), start);
        let actions = tracker.observe(Some(&player), start + Duration::from_mins(4));
        assert!(matches!(
            actions.as_slice(),
            [ScrobbleAction::Scrobble { .. }]
        ));
    }
}
