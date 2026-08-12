use std::{sync::Arc, time::Instant};

use anyhow::Result;
use tokio::sync::{RwLock, mpsc};
use tokio_util::sync::CancellationToken;

use crate::{
    config::{Config, LastFmConfig, ListenBrainzConfig, MqttConfig, WebhookConfig},
    domain::{NowPlayingMessage, Player, TrackIdentity, Verification, VerificationStatus},
    lastfm::{LastFmPublisher, LastFmStatus, PlaybackTracker, ScrobbleAction},
    listenbrainz::{ListenBrainzPublisher, ListenBrainzStatus},
    mpris::MprisMonitor,
    mqtt::{MqttPublisher, MqttStatus},
    selection::{PublicationGate, select_player},
    verification::Verifier,
    webhook::{WebhookPublisher, WebhookStatus},
};

#[derive(Debug, Clone, Default)]
pub struct RuntimeState {
    pub players: Vec<Player>,
    pub selected: Option<Player>,
    pub verification: Option<Verification>,
    pub mqtt: MqttStatus,
    pub webhook: WebhookStatus,
    pub lastfm: LastFmStatus,
    pub listenbrainz: ListenBrainzStatus,
    pub error: Option<String>,
}

pub struct Runtime {
    pub config: Arc<RwLock<Config>>,
    pub state: Arc<RwLock<RuntimeState>>,
    shutdown: CancellationToken,
}

struct VerifiedResult {
    generation: u64,
    player: Player,
    verification: Verification,
}

enum LoopEvent {
    Refresh,
    Verified(Box<VerifiedResult>),
    Shutdown,
}

struct Outputs {
    mqtt_config: MqttConfig,
    mqtt: Option<MqttPublisher>,
    mqtt_gate: PublicationGate,
    webhook_config: WebhookConfig,
    webhook: Option<WebhookPublisher>,
    webhook_gate: PublicationGate,
    lastfm_config: LastFmConfig,
    lastfm: Option<LastFmPublisher>,
    listenbrainz_config: ListenBrainzConfig,
    listenbrainz: Option<ListenBrainzPublisher>,
}

#[derive(Debug, Default)]
struct OutputChanges {
    any: bool,
    lastfm: bool,
    listenbrainz: bool,
}

impl Outputs {
    async fn new(config: &RwLock<Config>) -> Self {
        let current = config.read().await;
        let mqtt_config = current.mqtt.clone();
        let mqtt = mqtt_config
            .enabled
            .then(|| MqttPublisher::spawn(mqtt_config.clone()));
        let webhook_config = current.webhook.clone();
        let webhook = webhook_config
            .enabled
            .then(|| WebhookPublisher::spawn(webhook_config.clone()));
        let lastfm_config = current.lastfm.clone();
        let lastfm = lastfm_config
            .enabled
            .then(|| LastFmPublisher::spawn(lastfm_config.clone()));
        let listenbrainz_config = current.listenbrainz.clone();
        let listenbrainz = listenbrainz_config
            .enabled
            .then(|| ListenBrainzPublisher::spawn(listenbrainz_config.clone()));
        Self {
            mqtt_config,
            mqtt,
            mqtt_gate: PublicationGate::default(),
            webhook_config,
            webhook,
            webhook_gate: PublicationGate::default(),
            lastfm_config,
            lastfm,
            listenbrainz_config,
            listenbrainz,
        }
    }

    async fn refresh(&mut self, config: &RwLock<Config>) -> OutputChanges {
        let latest = config.read().await;
        let mut changes = OutputChanges::default();
        if latest.mqtt != self.mqtt_config {
            self.mqtt_config.clone_from(&latest.mqtt);
            self.mqtt = self
                .mqtt_config
                .enabled
                .then(|| MqttPublisher::spawn(self.mqtt_config.clone()));
            self.mqtt_gate.observe_idle();
            changes.any = true;
        }
        if latest.webhook != self.webhook_config {
            self.webhook_config.clone_from(&latest.webhook);
            self.webhook = self
                .webhook_config
                .enabled
                .then(|| WebhookPublisher::spawn(self.webhook_config.clone()));
            self.webhook_gate.observe_idle();
            changes.any = true;
        }
        if latest.lastfm != self.lastfm_config {
            self.lastfm_config.clone_from(&latest.lastfm);
            self.lastfm = self
                .lastfm_config
                .enabled
                .then(|| LastFmPublisher::spawn(self.lastfm_config.clone()));
            changes.any = true;
            changes.lastfm = true;
        }
        if latest.listenbrainz != self.listenbrainz_config {
            self.listenbrainz_config.clone_from(&latest.listenbrainz);
            self.listenbrainz = self
                .listenbrainz_config
                .enabled
                .then(|| ListenBrainzPublisher::spawn(self.listenbrainz_config.clone()));
            changes.any = true;
            changes.listenbrainz = true;
        }
        changes
    }

    fn clear_if_idle(&mut self) {
        self.mqtt_gate.observe_idle();
        self.webhook_gate.observe_idle();
        if let Some(mqtt) = &self.mqtt {
            mqtt.clear_if_idle();
        }
        if let Some(webhook) = &self.webhook {
            webhook.clear_if_idle();
        }
    }

    fn publish(&mut self, player: &Player, message: NowPlayingMessage) {
        if let Some(mqtt) = &self.mqtt
            && self.mqtt_gate.should_publish(player)
        {
            mqtt.publish_latest(message.clone());
            self.mqtt_gate.mark_published(player);
        }
        if let Some(webhook) = &self.webhook
            && self.webhook_gate.should_publish(player)
        {
            webhook.publish_latest(message);
            self.webhook_gate.mark_published(player);
        }
    }

    fn submit_lastfm(&self, actions: impl IntoIterator<Item = ScrobbleAction>) {
        if let Some(lastfm) = &self.lastfm {
            for action in actions {
                lastfm.submit(action);
            }
        }
    }

    fn submit_listenbrainz(&self, actions: impl IntoIterator<Item = ScrobbleAction>) {
        if let Some(listenbrainz) = &self.listenbrainz {
            for action in actions {
                listenbrainz.submit(action);
            }
        }
    }
}

impl Runtime {
    #[must_use]
    pub fn spawn(config: Config, cache_dir: std::path::PathBuf) -> Self {
        let config = Arc::new(RwLock::new(config));
        let state = Arc::new(RwLock::new(RuntimeState::default()));
        let shutdown = CancellationToken::new();
        tokio::spawn(run_loop(
            Arc::clone(&config),
            Arc::clone(&state),
            shutdown.clone(),
            cache_dir,
        ));
        Self {
            config,
            state,
            shutdown,
        }
    }

    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }
}

async fn run_loop(
    config: Arc<RwLock<Config>>,
    state: Arc<RwLock<RuntimeState>>,
    shutdown: CancellationToken,
    cache_dir: std::path::PathBuf,
) {
    let mut monitor = match MprisMonitor::connect().await {
        Ok(monitor) => monitor,
        Err(error) => {
            state.write().await.error = Some(format!("{error:#}"));
            return;
        }
    };
    let verifier = Arc::new(Verifier::new(cache_dir));
    let mut outputs = Outputs::new(&config).await;
    let mut lastfm_tracker = PlaybackTracker::default();
    let mut listenbrainz_tracker = PlaybackTracker::default();
    let (verified_tx, mut verified_rx) = mpsc::channel::<VerifiedResult>(1);
    let mut active: Option<(String, TrackIdentity)> = None;
    let mut generation = 0_u64;
    let mut verification_cancel = CancellationToken::new();
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        let event = next_loop_event(&shutdown, &mut interval, &mut monitor, &mut verified_rx).await;
        match event {
            LoopEvent::Shutdown => break,
            LoopEvent::Refresh => {
                let changes = outputs.refresh(&config).await;
                reset_changed_trackers(&changes, &mut lastfm_tracker, &mut listenbrainz_tracker);
                if changes.any {
                    // Re-evaluate the still-playing track so enabling or fixing an
                    // output does not require the user to change tracks.
                    active = None;
                }
                match monitor.discover().await {
                    Ok(players) => {
                        let allowlist = config.read().await.players.allowlist.clone();
                        let selected = select_player(&players, &allowlist).cloned();
                        let now = Instant::now();
                        let actions = playback_actions(
                            &mut lastfm_tracker,
                            outputs.lastfm.is_some(),
                            selected.as_ref(),
                            now,
                        );
                        outputs.submit_lastfm(actions);
                        let actions = playback_actions(
                            &mut listenbrainz_tracker,
                            outputs.listenbrainz.is_some(),
                            selected.as_ref(),
                            now,
                        );
                        outputs.submit_listenbrainz(actions);
                        let next = selected.as_ref().and_then(|player| {
                            player
                                .track
                                .as_ref()
                                .map(|track| (player.key.clone(), track.identity()))
                        });
                        update_state(
                            &state,
                            players,
                            selected.as_ref(),
                            outputs.mqtt.as_ref(),
                            outputs.webhook.as_ref(),
                            outputs.lastfm.as_ref(),
                            outputs.listenbrainz.as_ref(),
                        )
                        .await;
                        if next != active {
                            verification_cancel.cancel();
                            verification_cancel = CancellationToken::new();
                            generation = generation.wrapping_add(1);
                            active = next;
                            state.write().await.verification = None;
                            if let Some(player) = selected {
                                spawn_verification(
                                    Arc::clone(&verifier),
                                    player,
                                    generation,
                                    verification_cancel.clone(),
                                    verified_tx.clone(),
                                );
                            } else {
                                outputs.clear_if_idle();
                            }
                        }
                    }
                    Err(error) => state.write().await.error = Some(format!("{error:#}")),
                }
            }
            LoopEvent::Verified(result) => {
                handle_verified(
                    *result,
                    generation,
                    &config,
                    &state,
                    &mut outputs,
                    &mut lastfm_tracker,
                    &mut listenbrainz_tracker,
                )
                .await;
            }
        }
    }
}

fn reset_changed_trackers(
    changes: &OutputChanges,
    lastfm: &mut PlaybackTracker,
    listenbrainz: &mut PlaybackTracker,
) {
    if changes.lastfm {
        lastfm.reset();
    }
    if changes.listenbrainz {
        listenbrainz.reset();
    }
}

fn playback_actions(
    tracker: &mut PlaybackTracker,
    enabled: bool,
    selected: Option<&Player>,
    now: Instant,
) -> Vec<ScrobbleAction> {
    if !enabled {
        tracker.reset();
        return Vec::new();
    }
    match selected {
        Some(player) => tracker.observe(Some(player), now),
        None => tracker.pause(now),
    }
}

async fn next_loop_event(
    shutdown: &CancellationToken,
    interval: &mut tokio::time::Interval,
    monitor: &mut MprisMonitor,
    verified_rx: &mut mpsc::Receiver<VerifiedResult>,
) -> LoopEvent {
    tokio::select! {
        () = shutdown.cancelled() => LoopEvent::Shutdown,
        _ = interval.tick() => LoopEvent::Refresh,
        () = monitor.changed() => {
            // Players often emit title, artist, album, and length separately.
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            LoopEvent::Refresh
        }
        Some(result) = verified_rx.recv() => LoopEvent::Verified(Box::new(result)),
    }
}

async fn update_state(
    state: &RwLock<RuntimeState>,
    players: Vec<Player>,
    selected: Option<&Player>,
    mqtt: Option<&MqttPublisher>,
    webhook: Option<&WebhookPublisher>,
    lastfm: Option<&LastFmPublisher>,
    listenbrainz: Option<&ListenBrainzPublisher>,
) {
    let mqtt_status = if let Some(mqtt) = mqtt {
        mqtt.status().await
    } else {
        MqttStatus::default()
    };
    let webhook_status = if let Some(webhook) = webhook {
        webhook.status().await
    } else {
        WebhookStatus::default()
    };
    let lastfm_status = if let Some(lastfm) = lastfm {
        lastfm.status().await
    } else {
        LastFmStatus::default()
    };
    let listenbrainz_status = if let Some(listenbrainz) = listenbrainz {
        listenbrainz.status().await
    } else {
        ListenBrainzStatus::default()
    };
    let mut current = state.write().await;
    current.players = players;
    current.selected = selected.cloned();
    current.error = None;
    current.mqtt = mqtt_status;
    current.webhook = webhook_status;
    current.lastfm = lastfm_status;
    current.listenbrainz = listenbrainz_status;
}

async fn handle_verified(
    result: VerifiedResult,
    generation: u64,
    config: &RwLock<Config>,
    state: &RwLock<RuntimeState>,
    outputs: &mut Outputs,
    lastfm_tracker: &mut PlaybackTracker,
    listenbrainz_tracker: &mut PlaybackTracker,
) {
    if result.generation != generation {
        return;
    }
    state.write().await.verification = Some(result.verification.clone());
    let publish_unverified = config.read().await.verification.publish_unverified;
    let eligible = result.verification.status == VerificationStatus::Verified
        || (publish_unverified
            && matches!(
                result.verification.status,
                VerificationStatus::NotFound
                    | VerificationStatus::Ambiguous
                    | VerificationStatus::Unavailable
            ));
    if eligible && let Some(track) = &result.player.track {
        let message = NowPlayingMessage::new(&result.player, track, result.verification.clone());
        outputs.publish(&result.player, message);
        let actions =
            lastfm_tracker.authorize(&result.player, result.verification.clone(), Instant::now());
        outputs.submit_lastfm(actions);
        let actions =
            listenbrainz_tracker.authorize(&result.player, result.verification, Instant::now());
        outputs.submit_listenbrainz(actions);
    }
}

fn spawn_verification(
    verifier: Arc<Verifier>,
    player: Player,
    generation: u64,
    cancellation: CancellationToken,
    sender: mpsc::Sender<VerifiedResult>,
) {
    tokio::spawn(async move {
        // MPRIS clients commonly emit title, artists, and album as separate changes.
        tokio::select! {
            () = cancellation.cancelled() => return,
            () = tokio::time::sleep(std::time::Duration::from_millis(250)) => {}
        }
        let Some(track) = player.track.as_ref() else {
            return;
        };
        let verification = tokio::select! {
            () = cancellation.cancelled() => return,
            result = verifier.verify(track) => result,
        };
        let _ = sender
            .send(VerifiedResult {
                generation,
                player,
                verification,
            })
            .await;
    });
}

/// Run `TuneBeacon` headlessly until SIGINT or SIGTERM.
///
/// # Errors
///
/// Returns an error when daemon settings are incomplete or signal handlers
/// cannot be installed.
pub async fn run_daemon(config: Config, cache_dir: std::path::PathBuf) -> Result<()> {
    config.validate_daemon()?;
    let runtime = Runtime::spawn(config, cache_dir);
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result?,
            _ = terminate.recv() => {},
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;
    runtime.shutdown();
    Ok(())
}
