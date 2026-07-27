use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Result;
use crossterm::event::{Event, EventStream, KeyCode, KeyEvent, KeyModifiers};
use futures_util::StreamExt;
use image::ImageReader;
use ratatui::{
    DefaultTerminal, Frame,
    layout::{Alignment, Constraint, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, List, ListItem, Paragraph, Tabs, Wrap},
};
use ratatui_image::{
    StatefulImage,
    picker::Picker,
    thread::{ResizeRequest, ResizeResponse, ThreadProtocol},
};
use tokio::sync::{RwLock, mpsc::UnboundedReceiver};
use tui_input::{Input, backend::crossterm::EventHandler};

use crate::{
    artwork::cached_artwork,
    config::Config,
    domain::PlaybackStatus,
    mqtt,
    runtime::{Runtime, RuntimeState},
    webhook,
};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum Tab {
    NowPlaying,
    Players,
    Mqtt,
    Webhook,
    Verification,
}

impl Tab {
    const ALL: [Self; 5] = [
        Self::NowPlaying,
        Self::Players,
        Self::Mqtt,
        Self::Webhook,
        Self::Verification,
    ];

    const fn index(self) -> usize {
        match self {
            Self::NowPlaying => 0,
            Self::Players => 1,
            Self::Mqtt => 2,
            Self::Webhook => 3,
            Self::Verification => 4,
        }
    }
}

struct ArtworkState {
    protocol: ThreadProtocol,
    requests: UnboundedReceiver<ResizeRequest>,
}

struct App {
    tab: Tab,
    config: Config,
    config_path: PathBuf,
    cache_dir: PathBuf,
    runtime_config: Arc<RwLock<Config>>,
    runtime_state: Arc<RwLock<RuntimeState>>,
    state: RuntimeState,
    player_cursor: usize,
    mqtt_field: usize,
    mqtt_input: Input,
    webhook_field: usize,
    webhook_input: Input,
    editing: bool,
    message: String,
    message_expires_at: Option<Instant>,
    artwork_url: Option<String>,
    artwork: Option<ArtworkState>,
    artwork_sender: tokio::sync::mpsc::UnboundedSender<(String, Result<PathBuf, String>)>,
    artwork_results: UnboundedReceiver<(String, Result<PathBuf, String>)>,
    encoding_sender: tokio::sync::mpsc::UnboundedSender<(String, Result<ResizeResponse, String>)>,
    encoding_results: UnboundedReceiver<(String, Result<ResizeResponse, String>)>,
    picker: Picker,
}

/// Run the interactive terminal interface until the user quits or a signal arrives.
///
/// # Errors
///
/// Returns an error when terminal setup, input handling, rendering, or config
/// persistence fails.
pub async fn run(config: Config, config_path: PathBuf, cache_dir: PathBuf) -> Result<()> {
    let picker = Picker::from_query_stdio().unwrap_or_else(|_| Picker::halfblocks());
    let runtime = Runtime::spawn(config.clone(), cache_dir.clone());
    let (artwork_sender, artwork_results) = tokio::sync::mpsc::unbounded_channel();
    let (encoding_sender, encoding_results) = tokio::sync::mpsc::unbounded_channel();
    let previous_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic| {
        ratatui::restore();
        previous_hook(panic);
    }));
    let terminal = ratatui::init();
    let result = App {
        tab: Tab::NowPlaying,
        config,
        config_path,
        cache_dir,
        runtime_config: Arc::clone(&runtime.config),
        runtime_state: Arc::clone(&runtime.state),
        state: RuntimeState::default(),
        player_cursor: 0,
        mqtt_field: 0,
        mqtt_input: Input::default(),
        webhook_field: 0,
        webhook_input: Input::default(),
        editing: false,
        message: String::new(),
        message_expires_at: None,
        artwork_url: None,
        artwork: None,
        artwork_sender,
        artwork_results,
        encoding_sender,
        encoding_results,
        picker,
    }
    .run(terminal)
    .await;
    runtime.shutdown();
    ratatui::restore();
    result
}

impl App {
    async fn run(mut self, mut terminal: DefaultTerminal) -> Result<()> {
        let mut events = EventStream::new();
        let mut tick = tokio::time::interval(Duration::from_millis(100));
        loop {
            self.state = self.runtime_state.read().await.clone();
            self.update_artwork();
            self.process_artwork_downloads();
            self.process_artwork_requests();
            self.expire_message();
            terminal.draw(|frame| self.draw(frame))?;
            tokio::select! {
                _ = tick.tick() => {}
                event = events.next() => {
                    let Some(event) = event else { break };
                    if let Event::Key(key) = event?
                        && self.handle_key(key).await?
                    {
                        break;
                    }
                }
                result = tokio::signal::ctrl_c() => {
                    result?;
                    break;
                }
            }
        }
        Ok(())
    }

    async fn handle_key(&mut self, key: KeyEvent) -> Result<bool> {
        if self.editing {
            match key.code {
                KeyCode::Esc | KeyCode::Enter => {
                    self.editing = false;
                }
                KeyCode::Char(character)
                    if self.tab == Tab::Mqtt
                        && self.mqtt_field == 4
                        && !character.is_ascii_digit()
                        && !key.modifiers.contains(KeyModifiers::CONTROL) => {}
                _ => {
                    let changed = match self.tab {
                        Tab::Mqtt => {
                            let changed = self.mqtt_input.handle_event(&Event::Key(key)).is_some();
                            if changed {
                                self.apply_mqtt_input();
                            }
                            changed
                        }
                        Tab::Webhook => {
                            let changed =
                                self.webhook_input.handle_event(&Event::Key(key)).is_some();
                            if changed {
                                self.apply_webhook_input();
                            }
                            changed
                        }
                        _ => false,
                    };
                    if changed {
                        self.sync_config().await;
                    }
                }
            }
            return Ok(false);
        }
        if matches!(key.code, KeyCode::Char('q')) {
            return Ok(true);
        }
        match key.code {
            KeyCode::Char('1') => self.tab = Tab::NowPlaying,
            KeyCode::Char('2') => self.tab = Tab::Players,
            KeyCode::Char('3') => self.tab = Tab::Mqtt,
            KeyCode::Char('4') => self.tab = Tab::Webhook,
            KeyCode::Char('5') => self.tab = Tab::Verification,
            KeyCode::Left => self.tab = Tab::ALL[(self.tab.index() + 4) % 5],
            KeyCode::Right => self.tab = Tab::ALL[(self.tab.index() + 1) % 5],
            KeyCode::Char('s') => self.save()?,
            _ => match self.tab {
                Tab::Players => self.handle_player_key(key).await,
                Tab::Mqtt => self.handle_mqtt_key(key).await?,
                Tab::Webhook => self.handle_webhook_key(key).await?,
                Tab::Verification => {
                    if matches!(key.code, KeyCode::Char('f' | ' ')) {
                        self.config.verification.publish_unverified =
                            !self.config.verification.publish_unverified;
                        self.sync_config().await;
                    }
                }
                Tab::NowPlaying => {}
            },
        }
        Ok(false)
    }

    async fn handle_player_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Up if key.modifiers.contains(KeyModifiers::SHIFT) => {
                self.move_priority(-1);
                self.sync_config().await;
            }
            KeyCode::Down if key.modifiers.contains(KeyModifiers::SHIFT) => {
                self.move_priority(1);
                self.sync_config().await;
            }
            KeyCode::Up => self.player_cursor = self.player_cursor.saturating_sub(1),
            KeyCode::Down => {
                self.player_cursor =
                    (self.player_cursor + 1).min(self.state.players.len().saturating_sub(1));
            }
            KeyCode::Char(' ' | 'a') => {
                if let Some(player) = self.state.players.get(self.player_cursor) {
                    if let Some(index) = self
                        .config
                        .players
                        .allowlist
                        .iter()
                        .position(|key| key == &player.key)
                    {
                        self.config.players.allowlist.remove(index);
                    } else {
                        self.config.players.allowlist.push(player.key.clone());
                    }
                    self.sync_config().await;
                }
            }
            _ => {}
        }
    }

    async fn handle_mqtt_key(&mut self, key: KeyEvent) -> Result<()> {
        match key.code {
            KeyCode::Up => self.mqtt_field = self.mqtt_field.saturating_sub(1),
            KeyCode::Down | KeyCode::Tab => self.mqtt_field = (self.mqtt_field + 1).min(4),
            KeyCode::Enter => {
                self.editing = true;
                self.mqtt_input = Input::from(self.mqtt_field_value());
            }
            KeyCode::Char('e') => {
                self.config.mqtt.enabled = !self.config.mqtt.enabled;
                self.sync_config().await;
            }
            KeyCode::Char('l') => {
                self.config.mqtt.tls = !self.config.mqtt.tls;
                self.sync_config().await;
            }
            KeyCode::Char('r') => {
                self.config.mqtt.retain = !self.config.mqtt.retain;
                self.sync_config().await;
            }
            KeyCode::Char('o') => {
                self.config.mqtt.qos = (self.config.mqtt.qos + 1) % 3;
                self.sync_config().await;
            }
            KeyCode::Char('c') => {
                let message = match mqtt::test_connection(&self.config.mqtt).await {
                    Ok(()) => "MQTT connection succeeded".to_owned(),
                    Err(error) => format!("MQTT test failed: {error:#}"),
                };
                self.show_message(message, Duration::from_secs(4));
            }
            _ => {}
        }
        Ok(())
    }

    fn mqtt_field_value(&self) -> String {
        match self.mqtt_field {
            0 => self.config.mqtt.host.clone(),
            1 => self.config.mqtt.username.clone().unwrap_or_default(),
            2 => self.config.mqtt.password.clone().unwrap_or_default(),
            3 => self.config.mqtt.topic.clone(),
            4 => self.config.mqtt.port.to_string(),
            _ => String::new(),
        }
    }

    fn apply_mqtt_input(&mut self) {
        let value = self.mqtt_input.value().to_owned();
        match self.mqtt_field {
            0 => self.config.mqtt.host = value,
            1 => self.config.mqtt.username = Some(value),
            2 => self.config.mqtt.password = Some(value),
            3 => self.config.mqtt.topic = value,
            4 => {
                if let Ok(port) = value.parse::<u16>() {
                    self.config.mqtt.port = port;
                }
            }
            _ => {}
        }
    }

    async fn handle_webhook_key(&mut self, key: KeyEvent) -> Result<()> {
        match key.code {
            KeyCode::Up => self.webhook_field = self.webhook_field.saturating_sub(1),
            KeyCode::Down | KeyCode::Tab => {
                self.webhook_field = (self.webhook_field + 1).min(1);
            }
            KeyCode::Enter => {
                self.editing = true;
                self.webhook_input = Input::from(self.webhook_field_value());
            }
            KeyCode::Char('e') => {
                self.config.webhook.enabled = !self.config.webhook.enabled;
                self.sync_config().await;
            }
            KeyCode::Char('c') => {
                let message = match webhook::test_connection(&self.config.webhook).await {
                    Ok(()) => "Webhook test POST succeeded".to_owned(),
                    Err(error) => format!("Webhook test failed: {error:#}"),
                };
                self.show_message(message, Duration::from_secs(4));
            }
            _ => {}
        }
        Ok(())
    }

    fn webhook_field_value(&self) -> String {
        match self.webhook_field {
            0 => self.config.webhook.url.clone(),
            1 => self.config.webhook.bearer_token.clone().unwrap_or_default(),
            _ => String::new(),
        }
    }

    fn apply_webhook_input(&mut self) {
        let value = self.webhook_input.value().to_owned();
        match self.webhook_field {
            0 => self.config.webhook.url = value,
            1 => self.config.webhook.bearer_token = Some(value),
            _ => {}
        }
    }

    fn move_priority(&mut self, direction: isize) {
        let Some(player) = self.state.players.get(self.player_cursor) else {
            return;
        };
        let Some(index) = self
            .config
            .players
            .allowlist
            .iter()
            .position(|key| key == &player.key)
        else {
            return;
        };
        let target = index.saturating_add_signed(direction);
        if target < self.config.players.allowlist.len() {
            self.config.players.allowlist.swap(index, target);
        }
    }

    async fn sync_config(&self) {
        *self.runtime_config.write().await = self.config.clone();
    }

    fn save(&mut self) -> Result<()> {
        self.config.save(&self.config_path)?;
        self.show_message(
            format!("saved {}", self.config_path.display()),
            Duration::from_secs(2),
        );
        Ok(())
    }

    fn show_message(&mut self, message: impl Into<String>, duration: Duration) {
        self.message = message.into();
        self.message_expires_at = Some(Instant::now() + duration);
    }

    fn expire_message(&mut self) {
        if self
            .message_expires_at
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            self.message.clear();
            self.message_expires_at = None;
        }
    }

    fn update_artwork(&mut self) {
        let next = self
            .state
            .selected
            .as_ref()
            .and_then(|player| player.track.as_ref())
            .and_then(|track| track.art_url.clone())
            .or_else(|| {
                self.state
                    .verification
                    .as_ref()
                    .and_then(|value| value.release_group_id.as_ref())
                    .map(|id| format!("https://coverartarchive.org/release-group/{id}/front"))
            });
        if next == self.artwork_url {
            return;
        }
        self.artwork_url.clone_from(&next);
        self.artwork = None;
        let Some(url) = next else {
            return;
        };
        let cache_dir = self.cache_dir.clone();
        let sender = self.artwork_sender.clone();
        tokio::spawn(async move {
            let result = cached_artwork(&cache_dir, &url)
                .await
                .map_err(|error| format!("{error:#}"));
            let _ = sender.send((url, result));
        });
    }

    fn process_artwork_downloads(&mut self) {
        while let Ok((url, result)) = self.artwork_results.try_recv() {
            if self.artwork_url.as_deref() != Some(&url) {
                continue;
            }
            match result {
                Ok(path) => {
                    match ImageReader::open(path).and_then(ImageReader::with_guessed_format) {
                        Ok(reader) => match reader.decode() {
                            Ok(image) => {
                                let protocol = self.picker.new_resize_protocol(image);
                                let (sender, requests) = tokio::sync::mpsc::unbounded_channel();
                                self.artwork = Some(ArtworkState {
                                    protocol: ThreadProtocol::new(sender, Some(protocol)),
                                    requests,
                                });
                            }
                            Err(error) => {
                                self.message = format!("artwork unavailable: {error}");
                                self.message_expires_at =
                                    Some(Instant::now() + Duration::from_secs(4));
                            }
                        },
                        Err(error) => {
                            self.message = format!("artwork unavailable: {error}");
                            self.message_expires_at = Some(Instant::now() + Duration::from_secs(4));
                        }
                    }
                }
                Err(error) => {
                    tracing::debug!(%error, "artwork unavailable");
                }
            }
        }
    }

    fn process_artwork_requests(&mut self) {
        let Some(artwork) = &mut self.artwork else {
            return;
        };
        while let Ok(request) = artwork.requests.try_recv() {
            let url = self.artwork_url.clone().unwrap_or_default();
            let sender = self.encoding_sender.clone();
            tokio::task::spawn_blocking(move || {
                let result = request.resize_encode().map_err(|error| format!("{error}"));
                let _ = sender.send((url, result));
            });
        }
        while let Ok((url, result)) = self.encoding_results.try_recv() {
            if self.artwork_url.as_deref() != Some(&url) {
                continue;
            }
            match result {
                Ok(response) => {
                    artwork.protocol.update_resized_protocol(response);
                }
                Err(error) => {
                    self.message = format!("artwork encoding failed: {error}");
                    self.message_expires_at = Some(Instant::now() + Duration::from_secs(4));
                }
            }
        }
    }

    fn draw(&mut self, frame: &mut Frame) {
        let areas = Layout::vertical([
            Constraint::Length(3),
            Constraint::Min(8),
            Constraint::Length(2),
        ])
        .split(frame.area());
        frame.render_widget(
            Tabs::new(["Now playing", "Players", "MQTT", "Webhook", "Verification"])
                .select(self.tab.index())
                .block(Block::bordered().title(" TuneBeacon "))
                .highlight_style(Style::default().fg(Color::Cyan).bold()),
            areas[0],
        );
        match self.tab {
            Tab::NowPlaying => self.draw_now_playing(frame, areas[1]),
            Tab::Players => self.draw_players(frame, areas[1]),
            Tab::Mqtt => self.draw_mqtt(frame, areas[1]),
            Tab::Webhook => self.draw_webhook(frame, areas[1]),
            Tab::Verification => self.draw_verification(frame, areas[1]),
        }
        let footer = if self.message.is_empty() {
            "1–5/←→ tabs  s save  q quit"
        } else {
            &self.message
        };
        frame.render_widget(
            Paragraph::new(footer)
                .style(Style::default().fg(Color::DarkGray))
                .alignment(Alignment::Center),
            areas[2],
        );
    }

    fn draw_now_playing(&mut self, frame: &mut Frame, area: Rect) {
        let columns = if area.width >= 70 {
            Layout::horizontal([Constraint::Percentage(40), Constraint::Percentage(60)]).split(area)
        } else {
            Layout::vertical([Constraint::Percentage(45), Constraint::Percentage(55)]).split(area)
        };
        let art_block = Block::bordered().title(" Artwork ");
        let art_inner = art_block.inner(columns[0]);
        frame.render_widget(art_block, columns[0]);
        if let Some(artwork) = &mut self.artwork {
            frame.render_stateful_widget(
                StatefulImage::default(),
                art_inner,
                &mut artwork.protocol,
            );
        } else {
            frame.render_widget(
                Paragraph::new("No artwork")
                    .alignment(Alignment::Center)
                    .style(Style::default().fg(Color::DarkGray)),
                art_inner,
            );
        }

        let lines = if let Some(player) = &self.state.selected {
            let track = player.track.as_ref();
            vec![
                detail("Track", track.map_or("—", |value| value.title.as_str())),
                detail(
                    "Artists",
                    &track.map_or_else(String::new, |value| value.artists.join(", ")),
                ),
                detail("Album", track.map_or("—", |value| value.album.as_str())),
                detail("Player", &format!("{} ({})", player.identity, player.key)),
                detail(
                    "Verification",
                    &self
                        .state
                        .verification
                        .as_ref()
                        .map_or_else(|| "pending".to_owned(), |value| value.status.to_string()),
                ),
                detail(
                    "MQTT",
                    if self.state.mqtt.connected {
                        "connected"
                    } else if self.config.mqtt.enabled {
                        &self.state.mqtt.detail
                    } else {
                        "disabled"
                    },
                ),
                detail(
                    "Webhook",
                    if self.state.webhook.delivered {
                        "delivered"
                    } else if self.config.webhook.enabled {
                        &self.state.webhook.detail
                    } else {
                        "disabled"
                    },
                ),
            ]
        } else {
            vec![
                Line::from("Nothing eligible is playing."),
                Line::from("Open Players and explicitly allow trusted music players."),
            ]
        };
        frame.render_widget(
            Paragraph::new(lines)
                .block(Block::bordered().title(" Metadata "))
                .wrap(Wrap { trim: true }),
            columns[1],
        );
    }

    fn draw_players(&self, frame: &mut Frame, area: Rect) {
        let items = if self.state.players.is_empty() {
            vec![ListItem::new(
                "No MPRIS players discovered in this D-Bus session.",
            )]
        } else {
            self.state
                .players
                .iter()
                .enumerate()
                .map(|(index, player)| {
                    let priority = self
                        .config
                        .players
                        .allowlist
                        .iter()
                        .position(|key| key == &player.key);
                    let allowed = priority.map_or_else(
                        || "[ ] denied".to_owned(),
                        |value| format!("[✓] priority {}", value + 1),
                    );
                    let status = match player.status {
                        PlaybackStatus::Playing => "playing",
                        PlaybackStatus::Paused => "paused",
                        PlaybackStatus::Stopped => "stopped",
                        PlaybackStatus::Unknown => "unknown",
                    };
                    let style = if index == self.player_cursor {
                        Style::default().fg(Color::Black).bg(Color::Cyan)
                    } else {
                        Style::default()
                    };
                    ListItem::new(format!(
                        "{allowed:>14}  {:<20} {:<10} {}",
                        player.key, status, player.identity
                    ))
                    .style(style)
                })
                .collect()
        };
        frame.render_widget(
            List::new(items).block(
                Block::bordered()
                    .title(" Discovered players — ↑/↓ select, Space allow, Shift+↑/↓ priority "),
            ),
            area,
        );
    }

    fn draw_mqtt(&self, frame: &mut Frame, area: Rect) {
        let block = Block::bordered().title(" MQTT settings ");
        let inner = block.inner(area);
        frame.render_widget(block, area);
        if inner.is_empty() {
            return;
        }
        frame.render_widget(
            Paragraph::new(format!(
                "Enabled: {}   TLS: {}   QoS: {}   Retain: {}",
                yes_no(self.config.mqtt.enabled),
                yes_no(self.config.mqtt.tls),
                self.config.mqtt.qos,
                yes_no(self.config.mqtt.retain)
            )),
            Rect::new(inner.x, inner.y, inner.width, 1),
        );

        let values = [
            ("Host", self.config.mqtt.host.clone()),
            (
                "Username",
                self.config.mqtt.username.clone().unwrap_or_default(),
            ),
            (
                "Password",
                self.config.mqtt.password.clone().unwrap_or_default(),
            ),
            ("Topic", self.config.mqtt.topic.clone()),
            ("Port", self.config.mqtt.port.to_string()),
        ];
        for (index, (label, configured_value)) in values.into_iter().enumerate() {
            self.draw_input_field(
                frame,
                inner,
                index,
                self.mqtt_field,
                label,
                &configured_value,
                index == 2,
                &self.mqtt_input,
            );
        }

        let help_y = inner.y.saturating_add(8);
        if help_y < inner.bottom() {
            frame.render_widget(
                Paragraph::new("↑/↓ field  Enter edit  e enable  l TLS  o QoS  r retain  c test"),
                Rect::new(inner.x, help_y, inner.width, 1),
            );
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn draw_input_field(
        &self,
        frame: &mut Frame,
        inner: Rect,
        index: usize,
        selected_field: usize,
        label: &str,
        configured_value: &str,
        secret: bool,
        input: &Input,
    ) {
        let y = inner
            .y
            .saturating_add(2)
            .saturating_add(u16::try_from(index).unwrap_or(u16::MAX));
        if y >= inner.bottom() {
            return;
        }
        let selected = index == selected_field;
        let style = if selected {
            Style::default().add_modifier(Modifier::REVERSED)
        } else {
            Style::default()
        };
        let row = Rect::new(inner.x, y, inner.width, 1);
        let label_width = inner.width.min(12);
        let label_area = Rect::new(row.x, row.y, label_width, 1);
        let value_area = Rect::new(
            row.x.saturating_add(label_width),
            row.y,
            row.width.saturating_sub(label_width),
            1,
        );
        let raw_value = if selected && self.editing {
            input.value()
        } else {
            configured_value
        };
        let displayed_value = if secret {
            "•".repeat(raw_value.chars().count())
        } else {
            raw_value.to_owned()
        };
        let visible_width = usize::from(value_area.width.saturating_sub(1));
        let (cursor, scroll) = if selected && self.editing {
            let cursor = if secret {
                input.cursor()
            } else {
                input.visual_cursor()
            };
            let scroll = if secret {
                cursor.max(visible_width) - visible_width
            } else {
                input.visual_scroll(visible_width)
            };
            (cursor, scroll)
        } else {
            (0, 0)
        };
        frame.render_widget(
            Paragraph::new(format!("{label:>10}: ")).style(style),
            label_area,
        );
        frame.render_widget(
            Paragraph::new(displayed_value)
                .style(style)
                .scroll((0, u16::try_from(scroll).unwrap_or(u16::MAX))),
            value_area,
        );
        if selected && self.editing && value_area.width > 0 {
            let cursor_x = value_area
                .x
                .saturating_add(u16::try_from(cursor.saturating_sub(scroll)).unwrap_or(u16::MAX))
                .min(value_area.right().saturating_sub(1));
            frame.set_cursor_position((cursor_x, value_area.y));
        }
    }

    fn draw_webhook(&self, frame: &mut Frame, area: Rect) {
        let block = Block::bordered().title(" Webhook settings ");
        let inner = block.inner(area);
        frame.render_widget(block, area);
        if inner.is_empty() {
            return;
        }
        frame.render_widget(
            Paragraph::new(format!(
                "Enabled: {}   Status: {}",
                yes_no(self.config.webhook.enabled),
                if self.config.webhook.enabled {
                    self.state.webhook.detail.as_str()
                } else {
                    "disabled"
                }
            )),
            Rect::new(inner.x, inner.y, inner.width, 1),
        );

        let values = [
            ("URL", self.config.webhook.url.clone()),
            (
                "Bearer",
                self.config.webhook.bearer_token.clone().unwrap_or_default(),
            ),
        ];
        for (index, (label, configured_value)) in values.into_iter().enumerate() {
            self.draw_input_field(
                frame,
                inner,
                index,
                self.webhook_field,
                label,
                &configured_value,
                index == 1,
                &self.webhook_input,
            );
        }

        let help_y = inner.y.saturating_add(5);
        if help_y < inner.bottom() {
            frame.render_widget(
                Paragraph::new("↑/↓ field  Enter edit  e enable  c send diagnostic test POST"),
                Rect::new(inner.x, help_y, inner.width, 1),
            );
        }
    }

    fn draw_verification(&self, frame: &mut Frame, area: Rect) {
        let (mode, mode_style) = if self.config.verification.publish_unverified {
            (
                "UNSAFE FALLBACK: unverified metadata may be published and can expose private names.",
                Style::default().fg(Color::Red).bold(),
            )
        } else {
            (
                "Privacy mode: only known MusicBrainz title-and-artist pairs are published.",
                Style::default().fg(Color::Cyan).bold(),
            )
        };
        frame.render_widget(
            Paragraph::new(vec![
                Line::from(mode).style(mode_style),
                Line::from(""),
                Line::from("A match requires score ≥ 90, an exact normalized title, and every"),
                Line::from("reported MPRIS artist to match a MusicBrainz credited artist."),
                Line::from("Album and duration select better enrichment IDs but never block a"),
                Line::from("known song merely because releases, remasters, or recordings differ."),
                Line::from(""),
                Line::from("Press f or Space to toggle the unsafe fallback override."),
            ])
            .block(Block::bordered().title(" MusicBrainz verification "))
            .wrap(Wrap { trim: true }),
            area,
        );
    }
}

fn detail(label: &str, value: &str) -> Line<'static> {
    Line::from(vec![
        Span::styled(format!("{label:>13}: "), Style::default().bold()),
        Span::raw(value.to_owned()),
    ])
}

const fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

#[cfg(test)]
mod tests {
    use ratatui::{Terminal, backend::TestBackend, layout::Position};

    use super::*;

    fn app(tab: Tab) -> App {
        let mut config = Config::default();
        config.mqtt.password = Some("do-not-render-this".to_owned());
        config.webhook.bearer_token = Some("do-not-render-token".to_owned());
        let (artwork_sender, artwork_results) = tokio::sync::mpsc::unbounded_channel();
        let (encoding_sender, encoding_results) = tokio::sync::mpsc::unbounded_channel();
        App {
            tab,
            config: config.clone(),
            config_path: PathBuf::from("/tmp/tunebeacon-test-config.toml"),
            cache_dir: PathBuf::from("/tmp/tunebeacon-test-cache"),
            runtime_config: Arc::new(RwLock::new(config)),
            runtime_state: Arc::new(RwLock::new(RuntimeState::default())),
            state: RuntimeState::default(),
            player_cursor: 0,
            mqtt_field: 0,
            mqtt_input: Input::default(),
            webhook_field: 0,
            webhook_input: Input::default(),
            editing: false,
            message: String::new(),
            message_expires_at: None,
            artwork_url: None,
            artwork: None,
            artwork_sender,
            artwork_results,
            encoding_sender,
            encoding_results,
            picker: Picker::halfblocks(),
        }
    }

    fn render(width: u16, height: u16, mut app: App) -> String {
        let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
        terminal.draw(|frame| app.draw(frame)).unwrap();
        terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(ratatui::buffer::Cell::symbol)
            .collect()
    }

    #[test]
    fn normal_and_narrow_layouts_render() {
        let normal = render(100, 30, app(Tab::NowPlaying));
        assert!(normal.contains("Nothing eligible is playing"));
        assert!(normal.contains("Artwork"));

        let narrow = render(45, 18, app(Tab::Verification));
        assert!(narrow.contains("Privacy mode"));
        assert!(narrow.contains("MusicBrainz"));
    }

    #[test]
    fn mqtt_layout_never_renders_password() {
        let rendered = render(90, 24, app(Tab::Mqtt));
        assert!(!rendered.contains("do-not-render-this"));
        assert!(rendered.contains("••••"));
    }

    #[test]
    fn webhook_layout_masks_token_and_highlights_empty_url() {
        let mut app = app(Tab::Webhook);
        app.config.webhook.url.clear();
        let mut terminal = Terminal::new(TestBackend::new(90, 24)).unwrap();
        terminal.draw(|frame| app.draw(frame)).unwrap();
        let rendered: String = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(ratatui::buffer::Cell::symbol)
            .collect();
        assert!(!rendered.contains("do-not-render-token"));
        assert!(rendered.contains("••••"));
        let selected_blank = terminal.backend().buffer().cell((20, 6)).unwrap();
        assert!(selected_blank.modifier.contains(Modifier::REVERSED));
    }

    #[tokio::test]
    async fn webhook_editor_supports_non_destructive_cursor_movement() {
        let mut app = app(Tab::Webhook);
        app.config.webhook.url = "https://example.test/hook".to_owned();
        app.handle_webhook_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE))
            .await
            .unwrap();
        app.handle_key(KeyEvent::new(KeyCode::Left, KeyModifiers::NONE))
            .await
            .unwrap();
        app.handle_key(KeyEvent::new(KeyCode::Char('X'), KeyModifiers::SHIFT))
            .await
            .unwrap();
        assert_eq!(app.config.webhook.url, "https://example.test/hooXk");
    }

    #[test]
    fn mqtt_editor_places_visible_cursor_after_input() {
        let mut app = app(Tab::Mqtt);
        app.editing = true;
        app.config.mqtt.host = "broker".to_owned();
        app.mqtt_input = Input::from("broker");
        let mut terminal = Terminal::new(TestBackend::new(90, 24)).unwrap();
        terminal.draw(|frame| app.draw(frame)).unwrap();
        assert!(terminal.backend().cursor_visible());
        assert_eq!(terminal.backend().cursor_position(), Position::new(19, 6));
    }

    #[test]
    fn empty_selected_field_uses_reverse_video() {
        let mut app = app(Tab::Mqtt);
        app.mqtt_field = 1;
        app.config.mqtt.username = None;
        let mut terminal = Terminal::new(TestBackend::new(90, 24)).unwrap();
        terminal.draw(|frame| app.draw(frame)).unwrap();
        let selected_blank = terminal.backend().buffer().cell((20, 7)).unwrap();
        assert!(selected_blank.modifier.contains(Modifier::REVERSED));
    }

    #[tokio::test]
    async fn mqtt_input_supports_non_destructive_cursor_movement() {
        let mut app = app(Tab::Mqtt);
        app.config.mqtt.host = "broker".to_owned();
        app.handle_mqtt_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE))
            .await
            .unwrap();
        app.handle_key(KeyEvent::new(KeyCode::Left, KeyModifiers::NONE))
            .await
            .unwrap();
        app.handle_key(KeyEvent::new(KeyCode::Left, KeyModifiers::NONE))
            .await
            .unwrap();
        app.handle_key(KeyEvent::new(KeyCode::Char('X'), KeyModifiers::SHIFT))
            .await
            .unwrap();
        assert_eq!(app.config.mqtt.host, "brokXer");
        assert_eq!(app.mqtt_input.cursor(), 5);
    }

    #[test]
    fn status_messages_expire() {
        let mut app = app(Tab::NowPlaying);
        app.show_message("saved test", Duration::ZERO);
        app.expire_message();
        assert!(app.message.is_empty());
        assert!(app.message_expires_at.is_none());
    }
}
