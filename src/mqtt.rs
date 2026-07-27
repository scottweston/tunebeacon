use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS, Transport};
use tokio::sync::{Mutex, watch};

use crate::{config::MqttConfig, domain::NowPlayingMessage};

#[derive(Debug, Clone, Default)]
pub struct MqttStatus {
    pub connected: bool,
    pub detail: String,
}

pub struct MqttPublisher {
    latest: watch::Sender<Option<NowPlayingMessage>>,
    status: Arc<Mutex<MqttStatus>>,
}

impl MqttPublisher {
    #[must_use]
    pub fn spawn(config: MqttConfig) -> Self {
        let (latest, receiver) = watch::channel(None);
        let status = Arc::new(Mutex::new(MqttStatus {
            connected: false,
            detail: "connecting".to_owned(),
        }));
        tokio::spawn(run_mqtt(config, receiver, Arc::clone(&status)));
        Self { latest, status }
    }

    pub fn publish_latest(&self, message: NowPlayingMessage) {
        self.latest.send_replace(Some(message));
    }

    pub fn clear_if_idle(&self) {
        self.latest.send_replace(None);
    }

    pub async fn status(&self) -> MqttStatus {
        self.status.lock().await.clone()
    }
}

async fn run_mqtt(
    config: MqttConfig,
    mut latest: watch::Receiver<Option<NowPlayingMessage>>,
    status: Arc<Mutex<MqttStatus>>,
) {
    let mut backoff = 1;
    loop {
        match connect_and_run(&config, &mut latest, &status).await {
            Ok(()) => return,
            Err(error) => {
                let mut current = status.lock().await;
                if current.connected {
                    backoff = 1;
                }
                current.connected = false;
                current.detail = format!("{error:#}");
                drop(current);
                tokio::time::sleep(Duration::from_secs(backoff)).await;
                backoff = (backoff * 2).min(60);
            }
        }
    }
}

async fn connect_and_run(
    config: &MqttConfig,
    latest: &mut watch::Receiver<Option<NowPlayingMessage>>,
    status: &Arc<Mutex<MqttStatus>>,
) -> Result<()> {
    let client_id = format!(
        "tunebeacon-{}-{}",
        hostname::get()
            .ok()
            .and_then(|name| name.into_string().ok())
            .unwrap_or_else(|| "linux".to_owned()),
        std::process::id()
    );
    let mut options = MqttOptions::new(client_id, &config.host, config.port);
    options.set_keep_alive(Duration::from_secs(30));
    options.set_clean_session(true);
    if let Some(username) = config.username.as_deref().filter(|value| !value.is_empty()) {
        options.set_credentials(username, config.password.as_deref().unwrap_or_default());
    }
    if config.tls {
        options.set_transport(Transport::tls_with_default_config());
    }
    let (client, mut eventloop) = AsyncClient::new(options, 4);
    let mut last_sent_observation = None;

    loop {
        tokio::select! {
            event = eventloop.poll() => {
                if let Event::Incoming(Packet::ConnAck(_)) =
                    event.context("MQTT connection failed")?
                {
                    let mut current = status.lock().await;
                    current.connected = true;
                    "connected".clone_into(&mut current.detail);
                    drop(current);
                    // Reconnect only sends if the currently selected track is still live.
                    let current_message = {
                        let borrowed = latest.borrow();
                        borrowed.clone()
                    };
                    if let Some(message) = current_message {
                        send(&client, config, &message).await?;
                        last_sent_observation = Some(message.observed_at);
                    }
                }
            }
            changed = latest.changed() => {
                if changed.is_err() {
                    return Ok(());
                }
                if status.lock().await.connected {
                    let current_message = {
                        let borrowed = latest.borrow();
                        borrowed.clone()
                    };
                    if let Some(message) = current_message {
                        if last_sent_observation != Some(message.observed_at) {
                            send(&client, config, &message).await?;
                            last_sent_observation = Some(message.observed_at);
                        }
                    } else {
                        last_sent_observation = None;
                    }
                }
            }
        }
    }
}

async fn send(
    client: &AsyncClient,
    config: &MqttConfig,
    message: &NowPlayingMessage,
) -> Result<()> {
    let qos = match config.qos {
        0 => QoS::AtMostOnce,
        1 => QoS::AtLeastOnce,
        2 => QoS::ExactlyOnce,
        _ => anyhow::bail!("invalid MQTT QoS"),
    };
    let payload = serde_json::to_vec(message)?;
    client
        .publish(&config.topic, qos, config.retain, payload)
        .await
        .context("failed to queue MQTT publication")
}

/// Open a short-lived connection and wait for the broker acknowledgement.
///
/// # Errors
///
/// Returns an error for invalid settings, transport errors, broker refusal, or
/// an eight-second timeout.
pub async fn test_connection(config: &MqttConfig) -> Result<()> {
    let client_id = format!("tunebeacon-test-{}", std::process::id());
    let mut options = MqttOptions::new(client_id, &config.host, config.port);
    options.set_keep_alive(Duration::from_secs(5));
    if let Some(username) = config.username.as_deref().filter(|value| !value.is_empty()) {
        options.set_credentials(username, config.password.as_deref().unwrap_or_default());
    }
    if config.tls {
        options.set_transport(Transport::tls_with_default_config());
    }
    let (_client, mut eventloop) = AsyncClient::new(options, 1);
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            if matches!(eventloop.poll().await?, Event::Incoming(Packet::ConnAck(_))) {
                return Ok::<_, rumqttc::ConnectionError>(());
            }
        }
    })
    .await
    .context("MQTT connection timed out")??;
    Ok(())
}
