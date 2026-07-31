use std::{
    fs::OpenOptions,
    io::Write,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

pub const CONFIG_VERSION: u8 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(default)]
pub struct Config {
    pub version: u8,
    pub players: PlayerConfig,
    pub verification: VerificationConfig,
    pub mqtt: MqttConfig,
    pub webhook: WebhookConfig,
    pub lastfm: LastFmConfig,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(default)]
pub struct PlayerConfig {
    /// Stable player keys in descending priority. Empty means no player is trusted.
    pub allowlist: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(default)]
pub struct VerificationConfig {
    /// Publish raw MPRIS data with an explicit failure status when verification fails.
    pub publish_unverified: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(default)]
pub struct MqttConfig {
    pub enabled: bool,
    pub host: String,
    pub port: u16,
    pub tls: bool,
    pub username: Option<String>,
    pub password: Option<String>,
    pub topic: String,
    pub qos: u8,
    pub retain: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(default)]
pub struct WebhookConfig {
    pub enabled: bool,
    pub url: String,
    pub bearer_token: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(default)]
pub struct LastFmConfig {
    pub enabled: bool,
    pub api_key: String,
    pub shared_secret: String,
    pub session_key: String,
    pub username: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            version: CONFIG_VERSION,
            players: PlayerConfig::default(),
            verification: VerificationConfig::default(),
            mqtt: MqttConfig::default(),
            webhook: WebhookConfig::default(),
            lastfm: LastFmConfig::default(),
        }
    }
}

impl Default for MqttConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            host: String::new(),
            port: 1883,
            tls: false,
            username: None,
            password: None,
            topic: default_topic(),
            qos: 1,
            retain: false,
        }
    }
}

#[must_use]
pub fn default_topic() -> String {
    let hostname = hostname::get()
        .ok()
        .and_then(|value| value.into_string().ok())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "unknown".to_owned());
    format!("tunebeacon/{hostname}/now-playing")
}

impl Config {
    /// Load and validate a configuration file, returning defaults when absent.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be read, parsed, or validated.
    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        let config: Self =
            toml::from_str(&text).with_context(|| format!("invalid {}", path.display()))?;
        config.validate()?;
        Ok(config)
    }

    /// Validate the configuration schema and all enabled adapters.
    ///
    /// # Errors
    ///
    /// Returns an error for unsupported versions or invalid MQTT `QoS`.
    pub fn validate(&self) -> Result<()> {
        if self.version != CONFIG_VERSION {
            bail!(
                "unsupported config version {} (expected {CONFIG_VERSION})",
                self.version
            );
        }
        if self.mqtt.qos > 2 {
            bail!("mqtt.qos must be 0, 1, or 2");
        }
        Ok(())
    }

    /// Validate settings required by headless daemon mode.
    ///
    /// # Errors
    ///
    /// Returns an error when no output is enabled or an enabled endpoint is
    /// incomplete.
    pub fn validate_daemon(&self) -> Result<()> {
        self.validate()?;
        if !self.mqtt.enabled && !self.webhook.enabled && !self.lastfm.enabled {
            bail!("daemon mode requires MQTT, webhook, or Last.fm to be enabled");
        }
        if self.mqtt.enabled {
            if self.mqtt.host.trim().is_empty() {
                bail!("daemon mode requires mqtt.host to be configured");
            }
            if self.mqtt.topic.trim().is_empty() {
                bail!("daemon mode requires mqtt.topic to be configured");
            }
            if self.mqtt.port == 0 {
                bail!("daemon mode requires a nonzero mqtt.port");
            }
        }
        if self.webhook.enabled {
            validate_webhook_url(&self.webhook.url)?;
        }
        if self.lastfm.enabled {
            validate_lastfm(&self.lastfm)?;
        }
        Ok(())
    }

    /// Atomically persist configuration with owner-only Unix permissions.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid configuration, serialization failures, or
    /// filesystem failures.
    pub fn save(&self, path: &Path) -> Result<()> {
        self.validate()?;
        let parent = path
            .parent()
            .context("configuration path has no parent directory")?;
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
        #[cfg(unix)]
        std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))?;
        let temporary = parent.join(format!(
            ".{}.tmp-{}",
            path.file_name()
                .and_then(|v| v.to_str())
                .unwrap_or("config"),
            std::process::id()
        ));
        let encoded = toml::to_string_pretty(self).context("failed to encode configuration")?;

        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut file = options
            .open(&temporary)
            .with_context(|| format!("failed to create {}", temporary.display()))?;
        file.write_all(encoded.as_bytes())?;
        file.sync_all()?;
        drop(file);
        #[cfg(unix)]
        std::fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o600))?;
        std::fs::rename(&temporary, path)
            .with_context(|| format!("failed to replace {}", path.display()))?;
        Ok(())
    }
}

/// Validate the credentials required for authenticated Last.fm calls.
///
/// # Errors
///
/// Returns an error when any application or session credential is missing.
pub fn validate_lastfm(config: &LastFmConfig) -> Result<()> {
    if config.api_key.trim().is_empty() {
        bail!("lastfm.api_key must be configured");
    }
    if config.shared_secret.trim().is_empty() {
        bail!("lastfm.shared_secret must be configured");
    }
    if config.session_key.trim().is_empty() {
        bail!("lastfm.session_key must be authorized");
    }
    Ok(())
}

/// Validate a webhook URL and require an HTTP transport.
///
/// # Errors
///
/// Returns an error for an empty, malformed, or non-HTTP(S) URL.
pub fn validate_webhook_url(value: &str) -> Result<url::Url> {
    let value = value.trim();
    if value.is_empty() {
        bail!("daemon mode requires webhook.url to be configured");
    }
    let parsed = url::Url::parse(value).context("webhook.url is invalid")?;
    if !matches!(parsed.scheme(), "http" | "https") {
        bail!("webhook.url must use http or https");
    }
    Ok(parsed)
}

#[must_use]
pub fn default_config_path() -> PathBuf {
    dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("tunebeacon")
        .join("config.toml")
}

#[must_use]
pub fn default_cache_dir() -> PathBuf {
    dirs::cache_dir()
        .unwrap_or_else(|| PathBuf::from(".cache"))
        .join("tunebeacon")
}
