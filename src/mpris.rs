use std::collections::HashMap;

use anyhow::{Context, Result};
use chrono::Utc;
use futures_util::StreamExt;
use tokio::sync::mpsc;
use zbus::{
    Connection, MatchRule, MessageStream, Proxy,
    message::Type,
    zvariant::{ObjectPath, OwnedValue},
};

use crate::{
    domain::{PlaybackStatus, Player, RawTrack},
    selection::derive_player_key,
};

const PREFIX: &str = "org.mpris.MediaPlayer2.";
const PATH: &str = "/org/mpris/MediaPlayer2";

#[derive(Debug)]
pub struct MprisMonitor {
    connection: Connection,
    changes: mpsc::Receiver<()>,
}

impl MprisMonitor {
    /// Connect to the current Linux desktop's D-Bus session.
    ///
    /// # Errors
    ///
    /// Returns an error when no session bus is available.
    pub async fn connect() -> Result<Self> {
        let connection = Connection::session()
            .await
            .context("could not connect to the D-Bus session bus")?;
        let properties_rule = MatchRule::builder()
            .msg_type(Type::Signal)
            .interface("org.freedesktop.DBus.Properties")?
            .member("PropertiesChanged")?
            .path(PATH)?
            .build();
        let names_rule = MatchRule::builder()
            .msg_type(Type::Signal)
            .interface("org.freedesktop.DBus")?
            .member("NameOwnerChanged")?
            .build();
        let mut properties =
            MessageStream::for_match_rule(properties_rule, &connection, Some(32)).await?;
        let mut names = MessageStream::for_match_rule(names_rule, &connection, Some(32)).await?;
        let (change_sender, changes) = mpsc::channel(1);
        tokio::spawn(async move {
            loop {
                let received = tokio::select! {
                    message = properties.next() => message,
                    message = names.next() => message,
                };
                if received.is_none() {
                    break;
                }
                let _ = change_sender.try_send(());
            }
        });
        Ok(Self {
            connection,
            changes,
        })
    }

    /// Wait for a player property or D-Bus ownership change.
    pub async fn changed(&mut self) {
        if self.changes.recv().await.is_none() {
            std::future::pending::<()>().await;
        }
        while self.changes.try_recv().is_ok() {}
    }

    /// Returns a coherent snapshot. The runtime calls this after a 250 ms
    /// signal debounce; `list_names` also naturally handles players joining
    /// and leaving.
    ///
    /// # Errors
    ///
    /// Returns an error when D-Bus discovery fails. Individual players that
    /// disappear mid-read are skipped.
    pub async fn discover(&self) -> Result<Vec<Player>> {
        let dbus = zbus::fdo::DBusProxy::new(&self.connection).await?;
        let names = dbus.list_names().await?;
        let mut players = Vec::new();
        for name in names {
            let name = name.as_str();
            if !name.starts_with(PREFIX) {
                continue;
            }
            match read_player(&self.connection, name).await {
                Ok(player) => players.push(player),
                Err(error) => {
                    tracing::debug!(%name, %error, "MPRIS player disappeared while reading");
                }
            }
        }
        players.sort_unstable_by(|left, right| left.key.cmp(&right.key));
        Ok(players)
    }
}

async fn read_player(connection: &Connection, bus_name: &str) -> Result<Player> {
    let root = Proxy::new(
        connection,
        bus_name.to_owned(),
        PATH,
        "org.mpris.MediaPlayer2",
    )
    .await?;
    let identity = root
        .get_property::<String>("Identity")
        .await
        .unwrap_or_else(|_| bus_name.trim_start_matches(PREFIX).to_owned());
    let desktop_entry = root.get_property::<String>("DesktopEntry").await.ok();

    let player_proxy = Proxy::new(
        connection,
        bus_name.to_owned(),
        PATH,
        "org.mpris.MediaPlayer2.Player",
    )
    .await?;
    let status = match player_proxy
        .get_property::<String>("PlaybackStatus")
        .await
        .as_deref()
    {
        Ok("Playing") => PlaybackStatus::Playing,
        Ok("Paused") => PlaybackStatus::Paused,
        Ok("Stopped") => PlaybackStatus::Stopped,
        _ => PlaybackStatus::Unknown,
    };
    let metadata = player_proxy
        .get_property::<HashMap<String, OwnedValue>>("Metadata")
        .await
        .unwrap_or_default();
    let track = parse_metadata(&metadata);
    Ok(Player {
        key: derive_player_key(desktop_entry.as_deref(), bus_name),
        identity,
        bus_name: bus_name.to_owned(),
        desktop_entry,
        status,
        track,
    })
}

fn parse_metadata(metadata: &HashMap<String, OwnedValue>) -> Option<RawTrack> {
    let title = get_str(metadata, "xesam:title").unwrap_or_default();
    let artists = get_strings(metadata, "xesam:artist");
    if title.is_empty() && artists.is_empty() {
        return None;
    }
    Some(RawTrack {
        title,
        artists,
        album: get_str(metadata, "xesam:album").unwrap_or_default(),
        duration_ms: get_duration_ms(metadata),
        art_url: get_str(metadata, "mpris:artUrl"),
        track_id: get_object_path(metadata, "mpris:trackid"),
        observed_at: Utc::now(),
    })
}

fn get_str(metadata: &HashMap<String, OwnedValue>, key: &str) -> Option<String> {
    let value = metadata.get(key)?;
    <&str>::try_from(value).ok().map(str::to_owned)
}

fn get_strings(metadata: &HashMap<String, OwnedValue>, key: &str) -> Vec<String> {
    metadata
        .get(key)
        .and_then(|value| value.try_clone().ok())
        .and_then(|value| Vec::<String>::try_from(value).ok())
        .unwrap_or_default()
        .into_iter()
        .filter(|value| !value.trim().is_empty())
        .collect()
}

fn get_duration_ms(metadata: &HashMap<String, OwnedValue>) -> Option<u64> {
    let value = metadata.get("mpris:length")?;
    u64::try_from(value)
        .ok()
        .or_else(|| {
            i64::try_from(value)
                .ok()
                .and_then(|value| value.try_into().ok())
        })
        .map(|microseconds| microseconds / 1_000)
}

fn get_object_path(metadata: &HashMap<String, OwnedValue>, key: &str) -> Option<String> {
    metadata
        .get(key)
        .and_then(|value| <&ObjectPath<'_>>::try_from(value).ok())
        .map(ToString::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spotify_unsigned_duration_is_accepted() {
        let metadata =
            HashMap::from([("mpris:length".to_owned(), OwnedValue::from(276_693_000_u64))]);
        assert_eq!(get_duration_ms(&metadata), Some(276_693));
    }

    #[test]
    fn specification_signed_duration_is_accepted() {
        let metadata =
            HashMap::from([("mpris:length".to_owned(), OwnedValue::from(245_000_000_i64))]);
        assert_eq!(get_duration_ms(&metadata), Some(245_000));
    }
}
