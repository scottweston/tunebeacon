use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub enum PlaybackStatus {
    Playing,
    Paused,
    Stopped,
    #[default]
    Unknown,
}

#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct RawTrack {
    pub title: String,
    pub artists: Vec<String>,
    pub album: String,
    pub duration_ms: Option<u64>,
    pub art_url: Option<String>,
    pub track_id: Option<String>,
    pub observed_at: DateTime<Utc>,
}

impl RawTrack {
    #[must_use]
    pub fn identity(&self) -> TrackIdentity {
        TrackIdentity {
            title: self.title.clone(),
            artists: self.artists.clone(),
            album: self.album.clone(),
            duration_ms: self.duration_ms,
            track_id: self.track_id.clone(),
        }
    }

    #[must_use]
    pub fn is_publishable(&self) -> bool {
        !self.title.trim().is_empty() && !self.artists.is_empty()
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct TrackIdentity {
    pub title: String,
    pub artists: Vec<String>,
    pub album: String,
    pub duration_ms: Option<u64>,
    pub track_id: Option<String>,
}

#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct Player {
    pub key: String,
    pub identity: String,
    pub bus_name: String,
    pub desktop_entry: Option<String>,
    pub status: PlaybackStatus,
    pub track: Option<RawTrack>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum VerificationStatus {
    Pending,
    Verified,
    NotFound,
    Ambiguous,
    Unavailable,
}

impl std::fmt::Display for VerificationStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::Pending => "pending",
            Self::Verified => "verified",
            Self::NotFound => "not_found",
            Self::Ambiguous => "ambiguous",
            Self::Unavailable => "unavailable",
        };
        f.write_str(value)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct Verification {
    pub status: VerificationStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recording_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub release_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub release_group_id: Option<String>,
}

impl Verification {
    #[must_use]
    pub const fn failed(status: VerificationStatus) -> Self {
        Self {
            status,
            score: None,
            recording_id: None,
            release_id: None,
            release_group_id: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct PublishedPlayer {
    pub key: String,
    pub identity: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct NowPlayingMessage {
    pub schema_version: u8,
    pub observed_at: DateTime<Utc>,
    pub track: String,
    pub artists: Vec<String>,
    pub album: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub art_url: Option<String>,
    pub player: PublishedPlayer,
    pub verification: Verification,
}

impl NowPlayingMessage {
    #[must_use]
    pub fn new(player: &Player, track: &RawTrack, verification: Verification) -> Self {
        let art_url = track
            .art_url
            .as_ref()
            .filter(|value| !value.starts_with("file://"))
            .cloned()
            .or_else(|| {
                verification
                    .release_group_id
                    .as_ref()
                    .map(|id| format!("https://coverartarchive.org/release-group/{id}/front"))
            });
        Self {
            schema_version: 1,
            observed_at: track.observed_at,
            track: track.title.clone(),
            artists: track.artists.clone(),
            album: track.album.clone(),
            duration_ms: track.duration_ms,
            art_url,
            player: PublishedPlayer {
                key: player.key.clone(),
                identity: player.identity.clone(),
            },
            verification,
        }
    }
}
