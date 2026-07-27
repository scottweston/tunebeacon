use crate::domain::{PlaybackStatus, Player};

#[must_use]
pub fn derive_player_key(desktop_entry: Option<&str>, bus_name: &str) -> String {
    if let Some(entry) = desktop_entry
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
    {
        return entry.trim_end_matches(".desktop").to_ascii_lowercase();
    }
    bus_name
        .strip_prefix("org.mpris.MediaPlayer2.")
        .unwrap_or(bus_name)
        .split('.')
        .next()
        .unwrap_or(bus_name)
        .to_ascii_lowercase()
}

#[must_use]
pub fn select_player<'a>(players: &'a [Player], allowlist: &[String]) -> Option<&'a Player> {
    allowlist.iter().find_map(|key| {
        players.iter().find(|player| {
            player.key.eq_ignore_ascii_case(key)
                && player.status == PlaybackStatus::Playing
                && player
                    .track
                    .as_ref()
                    .is_some_and(crate::domain::RawTrack::is_publishable)
        })
    })
}

#[derive(Debug, Default)]
pub struct PublicationGate {
    last_published: Option<(String, crate::domain::TrackIdentity)>,
}

impl PublicationGate {
    #[must_use]
    pub fn should_publish(&self, player: &Player) -> bool {
        let Some(track) = player.track.as_ref() else {
            return false;
        };
        self.last_published
            .as_ref()
            .is_none_or(|last| last != &(player.key.clone(), track.identity()))
    }

    pub fn mark_published(&mut self, player: &Player) {
        if let Some(track) = &player.track {
            self.last_published = Some((player.key.clone(), track.identity()));
        }
    }

    /// Idle is deliberately not published, but clears deduplication so resume is observable.
    pub fn observe_idle(&mut self) {
        self.last_published = None;
    }
}
