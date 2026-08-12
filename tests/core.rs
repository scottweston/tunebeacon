use std::fs;

use chrono::{TimeZone, Utc};
use tempfile::tempdir;
use tunebeacon::{
    config::{CONFIG_VERSION, Config},
    domain::{
        NowPlayingMessage, PlaybackStatus, Player, RawTrack, Verification, VerificationStatus,
    },
    selection::{PublicationGate, derive_player_key, select_player},
    verification::{Candidate, match_candidates, normalize},
};

fn track() -> RawTrack {
    RawTrack {
        title: "Hoppípolla".to_owned(),
        artists: vec!["Sigur Rós".to_owned()],
        album: "Takk...".to_owned(),
        duration_ms: Some(268_000),
        art_url: Some("file:///tmp/private-cover.jpg".to_owned()),
        track_id: Some("/org/mpris/MediaPlayer2/Track/1".to_owned()),
        observed_at: Utc.with_ymd_and_hms(2026, 7, 27, 10, 15, 30).unwrap(),
    }
}

fn player(key: &str, status: PlaybackStatus) -> Player {
    Player {
        key: key.to_owned(),
        identity: key.to_uppercase(),
        bus_name: format!("org.mpris.MediaPlayer2.{key}.instance42"),
        desktop_entry: Some(key.to_owned()),
        status,
        track: Some(track()),
    }
}

fn candidate(score: u8) -> Candidate {
    Candidate {
        title: "Hoppipolla".to_owned(),
        artists: vec!["Sigur Ros".to_owned()],
        albums: vec!["Takk...".to_owned()],
        duration_ms: Some(268_200),
        score,
        recording_id: format!("recording-{score}"),
        release_id: Some("release-id".to_owned()),
        release_group_id: Some("release-group-id".to_owned()),
    }
}

#[test]
fn defaults_are_private_and_conservative() {
    let config = Config::default();
    assert_eq!(config.version, CONFIG_VERSION);
    assert!(config.players.allowlist.is_empty());
    assert!(!config.verification.publish_unverified);
    assert!(!config.mqtt.enabled);
    assert!(!config.webhook.enabled);
    assert!(!config.lastfm.enabled);
    assert!(config.lastfm.api_key.is_empty());
    assert!(config.lastfm.shared_secret.is_empty());
    assert!(config.lastfm.session_key.is_empty());
    assert!(config.lastfm.username.is_empty());
    assert!(!config.listenbrainz.enabled);
    assert!(config.listenbrainz.token.is_empty());
    assert!(config.listenbrainz.username.is_empty());
    assert!(config.webhook.url.is_empty());
    assert!(config.webhook.bearer_token.is_none());
    assert!(!config.mqtt.retain);
    assert_eq!(config.mqtt.qos, 1);
    assert!(config.mqtt.topic.starts_with("tunebeacon/"));
    assert!(config.mqtt.topic.ends_with("/now-playing"));
}

#[test]
fn daemon_accepts_lastfm_as_an_independent_output_when_authorized() {
    let mut config = Config::default();
    config.lastfm.enabled = true;
    assert!(config.validate_daemon().is_err());
    config.lastfm.api_key = "api-key".to_owned();
    config.lastfm.shared_secret = "shared-secret".to_owned();
    assert!(config.validate_daemon().is_err());
    config.lastfm.session_key = "session-key".to_owned();
    config.lastfm.username = "listener".to_owned();
    assert!(config.validate_daemon().is_ok());
}

#[test]
fn daemon_accepts_listenbrainz_as_an_independent_output_with_a_token() {
    let mut config = Config::default();
    config.listenbrainz.enabled = true;
    assert!(config.validate_daemon().is_err());
    config.listenbrainz.token = "user-token".to_owned();
    assert!(config.validate_daemon().is_ok());
}

#[test]
fn example_configuration_matches_the_current_schema() {
    let config: Config = toml::from_str(include_str!("../config.example.toml")).unwrap();
    config.validate().unwrap();
}

#[test]
fn daemon_requires_complete_mqtt_configuration() {
    let mut config = Config::default();
    assert!(config.validate_daemon().is_err());
    config.mqtt.enabled = true;
    assert!(config.validate_daemon().is_err());
    config.mqtt.host = "broker.example".to_owned();
    assert!(config.validate_daemon().is_ok());
    config.mqtt.qos = 3;
    assert!(config.validate().is_err());
}

#[test]
fn daemon_accepts_webhook_as_an_independent_output() {
    let mut config = Config::default();
    config.webhook.enabled = true;
    assert!(config.validate_daemon().is_err());
    config.webhook.url = "https://example.test/now-playing".to_owned();
    assert!(config.validate_daemon().is_ok());
    config.webhook.url = "file:///tmp/private.json".to_owned();
    assert!(config.validate_daemon().is_err());

    config.webhook.enabled = false;
    config.mqtt.enabled = true;
    config.mqtt.host = "broker.example".to_owned();
    assert!(config.validate_daemon().is_ok());

    config.webhook.enabled = true;
    config.webhook.url = "not a URL".to_owned();
    assert!(
        config.validate_daemon().is_err(),
        "every enabled output must be complete"
    );
}

#[test]
fn configuration_save_is_atomic_and_owner_only() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("nested/config.toml");
    let mut config = Config::default();
    config.players.allowlist.push("spotify".to_owned());
    config.save(&path).unwrap();
    assert_eq!(Config::load(&path).unwrap(), config);
    assert!(fs::read_dir(path.parent().unwrap()).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains(".tmp-")
    }));

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(
            fs::metadata(path.parent().unwrap())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
    }
}

#[test]
fn stable_player_key_prefers_desktop_entry() {
    assert_eq!(
        derive_player_key(
            Some("org.spotify.Client.desktop"),
            "org.mpris.MediaPlayer2.spotify.instance42"
        ),
        "org.spotify.client"
    );
    assert_eq!(
        derive_player_key(None, "org.mpris.MediaPlayer2.vlc.instance901"),
        "vlc"
    );
}

#[test]
fn selection_obeys_allowlist_priority_and_playback() {
    let players = vec![
        player("spotify", PlaybackStatus::Playing),
        player("amberol", PlaybackStatus::Playing),
        player("vlc", PlaybackStatus::Playing),
    ];
    let selected = select_player(&players, &["amberol".to_owned(), "spotify".to_owned()]).unwrap();
    assert_eq!(selected.key, "amberol");
    assert!(select_player(&players, &[]).is_none());
    assert!(select_player(&players, &["mpv".to_owned()]).is_none());
}

#[test]
fn pause_clears_deduplication_so_resume_republishes() {
    let playing = player("spotify", PlaybackStatus::Playing);
    let mut gate = PublicationGate::default();
    assert!(gate.should_publish(&playing));
    gate.mark_published(&playing);
    assert!(!gate.should_publish(&playing));
    gate.observe_idle();
    assert!(gate.should_publish(&playing));
}

#[test]
fn matching_requires_confident_title_and_artist_but_not_release_details() {
    let verification = match_candidates(&track(), &[candidate(100)]);
    assert_eq!(verification.status, VerificationStatus::Verified);
    assert_eq!(verification.score, Some(100));

    let mut wrong_duration_and_album = candidate(100);
    wrong_duration_and_album.albums = vec!["Ágætis byrjun".to_owned()];
    wrong_duration_and_album.duration_ms = Some(300_000);
    assert_eq!(
        match_candidates(&track(), &[wrong_duration_and_album.clone()]).status,
        VerificationStatus::Verified
    );

    wrong_duration_and_album.artists = vec!["A different artist".to_owned()];
    assert_eq!(
        match_candidates(&track(), &[wrong_duration_and_album]).status,
        VerificationStatus::NotFound
    );

    let low_confidence = candidate(89);
    assert_eq!(
        match_candidates(&track(), &[low_confidence]).status,
        VerificationStatus::NotFound
    );
}

#[test]
fn matching_handles_multiple_artists_and_duplicate_recordings() {
    let mut raw = track();
    raw.artists = vec!["Beyoncé".to_owned(), "Jay-Z".to_owned()];
    let mut first = candidate(96);
    first.title.clone_from(&raw.title);
    first.artists = vec!["Beyonce".to_owned(), "JAY Z".to_owned()];
    let mut second = first.clone();
    second.score = 92;
    second.recording_id = "runner-up".to_owned();
    first.duration_ms = Some(266_000);
    second.duration_ms = Some(270_000);
    let duplicate_result = match_candidates(&raw, &[first.clone(), second]);
    assert_eq!(duplicate_result.status, VerificationStatus::Verified);
    assert_eq!(
        duplicate_result.recording_id.as_deref(),
        Some(first.recording_id.as_str())
    );
    first.score = 100;
    assert_eq!(
        match_candidates(&raw, &[first]).status,
        VerificationStatus::Verified
    );
}

#[test]
fn matching_prefers_album_and_duration_for_live_spotify_fixture() {
    let raw = RawTrack {
        title: "Wrong".to_owned(),
        artists: vec!["Everything But The Girl".to_owned()],
        album: "Walking Wounded (Deluxe Edition)".to_owned(),
        duration_ms: Some(276_693),
        ..RawTrack::default()
    };
    let candidates = [
        Candidate {
            title: "Wrong".to_owned(),
            artists: vec!["Everything but the Girl".to_owned()],
            albums: vec!["100 Hits: 90s Anthems".to_owned()],
            duration_ms: Some(277_146),
            score: 100,
            recording_id: "compilation-near-duration".to_owned(),
            release_id: Some("compilation-release".to_owned()),
            release_group_id: Some("compilation-group".to_owned()),
        },
        Candidate {
            title: "Wrong".to_owned(),
            artists: vec!["Everything but the Girl".to_owned()],
            albums: vec!["90s: 120 Original Hits".to_owned()],
            duration_ms: Some(276_693),
            score: 100,
            recording_id: "compilation-exact-duration".to_owned(),
            release_id: Some("other-release".to_owned()),
            release_group_id: Some("other-group".to_owned()),
        },
        Candidate {
            title: "Wrong".to_owned(),
            artists: vec!["Everything but the Girl".to_owned()],
            albums: vec!["Walking Wounded".to_owned(), "Wrong".to_owned()],
            duration_ms: Some(276_693),
            score: 100,
            recording_id: "original-album-recording".to_owned(),
            release_id: Some("walking-wounded-release".to_owned()),
            release_group_id: Some("walking-wounded-group".to_owned()),
        },
    ];
    let verification = match_candidates(&raw, &candidates);
    assert_eq!(verification.status, VerificationStatus::Verified);
    assert_eq!(
        verification.recording_id.as_deref(),
        Some("original-album-recording")
    );
}

#[test]
fn normalization_ignores_case_diacritics_and_punctuation() {
    assert_eq!(normalize("HOPPÍPOLLA!"), normalize("hoppipolla"));
    assert_eq!(normalize("Jay-Z"), normalize("jay z"));
}

#[test]
fn json_contract_is_flat_versioned_and_excludes_local_artwork() {
    let player = player("spotify", PlaybackStatus::Playing);
    let verification = Verification {
        status: VerificationStatus::Verified,
        score: Some(100),
        recording_id: Some("recording-id".to_owned()),
        release_id: Some("release-id".to_owned()),
        release_group_id: Some("release-group-id".to_owned()),
    };
    let value =
        serde_json::to_value(NowPlayingMessage::new(&player, &track(), verification)).unwrap();
    assert_eq!(value["schema_version"], 1);
    assert_eq!(value["track"], "Hoppípolla");
    assert_eq!(value["artists"], serde_json::json!(["Sigur Rós"]));
    assert_eq!(value["verification"]["status"], "verified");
    assert_eq!(
        value["art_url"],
        "https://coverartarchive.org/release-group/release-group-id/front"
    );
    assert!(value.get("utc timestamp").is_none());
}

#[test]
fn optional_json_fields_are_omitted() {
    let mut raw = track();
    raw.duration_ms = None;
    raw.art_url = None;
    let verification = Verification::failed(VerificationStatus::Unavailable);
    let mut player = player("spotify", PlaybackStatus::Playing);
    player.track = Some(raw.clone());
    let value = serde_json::to_value(NowPlayingMessage::new(&player, &raw, verification)).unwrap();
    assert!(value.get("duration_ms").is_none());
    assert!(value.get("art_url").is_none());
    assert!(value["verification"].get("score").is_none());
}
