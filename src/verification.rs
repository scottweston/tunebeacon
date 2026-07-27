use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime},
};

use anyhow::{Context, Result};
use musicbrainz_rs::{
    MusicBrainzClient, Search,
    entity::{artist_credit::ArtistCredit, recording::Recording},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use unicode_normalization::UnicodeNormalization;

use crate::{
    USER_AGENT,
    domain::{RawTrack, Verification, VerificationStatus},
};

const SUCCESS_TTL: Duration = Duration::from_hours(30 * 24);
const FAILURE_TTL: Duration = Duration::from_hours(1);
pub const CACHE_LIMIT_BYTES: u64 = 100 * 1024 * 1024;
const MATCHER_VERSION: u8 = 3;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Candidate {
    pub title: String,
    pub artists: Vec<String>,
    pub albums: Vec<String>,
    pub duration_ms: Option<u64>,
    pub score: u8,
    pub recording_id: String,
    pub release_id: Option<String>,
    pub release_group_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct CacheEntry {
    saved_at_unix: u64,
    verification: Verification,
}

pub struct Verifier {
    cache_dir: PathBuf,
    client: MusicBrainzClient,
    last_request: Arc<Mutex<Option<tokio::time::Instant>>>,
}

impl std::fmt::Debug for Verifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Verifier")
            .field("cache_dir", &self.cache_dir)
            .finish_non_exhaustive()
    }
}

impl Verifier {
    #[must_use]
    pub fn new(cache_dir: PathBuf) -> Self {
        Self {
            cache_dir,
            client: MusicBrainzClient::new(USER_AGENT),
            last_request: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn verify(&self, track: &RawTrack) -> Verification {
        if let Ok(Some(cached)) = self.read_cache(track).await {
            return cached;
        }

        self.rate_limit().await;
        let result = self.search(track).await.unwrap_or_else(|error| {
            tracing::warn!(%error, "MusicBrainz verification unavailable");
            Verification::failed(VerificationStatus::Unavailable)
        });
        if let Err(error) = self.write_cache(track, &result).await {
            tracing::warn!(%error, "could not update verification cache");
        }
        result
    }

    async fn rate_limit(&self) {
        let mut last = self.last_request.lock().await;
        if let Some(previous) = *last {
            let elapsed = previous.elapsed();
            if let Some(remaining) = Duration::from_secs(1).checked_sub(elapsed) {
                tokio::time::sleep(remaining).await;
            }
        }
        *last = Some(tokio::time::Instant::now());
    }

    async fn search(&self, track: &RawTrack) -> Result<Verification> {
        let artist = track.artists.join(" ");
        let query_string = format!(
            "recording:\"{}\" AND artistname:\"{}\"",
            lucene_escape(&track.title),
            lucene_escape(&artist)
        );
        let mut query = Recording::search(query_string);
        query.limit(10).with_releases();
        let result = query
            .execute_with_client_async(&self.client)
            .await
            .context("MusicBrainz recording search failed")?;
        let candidates = result
            .entities
            .into_iter()
            .map(candidate_from_recording)
            .collect::<Vec<_>>();
        Ok(match_candidates(track, &candidates))
    }

    fn cache_path(&self, track: &RawTrack) -> PathBuf {
        let mut hasher = Sha256::new();
        hasher.update([MATCHER_VERSION]);
        hasher.update(normalize(&track.title));
        for artist in &track.artists {
            hasher.update([0]);
            hasher.update(normalize(artist));
        }
        hasher.update([0]);
        hasher.update(normalize(&track.album));
        if let Some(duration) = track.duration_ms {
            hasher.update(duration.to_le_bytes());
        }
        self.cache_dir
            .join("verification")
            .join(format!("{:x}.json", hasher.finalize()))
    }

    async fn read_cache(&self, track: &RawTrack) -> Result<Option<Verification>> {
        let path = self.cache_path(track);
        let Ok(data) = tokio::fs::read(&path).await else {
            return Ok(None);
        };
        let entry: CacheEntry = serde_json::from_slice(&data)?;
        let now = unix_time(SystemTime::now());
        let age = now.saturating_sub(entry.saved_at_unix);
        let ttl = if entry.verification.status == VerificationStatus::Verified {
            SUCCESS_TTL
        } else {
            FAILURE_TTL
        };
        Ok((age < ttl.as_secs()).then_some(entry.verification))
    }

    async fn write_cache(&self, track: &RawTrack, verification: &Verification) -> Result<()> {
        let path = self.cache_path(track);
        let parent = path.parent().context("cache path has no parent")?;
        tokio::fs::create_dir_all(parent).await?;
        let entry = CacheEntry {
            saved_at_unix: unix_time(SystemTime::now()),
            verification: verification.clone(),
        };
        tokio::fs::write(&path, serde_json::to_vec(&entry)?).await?;
        prune_cache(&self.cache_dir, CACHE_LIMIT_BYTES).await
    }
}

fn candidate_from_recording(recording: Recording) -> Candidate {
    let releases = recording.releases.unwrap_or_default();
    Candidate {
        title: recording.title,
        artists: artist_names(recording.artist_credit.as_deref()),
        albums: releases
            .iter()
            .map(|release| release.title.clone())
            .collect(),
        duration_ms: recording.length.map(u64::from),
        score: recording.score.unwrap_or_default(),
        recording_id: recording.id,
        release_id: releases.first().map(|release| release.id.clone()),
        release_group_id: releases
            .iter()
            .find_map(|release| release.release_group.as_ref().map(|group| group.id.clone())),
    }
}

fn artist_names(credits: Option<&[ArtistCredit]>) -> Vec<String> {
    credits
        .unwrap_or_default()
        .iter()
        .flat_map(|credit| [credit.name.as_str(), credit.artist.name.as_str()])
        .map(str::to_owned)
        .collect()
}

#[must_use]
pub fn match_candidates(track: &RawTrack, candidates: &[Candidate]) -> Verification {
    let mut valid = candidates
        .iter()
        .filter(|candidate| is_known_recording(track, candidate))
        .collect::<Vec<_>>();
    valid.sort_by(|left, right| compare_candidates(track, left, right));
    let Some(best) = valid.first() else {
        return Verification::failed(VerificationStatus::NotFound);
    };
    Verification {
        status: VerificationStatus::Verified,
        score: Some(best.score),
        recording_id: Some(best.recording_id.clone()),
        release_id: best.release_id.clone(),
        release_group_id: best.release_group_id.clone(),
    }
}

fn compare_candidates(track: &RawTrack, left: &Candidate, right: &Candidate) -> std::cmp::Ordering {
    let left_evidence = evidence(track, left);
    let right_evidence = evidence(track, right);
    right_evidence
        .album_matches
        .cmp(&left_evidence.album_matches)
        .then_with(|| {
            right_evidence
                .duration_matches
                .cmp(&left_evidence.duration_matches)
        })
        .then_with(|| right.score.cmp(&left.score))
        .then_with(|| {
            left_evidence
                .duration_delta
                .cmp(&right_evidence.duration_delta)
        })
        .then_with(|| left.recording_id.cmp(&right.recording_id))
}

#[derive(Debug, Clone, Copy)]
struct MatchEvidence {
    album_matches: bool,
    duration_matches: bool,
    duration_delta: u64,
}

fn evidence(track: &RawTrack, candidate: &Candidate) -> MatchEvidence {
    let album_matches = !track.album.trim().is_empty()
        && candidate
            .albums
            .iter()
            .any(|album| albums_match(&track.album, album));
    let duration_delta = track
        .duration_ms
        .zip(candidate.duration_ms)
        .map_or(u64::MAX, |(left, right)| left.abs_diff(right));
    MatchEvidence {
        album_matches,
        duration_matches: duration_delta <= 5_000,
        duration_delta,
    }
}

fn is_known_recording(track: &RawTrack, candidate: &Candidate) -> bool {
    if candidate.score < 90 || normalize(&candidate.title) != normalize(&track.title) {
        return false;
    }
    let credited = candidate
        .artists
        .iter()
        .map(|artist| normalize(artist))
        .collect::<Vec<_>>();
    if !track
        .artists
        .iter()
        .all(|artist| credited.contains(&normalize(artist)))
    {
        return false;
    }
    true
}

fn albums_match(reported: &str, candidate: &str) -> bool {
    normalize_album(reported) == normalize_album(candidate)
}

fn normalize_album(value: &str) -> String {
    let mut base = value.trim();
    while let Some((prefix, qualifier)) = trailing_qualifier(base) {
        let qualifier = normalize(qualifier);
        if [
            "deluxe",
            "edition",
            "expanded",
            "remaster",
            "anniversary",
            "bonus",
            "reissue",
            "special",
        ]
        .iter()
        .any(|marker| qualifier.contains(marker))
        {
            base = prefix.trim();
        } else {
            break;
        }
    }
    normalize(base)
}

fn trailing_qualifier(value: &str) -> Option<(&str, &str)> {
    let (opening, closing) = match value.as_bytes().last()? {
        b')' => ('(', ')'),
        b']' => ('[', ']'),
        _ => return None,
    };
    let opening_index = value.rfind(opening)?;
    let qualifier =
        value.get(opening_index + opening.len_utf8()..value.len() - closing.len_utf8())?;
    Some((&value[..opening_index], qualifier))
}

#[must_use]
pub fn normalize(value: &str) -> String {
    value
        .nfkd()
        .filter(|character| !unicode_normalization::char::is_combining_mark(*character))
        .flat_map(char::to_lowercase)
        .filter(|character| character.is_alphanumeric())
        .collect()
}

fn lucene_escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace(':', "\\:")
}

fn unix_time(time: SystemTime) -> u64 {
    time.duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Remove oldest cache files until the tree fits within `limit` bytes.
///
/// # Errors
///
/// Returns an error when cache directory traversal fails.
pub async fn prune_cache(root: &Path, limit: u64) -> Result<()> {
    let mut files = Vec::new();
    collect_files(root, &mut files).await?;
    let mut total = files.iter().map(|(_, size)| size).sum::<u64>();
    if total <= limit {
        return Ok(());
    }
    files.sort_unstable_by_key(|(path, _)| {
        std::fs::metadata(path)
            .and_then(|metadata| metadata.modified())
            .unwrap_or(SystemTime::UNIX_EPOCH)
    });
    for (path, size) in files {
        if total <= limit {
            break;
        }
        if tokio::fs::remove_file(path).await.is_ok() {
            total = total.saturating_sub(size);
        }
    }
    Ok(())
}

async fn collect_files(root: &Path, output: &mut Vec<(PathBuf, u64)>) -> Result<()> {
    let Ok(mut pending) = tokio::fs::read_dir(root).await else {
        return Ok(());
    };
    let mut directories = Vec::new();
    while let Some(entry) = pending.next_entry().await? {
        let metadata = entry.metadata().await?;
        if metadata.is_dir() {
            directories.push(entry.path());
        } else if metadata.is_file() {
            output.push((entry.path(), metadata.len()));
        }
    }
    for directory in directories {
        Box::pin(collect_files(&directory, output)).await?;
    }
    Ok(())
}
