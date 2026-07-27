use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use reqwest::header::CONTENT_LENGTH;
use sha2::{Digest, Sha256};

use crate::USER_AGENT;

const MAX_ART_BYTES: u64 = 12 * 1024 * 1024;

#[must_use]
pub fn preferred_art_url(
    mpris_url: Option<&str>,
    release_group_id: Option<&str>,
) -> Option<String> {
    mpris_url.map(str::to_owned).or_else(|| {
        release_group_id.map(|id| format!("https://coverartarchive.org/release-group/{id}/front"))
    })
}

/// Resolve local artwork or download and validate remote artwork into the cache.
///
/// # Errors
///
/// Returns an error for invalid URLs, unsupported schemes or images, oversized
/// downloads, HTTP failures, and filesystem failures.
pub async fn cached_artwork(cache_dir: &Path, url: &str) -> Result<PathBuf> {
    if let Some(path) = url.strip_prefix("file://") {
        let decoded = percent_decode(path);
        let path = PathBuf::from(decoded);
        anyhow::ensure!(path.is_file(), "local artwork does not exist");
        return Ok(path);
    }
    let parsed = url::Url::parse(url)?;
    anyhow::ensure!(
        matches!(parsed.scheme(), "http" | "https"),
        "unsupported artwork URL scheme"
    );
    let mut hasher = Sha256::new();
    hasher.update(url.as_bytes());
    let target = cache_dir
        .join("artwork")
        .join(format!("{:x}.img", hasher.finalize()));
    if target.is_file() {
        return Ok(target);
    }
    let parent = target.parent().context("artwork cache has no parent")?;
    tokio::fs::create_dir_all(parent).await?;
    let response = reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .timeout(Duration::from_secs(15))
        .redirect(reqwest::redirect::Policy::limited(5))
        .build()?
        .get(parsed)
        .send()
        .await?
        .error_for_status()?;
    if response
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .is_some_and(|length| length > MAX_ART_BYTES)
    {
        bail!("artwork exceeds {} MiB", MAX_ART_BYTES / 1024 / 1024);
    }
    let bytes = response.bytes().await?;
    anyhow::ensure!(
        bytes.len() as u64 <= MAX_ART_BYTES,
        "artwork exceeds download limit"
    );
    image::load_from_memory(&bytes).context("artwork is not a supported image")?;
    let temporary = target.with_extension(format!("tmp-{}", std::process::id()));
    tokio::fs::write(&temporary, &bytes).await?;
    tokio::fs::rename(&temporary, &target).await?;
    crate::verification::prune_cache(cache_dir, crate::verification::CACHE_LIMIT_BYTES).await?;
    Ok(target)
}

fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%'
            && index + 2 < bytes.len()
            && let (Some(high), Some(low)) =
                (hex_nibble(bytes[index + 1]), hex_nibble(bytes[index + 2]))
        {
            output.push((high << 4) | low);
            index += 3;
            continue;
        }
        output.push(bytes[index]);
        index += 1;
    }
    String::from_utf8_lossy(&output).into_owned()
}

const fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}
