use std::{io, time::Duration};

use chrono::{TimeZone, Utc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tunebeacon::{
    config::WebhookConfig,
    domain::{NowPlayingMessage, PublishedPlayer, Verification, VerificationStatus},
    webhook::WebhookPublisher,
};

fn message(track: &str) -> NowPlayingMessage {
    NowPlayingMessage {
        schema_version: 1,
        observed_at: Utc.with_ymd_and_hms(2026, 7, 27, 10, 15, 30).unwrap(),
        track: track.to_owned(),
        artists: vec!["Integration Artist".to_owned()],
        album: "Integration Album".to_owned(),
        duration_ms: Some(180_000),
        art_url: None,
        player: PublishedPlayer {
            key: "fake".to_owned(),
            identity: "Fake Player".to_owned(),
        },
        verification: Verification {
            status: VerificationStatus::Verified,
            score: Some(100),
            recording_id: Some("recording-id".to_owned()),
            release_id: None,
            release_group_id: None,
        },
    }
}

async fn read_request(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut request = Vec::new();
    let header_end = loop {
        let mut chunk = [0_u8; 1024];
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "request ended before its headers",
            ));
        }
        request.extend_from_slice(&chunk[..read]);
        if let Some(position) = request.windows(4).position(|value| value == b"\r\n\r\n") {
            break position + 4;
        }
    };
    let headers = String::from_utf8_lossy(&request[..header_end]);
    let content_length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(0);
    while request.len() < header_end + content_length {
        let mut chunk = [0_u8; 1024];
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            break;
        }
        request.extend_from_slice(&chunk[..read]);
    }
    Ok(request)
}

#[tokio::test]
async fn posts_same_json_with_bearer_auth_and_stays_silent_when_idle() {
    let listener = match TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => listener,
        Err(error) if error.kind() == io::ErrorKind::PermissionDenied => return,
        Err(error) => panic!("could not bind webhook integration server: {error}"),
    };
    let port = listener.local_addr().unwrap().port();
    let publisher = WebhookPublisher::spawn(WebhookConfig {
        enabled: true,
        url: format!("http://127.0.0.1:{port}/now-playing"),
        bearer_token: Some("integration-secret".to_owned()),
    });

    publisher.clear_if_idle();
    assert!(
        tokio::time::timeout(Duration::from_millis(200), listener.accept())
            .await
            .is_err(),
        "idle unexpectedly produced a webhook request"
    );

    publisher.publish_latest(message("Integration Song"));
    let (mut stream, _) = tokio::time::timeout(Duration::from_secs(3), listener.accept())
        .await
        .expect("webhook publisher did not connect")
        .unwrap();
    let request = read_request(&mut stream).await.unwrap();
    stream
        .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
        .await
        .unwrap();

    let separator = request
        .windows(4)
        .position(|value| value == b"\r\n\r\n")
        .unwrap()
        + 4;
    let headers = String::from_utf8_lossy(&request[..separator]);
    assert!(headers.starts_with("POST /now-playing HTTP/1.1\r\n"));
    assert!(
        headers
            .to_ascii_lowercase()
            .contains("content-type: application/json")
    );
    assert!(headers.contains("authorization: Bearer integration-secret"));
    let payload: serde_json::Value = serde_json::from_slice(&request[separator..]).unwrap();
    assert_eq!(payload["track"], "Integration Song");
    assert_eq!(payload["verification"]["status"], "verified");

    tokio::time::timeout(Duration::from_secs(1), async {
        while !publisher.status().await.delivered {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("webhook status was not updated after delivery");
}

#[tokio::test]
async fn failed_delivery_is_replaced_by_the_newest_track() {
    let listener = match TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => listener,
        Err(error) if error.kind() == io::ErrorKind::PermissionDenied => return,
        Err(error) => panic!("could not bind webhook integration server: {error}"),
    };
    let port = listener.local_addr().unwrap().port();
    let publisher = WebhookPublisher::spawn(WebhookConfig {
        enabled: true,
        url: format!("http://127.0.0.1:{port}/now-playing"),
        bearer_token: None,
    });

    publisher.publish_latest(message("Old Track"));
    let (mut first, _) = tokio::time::timeout(Duration::from_secs(3), listener.accept())
        .await
        .expect("first webhook request did not connect")
        .unwrap();
    let first_request = read_request(&mut first).await.unwrap();
    let first_body = first_request
        .windows(4)
        .position(|value| value == b"\r\n\r\n")
        .map(|position| &first_request[position + 4..])
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(first_body).unwrap()["track"],
        "Old Track"
    );

    publisher.publish_latest(message("New Track"));
    first
        .write_all(
            b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        )
        .await
        .unwrap();
    drop(first);

    let (mut second, _) = tokio::time::timeout(Duration::from_secs(3), listener.accept())
        .await
        .expect("replacement webhook request did not connect")
        .unwrap();
    let second_request = read_request(&mut second).await.unwrap();
    second
        .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
        .await
        .unwrap();
    let second_body = second_request
        .windows(4)
        .position(|value| value == b"\r\n\r\n")
        .map(|position| &second_request[position + 4..])
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(second_body).unwrap()["track"],
        "New Track"
    );
}
