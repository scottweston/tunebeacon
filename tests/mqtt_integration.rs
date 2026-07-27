use std::{
    net::TcpListener,
    process::{Child, Command, Stdio},
    time::Duration,
};

use chrono::{TimeZone, Utc};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use tunebeacon::{
    config::MqttConfig,
    domain::{NowPlayingMessage, PublishedPlayer, Verification, VerificationStatus},
    mqtt::MqttPublisher,
};

struct Broker(Child);

impl Drop for Broker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
async fn qos_one_delivery_and_idle_silence_against_mosquitto() {
    let listener = match TcpListener::bind("127.0.0.1:0") {
        Ok(listener) => listener,
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => return,
        Err(error) => panic!("could not reserve an integration-test port: {error}"),
    };
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let child = match Command::new("mosquitto")
        .args(["-p", &port.to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(child) => child,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
        Err(error) => panic!("could not start mosquitto: {error}"),
    };
    let _broker = Broker(child);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let topic = "tunebeacon/integration/now-playing";
    let mut subscriber_options = MqttOptions::new("tunebeacon-integration-sub", "127.0.0.1", port);
    subscriber_options.set_keep_alive(Duration::from_secs(5));
    let (subscriber, mut subscriber_events) = AsyncClient::new(subscriber_options, 8);
    subscriber.subscribe(topic, QoS::AtLeastOnce).await.unwrap();
    loop {
        if matches!(
            subscriber_events.poll().await.unwrap(),
            Event::Incoming(Packet::SubAck(_))
        ) {
            break;
        }
    }

    let publisher = MqttPublisher::spawn(MqttConfig {
        enabled: true,
        host: "127.0.0.1".to_owned(),
        port,
        topic: topic.to_owned(),
        qos: 1,
        ..MqttConfig::default()
    });
    tokio::time::timeout(Duration::from_secs(3), async {
        while !publisher.status().await.connected {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("publisher did not connect");

    publisher.clear_if_idle();
    let idle_event =
        tokio::time::timeout(Duration::from_millis(300), subscriber_events.poll()).await;
    assert!(
        idle_event.is_err(),
        "idle unexpectedly produced an MQTT packet"
    );

    let message = NowPlayingMessage {
        schema_version: 1,
        observed_at: Utc.with_ymd_and_hms(2026, 7, 27, 10, 15, 30).unwrap(),
        track: "Integration Song".to_owned(),
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
    };
    publisher.publish_latest(message);

    let publish = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Event::Incoming(Packet::Publish(publish)) =
                subscriber_events.poll().await.unwrap()
            {
                break publish;
            }
        }
    })
    .await
    .expect("subscriber did not receive publication");
    assert_eq!(publish.qos, QoS::AtLeastOnce);
    let payload: serde_json::Value = serde_json::from_slice(&publish.payload).unwrap();
    assert_eq!(payload["track"], "Integration Song");
    assert_eq!(payload["verification"]["status"], "verified");
}
