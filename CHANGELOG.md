# Changelog

All notable changes to TuneBeacon will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-08-12

### Added

- ListenBrainz token validation, now-playing notifications, and native listen
  submissions with pause-aware timing, MusicBrainz enrichment, retry handling,
  daemon support, and TUI configuration/status controls.

## [0.1.0] - 2026-08-02

### Added

- Interactive terminal interface and headless daemon for monitoring explicitly
  approved MPRIS media players.
- Privacy-first MusicBrainz verification, cached artwork, and an opt-in fallback
  policy for unverified metadata.
- MQTT and HTTP webhook delivery with configurable authentication, TLS, QoS,
  retention, diagnostics, and retry behaviour.
- Last.fm authorization, now-playing updates, and scrobbling with pause-aware
  timing and retry handling.
- Ordered player selection, persistent offline-player configuration, and a
  review screen for unsaved settings.
- Atomic, owner-only configuration storage and a sample systemd user service.

[Unreleased]: https://github.com/scottweston/tunebeacon/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/scottweston/tunebeacon/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/scottweston/tunebeacon/releases/tag/v0.1.0
