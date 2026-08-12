pub mod artwork;
pub mod config;
pub mod domain;
pub mod lastfm;
pub mod listenbrainz;
pub mod mpris;
pub mod mqtt;
pub mod runtime;
pub mod selection;
pub mod tui;
pub mod verification;
pub mod webhook;

pub const APP_NAME: &str = "TuneBeacon";
pub const USER_AGENT: &str = concat!("TuneBeacon/", env!("CARGO_PKG_VERSION"));
