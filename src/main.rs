use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use tunebeacon::config::{Config, default_cache_dir, default_config_path};

#[derive(Debug, Parser)]
#[command(name = "tunebeacon", version, about)]
struct Cli {
    /// Override ~/.config/tunebeacon/config.toml.
    #[arg(long, global = true)]
    config: Option<PathBuf>,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run headless in the foreground for a service supervisor.
    Daemon,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "tunebeacon=info".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    let cli = Cli::parse();
    let config_path = cli.config.unwrap_or_else(default_config_path);
    match cli.command {
        Some(Command::Daemon) => {
            let config = Config::load(&config_path).with_context(|| {
                format!("could not load configuration {}", config_path.display())
            })?;
            tunebeacon::runtime::run_daemon(config, default_cache_dir()).await
        }
        None => {
            let config = Config::load(&config_path).unwrap_or_else(|error| {
                eprintln!(
                    "Warning: could not load {}: {error:#}\nStarting with recoverable defaults; \
                     press s to replace the malformed file.",
                    config_path.display()
                );
                Config::default()
            });
            tunebeacon::tui::run(config, config_path, default_cache_dir()).await
        }
    }
}
