use clap::Parser;

use anyhow::{Context, Result};
use dotenv::dotenv;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
struct Args {
    /// Controls the log level of the program
    #[arg(short, long)]
    verbose: bool,

    /// The input .torrent file to download
    #[arg(short, long, required = true)]
    file: String,

    /// The maximum number of peers to connect to at once
    #[arg(short, long, default_value_t = 3)]
    max_peers: usize,
}

pub struct Config {
    pub file: String,
    pub verbose: bool,
    pub max_peers: usize,
    pub peer_id: [u8; 20],
}
impl Config {
    pub fn from_args() -> Result<Self> {
        let args = Args::parse();
        dotenv().ok();

        let peer_id: [u8; 20] = std::env::var("USER_PEER_ID")
            .context("USER_PEER_ID must be set.")?
            .as_bytes()
            .try_into()
            .context("Invalid peer ID format.")?;

        Ok(Config {
            file: args.file,
            verbose: args.verbose,
            max_peers: args.max_peers,
            peer_id,
        })
    }
}

pub fn init_logging(config: &Config) {
    let filter_level = if config.verbose { "debug" } else { "info" };
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(filter_level))
        .init();
}
