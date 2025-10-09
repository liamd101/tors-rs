use anyhow::Result;
use clap::Parser;
use rand::prelude::*;
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

const CLIENT_NAME: &str = "RS";
const CLIENT_VERSION_MAJOR: u8 = 0;
const CLIENT_VERSION_MINOR: u8 = 1;
const CLIENT_VERSION_PATCH: u8 = 0;

pub struct Config {
    pub file: String,
    pub verbose: bool,
    pub max_peers: usize,
    pub peer_id: [u8; 20],
}
impl Config {
    pub fn from_args() -> Result<Self> {
        let args = Args::parse();

        let mut peer_id: [u8; 20] = [0u8; 20];
        peer_id[0] = b'-';
        peer_id[1] = CLIENT_NAME.bytes().collect::<Vec<u8>>()[0];
        peer_id[2] = CLIENT_NAME.bytes().collect::<Vec<u8>>()[1];
        peer_id[3] = CLIENT_VERSION_MAJOR;
        peer_id[4] = CLIENT_VERSION_MINOR;
        peer_id[5] = CLIENT_VERSION_PATCH;
        peer_id[6] = 0;
        peer_id[7] = b'-';
        rand::rng().fill_bytes(&mut peer_id[8..]);

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
