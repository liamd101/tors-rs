use anyhow::Result;
use clap::Parser;
use rand::prelude::*;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(version)]
pub struct Args {
    /// Controls the log level of the program
    #[arg(short='v', long)]
    pub verbose: bool,

    /// The input .torrent file to download
    #[arg(short, long, required = true)]
    pub file: String,

    /// The maximum number of peers to connect to at once
    #[arg(short, long, default_value_t = 10)]
    pub max_peers: usize,

    /// Directory to write the file contents to. Defaults to `out` or path specified by input file
    #[arg(short='d', long)]
    pub dir: Option<String>,

    /// Whether or not to use the Fast Extension (BEP0006)
    #[arg(short='f', long, default_value_t=true)]
    pub fast_extension: bool,
}

const CLIENT_NAME: &str = "RS";
const CLIENT_VERSION_MAJOR: u8 = 0;
const CLIENT_VERSION_MINOR: u8 = 1;
const CLIENT_VERSION_PATCH: u8 = 0;

pub struct Config {
    pub args: Args,
    pub peer_id: [u8; 20],
}
impl Config {
    pub fn from_args() -> Result<Self> {
        let args = Args::parse();

        let mut peer_id: [u8; 20] = [0u8; 20];
        peer_id[0] = b'-';
        peer_id[1] = CLIENT_NAME.bytes().collect::<Vec<u8>>()[0];
        peer_id[2] = CLIENT_NAME.bytes().collect::<Vec<u8>>()[1];
        peer_id[3] = b'0' + CLIENT_VERSION_MAJOR;
        peer_id[4] = b'0' + CLIENT_VERSION_MINOR;
        peer_id[5] = b'0' + CLIENT_VERSION_PATCH;
        peer_id[6] = b'0';
        peer_id[7] = b'-';
        rand::rng().fill_bytes(&mut peer_id[8..]);

        Ok(Config {
            args,
            peer_id,
        })
    }
}

/// Initialize `tracing` to support logging.
pub fn init_logging(config: &Config) {
    let filter_level = if config.args.verbose { "debug" } else { "info" };
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(filter_level))
        .init();
}
