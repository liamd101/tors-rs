use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use rand::prelude::*;
use std::{fs::File, sync::Mutex};
use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, Layer};

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Parser, Debug)]
#[command(version)]
pub struct Opts {
    /// Controls the log level of the program
    #[arg(value_enum, short = 'v', long)]
    pub log_level: Option<LogLevel>,

    /// Log filename to also write to in addition to the console.
    #[arg(long = "log-file")]
    pub log_file: Option<String>,

    /// The value for RUST_LOG in the log file
    #[arg(long = "log-file-rust-log", default_value = "tors_rs=debug,info")]
    log_file_rust_log: String,

    /// The input .torrent file to download
    #[arg(short, long, required = true)]
    pub file: String,

    /// The maximum number of peers to connect to at once
    #[arg(short, long, default_value_t = 10)]
    pub max_peers: usize,

    /// Directory to write the file contents to. Defaults to `out` or path specified by input file
    #[arg(short = 'd', long)]
    pub dir: Option<String>,

    /// Whether or not to use the Fast Extension (BEP0006)
    #[arg(short = 'F', long)]
    pub fast_extension: bool,
}

const CLIENT_NAME: &str = "RS";
const CLIENT_VERSION_MAJOR: u8 = 0;
const CLIENT_VERSION_MINOR: u8 = 1;
const CLIENT_VERSION_PATCH: u8 = 0;

pub struct Config {
    pub args: Opts,
    pub peer_id: [u8; 20],
}
impl Config {
    pub fn from_args() -> Result<Self> {
        let args = Opts::parse();

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

        Ok(Config { args, peer_id })
    }
}

pub fn init_logging(config: &Config) -> Result<()> {
    let filter_level = match config.args.log_level.unwrap_or(LogLevel::Info) {
        LogLevel::Trace => "trace",
        LogLevel::Debug => "debug",
        LogLevel::Info => "info",
        LogLevel::Warn => "warn",
        LogLevel::Error => "error",
    };
    let stderr_filter = EnvFilter::builder()
        .with_default_directive(filter_level.parse().context("parsing filter level")?)
        .from_env()
        .context("invalid RUST_LOG value")?;

    let subscriber = tracing_subscriber::Registry::default();

    let layered = subscriber.with(tracing_subscriber::fmt::layer().with_filter(stderr_filter));

    if let Some(log_file) = &config.args.log_file {
        let log_file = Mutex::new(File::create(log_file)?);
        let layer = tracing_subscriber::fmt::layer()
            .with_ansi(false)
            .with_writer(log_file)
            .with_filter(
                EnvFilter::builder()
                    .parse(&config.args.log_file_rust_log)
                    .context("parsing log-file level")?,
            );
        layered
            .with(layer)
            .try_init()
            .context("initializing logger")?;
    } else {
        layered.try_init().context("initializing logger")?;
    }

    Ok(())
}
