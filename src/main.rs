use tors_rs::{
    ThreadUpdate, download,
    parsing::Metadata,
    peer::{Handshake, handle_peer, try_handshake},
    tracker,
};

use anyhow::Result;
use clap::Parser;
use dotenv::dotenv;
use tokio::{net::TcpListener, sync::broadcast, task::JoinSet};
use tracing::{Instrument, debug, error, info, warn};
use tracing_subscriber::{self, EnvFilter};

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    verbose: bool,

    #[arg(short, long, required = true)]
    file: String,
}

async fn find_open_port() -> Result<TcpListener, std::io::Error> {
    for port_num in 6881..=6889 {
        match TcpListener::bind(format!("127.0.0.1:{port_num}")).await {
            Ok(out) => return Ok(out),
            Err(_) => continue,
        }
    }
    Err(std::io::Error::other("unable to find open port"))
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let filter_level = if args.verbose { "debug" } else { "info" };

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(filter_level))
        .init();
    dotenv().ok();

    let metadata = Metadata::new(&args.file).expect("Unable to parse torrent file.");

    let peer_id: [u8; 20] = std::env::var("USER_PEER_ID")
        .expect("USER_PEER_ID must be set.")
        .as_bytes()
        .try_into()
        .expect("invalid peer ID.");

    let listener: TcpListener = find_open_port().await.expect("unable to find open port.");

    let res = tracker::make_request(&metadata, &listener)
        .await
        .expect("unable to contact tracker");
    let peers = match res {
        tracker::Response::Success { peers, .. } => peers.0,
        tracker::Response::Error { failure_reason } => {
            error!("Making request to tracker failed: {failure_reason}");
            return;
        }
    };

    debug!("tracker supplied {} peers", peers.len());

    // want to make bitfield from our file
    let my_download = download::Download::new(&metadata)
        .await
        .expect("couldn't create download struct");

    if my_download.is_downloaded() {
        info!("file is downloaded already!!");
        return;
    }

    let my_bitfield = my_download.bitfield();

    let mut set = JoinSet::new();
    let (tx, rx1) = broadcast::channel::<ThreadUpdate>(4);

    let tx2 = tx.clone();
    set.spawn(async move {
        match download::watch_download(my_download, tx2, rx1).await {
            Ok(()) => {}
            Err(e) => error!("{e}"),
        }
    });

    for peer in &peers {
        let span = tracing::info_span!("peer", peer_addr = %peer.socket_addr);

        let mut stream = tokio::net::TcpStream::connect(peer.socket_addr)
            .await
            .expect("couldn't connect to peer");

        let metadata = metadata.clone();
        let info_hash = metadata.info_hash();
        let handshake = Handshake::v1(info_hash, peer_id);
        let my_bitfield = my_bitfield.clone();

        match try_handshake(&mut stream, &handshake).await {
            Ok(true) => {}
            Ok(false) => {
                warn!("peer failed the handshake. trying another peer instead");
                continue;
            }
            Err(e) => {
                error!("{e}");
                continue;
            }
        }

        let thread_tx = tx.clone();
        let thread_rx = tx.subscribe();

        set.spawn(async move {
            match handle_peer(thread_tx, thread_rx, stream, metadata, my_bitfield)
                .instrument(span)
                .await
            {
                Ok(()) => {}
                Err(e) => error!("{e}"),
            }
        });
    }

    while set.join_next().await.is_some() {}
}
