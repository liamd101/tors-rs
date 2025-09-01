use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use tors_rs::{
    ThreadUpdate,
    message::BitField,
    parsing::{self, Hashes, Metadata},
    peer::{PeerHandshake, handle_peer, try_handshake},
    tracker::{TrackerResponse, create_tracker_url},
};

use anyhow::Result;
use clap::Parser;
use sha1::{Digest, Sha1};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
    net::TcpListener,
    sync::broadcast,
    task::JoinSet,
};
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

    let torr_path: PathBuf = PathBuf::from(args.file);

    let data: &[u8] = &std::fs::read(torr_path).expect("file does not exist");
    let metadata: Metadata = serde_bencode::from_bytes(data).expect("unable to parse file");
    let info_hash = parsing::get_info_hash(&metadata.info);
    let peer_id = parsing::hash_string("liamdodds11223344556".to_string());

    let listener: TcpListener = find_open_port().await.expect("unable to find open port");
    let announce = create_tracker_url(&metadata, listener).expect("valid tracker URL");

    // let announce = reqwest::Url::parse_with_params(announce.as_str(), params).expect("unable to create tracker URL");
    let res = reqwest::get(announce).await.expect("invalid tracker URL");
    let body = res.bytes().await.expect("error reading body");

    let res: TrackerResponse = serde_bencode::from_bytes(&body).expect("unable to parse file");
    let peers = match res {
        TrackerResponse::Success { peers, .. } => peers.0,
        TrackerResponse::Error { failure_reason } => {
            error!("{failure_reason}");
            return;
        }
    };
    debug!("tracker supplied {} peers", peers.len());
    // now to connect to a peer
    let num_pieces = metadata
        .info
        .torr_type
        .len()
        .div_ceil(metadata.info.piece_length);
    let my_bitfield = Arc::new(RwLock::new(BitField::with_settable(num_pieces)));

    // want to make bitfield from our file
    let mut my_download = Download::new(
        metadata.info.name.clone(),
        metadata.info.pieces.clone(),
        metadata.info.torr_type.len(),
        metadata.info.piece_length as u64,
        my_bitfield.clone(),
    )
    .await
    .expect("couldn't create download struct");

    if my_download.is_downloaded() {
        info!("file is downloaded already!!");
        return;
    }

    let mut set = JoinSet::new();
    let (tx, mut rx1) = broadcast::channel::<ThreadUpdate>(4);

    let tx2 = tx.clone();
    set.spawn(async move {
        loop {
            match rx1.recv().await.unwrap() {
                ThreadUpdate::Downloaded(piece, block) => {
                    debug!("downloaded piece={piece} block={block}");
                    let changed = my_download
                        .update_downloads()
                        .await
                        .expect("couldn't update download state");
                    debug!("changed pieces={changed:?}");
                    for changed_piece in changed {
                        tx2.send(ThreadUpdate::Completed(changed_piece as u32))
                            .expect("couldn't send");
                    }
                    if my_download.is_downloaded() {
                        break;
                    }
                }
                ThreadUpdate::Completed(_piece) => continue,
            }
        }
    });

    for peer in &peers {
        let span = tracing::info_span!("peer", peer_addr = %peer.socket_addr);

        let mut stream = tokio::net::TcpStream::connect(peer.socket_addr)
            .await
            .expect("couldn't connect to peer");

        let metadata = metadata.clone();
        let handshake = PeerHandshake::v1(info_hash, peer_id);
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

/// A data structure representing information for downloading a file from a `.torrent` file.
#[derive(Debug)]
struct Download {
    /// The location of the file being downloaded
    pub name: PathBuf,
    piece_length: u64,
    length: u64,
    /// The number of pieces in the file being downloaded
    num_pieces: usize,
    /// The piece hashes of the torrent file being downloaded. Derived from the `.torrent` file
    piece_hashes: Hashes,
    /// A BitField of the currently downloaded pieces. Read from left-to-right with a 1 set if the
    /// piece is downloaded and verified. 0 otherwise
    bitfield: Arc<RwLock<BitField>>,
}

impl Download {
    // TODO: update this error type to something more robust
    pub async fn new(
        name: String,
        piece_hashes: Hashes,
        length: u64,
        piece_length: u64,
        bitfield: Arc<RwLock<BitField>>,
    ) -> Result<Self, std::io::Error> {
        let name = PathBuf::from(name);
        let num_pieces = length.div_ceil(piece_length) as usize;
        let mut out = Self {
            name,
            piece_length,
            length,
            num_pieces,
            piece_hashes,
            bitfield,
        };
        out.update_downloads().await?;
        Ok(out)
    }

    pub fn is_downloaded(&self) -> bool {
        self.bitfield.read().unwrap().set_bits().len() == self.num_pieces
    }

    /// Iterates through all pieces of the file, computes their SHA1 hash, and then sets their
    /// correspondign bits in the BitField to true/false accordingly
    pub async fn update_downloads(&mut self) -> Result<Vec<usize>, std::io::Error> {
        let mut file: File = tokio::fs::File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&self.name)
            .await?;

        if file.metadata().await?.len() == 0 {
            file.set_len(self.length).await?;
            return Ok(vec![]); /* no need to check pieces since the file has not been written to */
        }

        let mut changed = vec![];

        for piece in 0..self.num_pieces {
            let piece_len = if piece == (self.num_pieces - 1) {
                self.length % self.piece_length
            } else {
                self.piece_length
            };
            let mut piece_data: Vec<u8> = vec![0u8; piece_len as usize];
            file.seek(SeekFrom::Start(self.piece_length * piece as u64))
                .await?;
            file.read_exact(&mut piece_data).await?;

            let mut hasher = Sha1::new();
            hasher.update(piece_data);
            let piece_hash: [u8; 20] = hasher.finalize().into();
            let hash: [u8; 20] = self.piece_hashes.0[piece];

            let finished_download: bool = piece_hash == hash;

            let prev = self
                .bitfield
                .write()
                .unwrap()
                .set(piece, finished_download)
                .expect("index out of bounds");

            if finished_download && !prev {
                changed.push(piece);
            }
        }

        Ok(changed)
    }
}
