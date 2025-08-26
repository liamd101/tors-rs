use sha1::{Digest, Sha1};
use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;

use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::task::JoinSet;

use tors_rs::{
    message::BitField,
    parsing::{Hashes, Metadata},
    peer::{PeerHandshake, handle_peer},
    tracker::TrackerResponse,
};

use tracing::{debug, error, info};
use tracing_subscriber::{self, EnvFilter};

use anyhow::Result;

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    verbose: bool,

    #[arg(short, long, required = true)]
    file: String,
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
    let announce: reqwest::Url = metadata.announce.parse().expect("unable to parse announce");
    match announce.scheme() {
        "http" | "https" => {}
        _ => {
            error!("invalid scheme");
            return;
        }
    }

    let port = 6969;
    let peer_id = tors_rs::parsing::hash_string("liamdodds11223344556".to_string());
    let mut params: HashMap<String, String> = HashMap::new();
    params.insert("port".into(), format!("{port}"));
    params.insert("event".into(), "started".into());
    params.insert("compact".into(), "1".into());
    params.insert("uploaded".into(), "0".into());
    params.insert("downloaded".into(), "0".into());
    params.insert(
        "peer_id".into(),
        urlencoding::encode_binary(&peer_id).to_string(),
    );
    match metadata.info.torr_type {
        tors_rs::parsing::FileTypes::SingleFile { length } => {
            params.insert("left".into(), format!("{length}"));
        }
        _ => unimplemented!("don't have support for multiple files yet"),
    }

    let info_hash = tors_rs::parsing::get_info_hash(&metadata.info);
    params.insert(
        "info_hash".into(),
        urlencoding::encode_binary(&info_hash).to_string(),
    );

    // what the fuck
    let params = params
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<String>>()
        .join("&");
    let announce = format!("{announce}?{params}");

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

    // want to make bitfield from our file
    let my_download = Download::new(
        metadata.info.name.clone(),
        metadata.info.pieces.clone(),
        metadata.info.torr_type.len(),
        metadata.info.piece_length as u64,
    )
    .await
    .expect("couldn't create download struct");
    debug!("my_download={my_download:?}");

    if my_download.is_downloaded() {
        info!("file is downloaded already!!");
        return;
    }

    let my_bitfield = Arc::new(my_download.bitfield);

    let handshake = PeerHandshake::v1(info_hash, peer_id);

    // TODO: maybe specify a limit of peers we can connect to at once?
    let mut set = JoinSet::new();

    // TODO: this should probably use threads to download files in parallel
    for peer in &peers {
        let stream = tokio::net::TcpStream::connect(peer.socket_addr)
            .await
            .expect("couldn't connect to peer");

        let metadata = metadata.clone();
        let handshake = handshake.clone();
        let peer_addr = peer.socket_addr;
        let my_bitfield = my_bitfield.clone();

        set.spawn(async move {
            match handle_peer(metadata, handshake, my_bitfield, stream, peer_addr).await {
                Ok(()) => {}
                Err(e) => error!("handle_peer {peer_addr}: {e}"),
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
    bitfield: tors_rs::message::BitField,
}

impl Download {
    // TODO: update this error type to something more robust
    pub async fn new(
        name: String,
        piece_hashes: Hashes,
        length: u64,
        piece_length: u64,
    ) -> Result<Self, std::io::Error> {
        let name = PathBuf::from(name);
        let num_pieces: usize = ((length + piece_length - 1) / piece_length) as usize;
        let bitfield: BitField = BitField::with_settable(num_pieces);
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
        self.bitfield.set_bits().len() == self.num_pieces
    }

    /// Iterates through all pieces of the file, computes their SHA1 hash, and then sets their
    /// correspondign bits in the BitField to true/false accordingly
    pub async fn update_downloads(&mut self) -> Result<(), std::io::Error> {
        let mut file: File = tokio::fs::File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&self.name)
            .await?;

        if file.metadata().await?.len() == 0 {
            file.set_len(self.length).await?;
            return Ok(()); /* no need to check pieces since the file has not been written to */
        }

        for piece in 0..self.num_pieces {
            let piece_len = if piece == (self.num_pieces - 1) as usize {
                self.length % self.piece_length
            } else {
                self.piece_length
            };
            let mut piece_data: Vec<u8> = vec![0u8; piece_len as usize];
            file.seek(SeekFrom::Start(self.piece_length * piece as u64)).await?;
            file.read_exact(&mut piece_data).await?;

            let mut hasher = Sha1::new();
            hasher.update(piece_data);
            let piece_hash: [u8; 20] = hasher.finalize().into();
            let hash: [u8; 20] = self.piece_hashes.0[piece];

            let finished_download: bool = piece_hash == hash;
            debug!("checking piece={piece}");
            debug!("downloaded_hash ={piece_hash:?}");
            debug!("tracker    hash ={piece_hash:?}");
            self.bitfield = self.bitfield
                .set(piece, finished_download)
                .expect("index out of bounds");
        }

        Ok(())
    }
}
