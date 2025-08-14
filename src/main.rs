use sha1::{Digest, Sha1};
use std::collections::HashMap;
use std::path::PathBuf;

use std::io::prelude::*;

mod tracker;
use tracker::TrackerResponse;

mod parsing;
use parsing::Metadata;

fn get_info_hash(info: &parsing::TorrInfo) -> [u8; 20] {
    let serialized = serde_bencode::to_bytes(info).expect("could not serialize metadata");
    let mut hasher = Sha1::new();
    hasher.update(serialized);
    hasher.finalize().into()
}

#[tokio::main]
async fn main() {
    // let testing_string: String = "d7:meaningi42e4:wiki7:bencodee".into();
    let torr_path: PathBuf = PathBuf::from("sample.torrent");

    if !torr_path.exists() {
        println!("torr_path={torr_path:?}");
        println!("file does not exist");
        return;
    }

    let data: &[u8] = &std::fs::read(torr_path).expect("unable to read file");
    let metadata: Metadata = serde_bencode::from_bytes(data).expect("unable to parse file");
    let announce: reqwest::Url = metadata.announce.parse().expect("unable to parse announce");
    match announce.scheme() {
        "http" | "https" => {}
        _ => {
            println!("invalid scheme");
            return;
        }
    }

    let port = 6969;
    let peer_id = "wdodds12345123451234";
    let mut params: HashMap<String, String> = HashMap::new();
    params.insert("port".into(), format!("{port}"));
    params.insert("event".into(), "started".into());
    params.insert("compact".into(), "1".into());
    params.insert("uploaded".into(), "0".into());
    params.insert("downloaded".into(), "0".into());
    params.insert("peer_id".into(), peer_id.into());
    match metadata.info.torr_type {
        parsing::FileTypes::SingleFile { length } => {
            params.insert("left".into(), format!("{length}"));
        }
        _ => panic!(),
    }

    let info_hash = get_info_hash(&metadata.info);
    let info_hash = urlencoding::encode_binary(&info_hash);
    params.insert("info_hash".into(), info_hash.to_string());

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
        TrackerResponse::Error { .. } => {
            return;
        }
    };
    // now to connect to a peer

    let peer = peers.first().unwrap();

    let mut stream: std::net::TcpStream =
        std::net::TcpStream::connect(peer.socket_addr).expect("could not connect to peer");
    let mut parts = [0; 128];
    let handshake = Handshake {
        pstrlen: 19,
        pstr: "BitTorrent protocol".into(),
        reserved: [0; 8],
        info_hash: get_info_hash(&metadata.info),
        peer_id: peer_id.into(),
    };
    stream.read(&mut parts).expect("error reading from stream");
    println!("{parts:?}");
}

struct Handshake {
    /// String length of pstr as a single raw byte
    pub pstrlen: u8,
    /// String identifier of the protocol
    pub pstr: String,
    /// 8 reserved bits. All current implementations use all zeroes. Each bit in these bytes can be
    /// used to change the behavior of the protocol
    pub reserved: [u8; 8],
    /// 20-byte SHA1 hash of the info key in the metainfo file. Same info_hash that is transmitted
    /// in tracker requests
    pub info_hash: [u8; 20],
    /// 20-byte string used as a unique ID for the client. This is usually the same peer_id that
    /// is transmitted in tracker requests, but not always.
    pub peer_id: [u8; 20],
}
