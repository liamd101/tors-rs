use sha1::{Digest, Sha1};
use std::collections::HashMap;
use std::path::PathBuf;

use std::net::{Ipv4Addr, SocketAddrV4};

mod tracker;
use tracker::{TrackerResponse, Peer};

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
    let torr_path: PathBuf = PathBuf::from("/Users/liamdodds/Downloads/sample.torrent");
    if !torr_path.exists() {
        println!("torr_path={:?}", torr_path);
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

    let mut params: HashMap<String, String> = HashMap::new();
    params.insert("port".into(), "6969".into());
    params.insert("event".into(), "started".into());
    params.insert("compact".into(), "1".into());
    params.insert("uploaded".into(), "0".into());
    params.insert("downloaded".into(), "0".into());
    params.insert("peer_id".into(), "wdodds12345123451234".into());
    match metadata.info.torr_type {
        parsing::FileTypes::SingleFile { length } => {
            params.insert("left".into(), format!("{}", length));
        }
        _ => panic!(),
    }

    let info_hash = get_info_hash(&metadata.info);
    let info_hash = urlencoding::encode_binary(&info_hash);
    params.insert("info_hash".into(), info_hash.to_string());

    // what the fuck
    let params = params
        .iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<String>>()
        .join("&");

    let announce = format!("{}?{}", announce, params);

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
    let peers: Vec<SocketAddrV4> = peers.iter().map(|peer| match peer {
        Peer::Compact { ip_addr, port } => SocketAddrV4::new(Ipv4Addr::from_bits(*ip_addr), *port),
        Peer::Expanded { ip_addr, port, .. } => SocketAddrV4::new(todo!(), *port),
    }).collect();
    println!("{:?}", peers);
}
