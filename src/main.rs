use sha1::{Digest, Sha1};
use std::collections::HashMap;
use std::path::PathBuf;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

mod tracker;
use tracker::TrackerResponse;

mod parsing;
use parsing::Metadata;

mod peer;
use peer::{Peer, PeerHandshake};

fn hash_string(s: String) -> [u8; 20] {
    let mut hasher = Sha1::new();
    hasher.update(s);
    hasher.finalize().into()
}

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
    let peer_id = hash_string("liamdodds11223344556".to_string());
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
        parsing::FileTypes::SingleFile { length } => {
            params.insert("left".into(), format!("{length}"));
        }
        _ => unimplemented!("don't have support for multiple files yet"),
    }

    let info_hash = get_info_hash(&metadata.info);
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
            println!("{failure_reason}");
            return;
        }
    };
    // now to connect to a peer

    let handshake = PeerHandshake::v1(info_hash, peer_id);
    let handshake_bytes = handshake.to_bytes();

    // TODO: handle incoming connections from peers

    // TODO: this should probably use threads to download files in parallel
    for peer in &peers {
        let stream = tokio::net::TcpStream::connect(peer.socket_addr)
            .await
            .expect("unable to connect to peer");
        /*
        tokio::spawn(async move {
            handle_peer(
                std::sync::Arc::new(metadata),
                stream,
                std::sync::Arc::new(Mutex::new(peer)),
            )
            .await
        });
        */
        handle_peer(&metadata, &handshake, stream, &peer).await;
    }

    // tokio::join!();
}

use tokio::net::TcpStream;

async fn handle_peer(
    metadata: &Metadata,
    handshake: &PeerHandshake,
    mut stream: TcpStream,
    peer: &Peer,
) {
    let handshake_bytes = handshake.to_bytes();

    println!("{:?}", peer.socket_addr);

    stream
        .write_all(&handshake_bytes)
        .await
        .expect("couldn't write to peer");

    // first read will be 68 bytes the majority of the time according to the bittorrent spec
    let mut parts = vec![0u8; 68];

    stream
        .read_exact(&mut parts)
        .await
        .expect("error reading from stream");

    let peer_response = PeerHandshake::from_bytes(&parts).expect("invalid peer response");
    // eprintln!("{peer_response:?}");
    println!("{}", hex::encode(peer_response.peer_id));
    if peer_response.info_hash != handshake.info_hash {
        println!("invalid info_hash received");
    }
    stream.shutdown().await.expect("shutdown call failed");
}

/// A struct representing messages between peers
/// All communication between peers in the BitTorrent protocol is communicated in Messages of this
/// format
#[repr(C)]
#[derive(Debug, Default)]
struct Message {
    /// The length of the entire message being transmitted
    length: u32,
    /// Optional parameter indicating the type of message being communciated
    /// This value is only None in a keep-alive message (i.e. length == 0)
    message_id: Option<MessageIds>,
    /// The payload being comminicated
    payload: Option<Vec<u8>>,
}

impl Message {
    pub fn keep_alive() -> Self {
        Self {
            length: 0,
            message_id: None,
            payload: None,
        }
    }

    pub fn choke() -> Self {
        Self {
            length: 1,
            message_id: Some(MessageIds::Choke),
            payload: None,
        }
    }

    pub fn unchoke() -> Self {
        Self {
            length: 1,
            message_id: Some(MessageIds::UnChoke),
            payload: None,
        }
    }

    pub fn interested() -> Self {
        Self {
            length: 1,
            message_id: Some(MessageIds::Interested),
            payload: None,
        }
    }

    pub fn not_interested() -> Self {
        Self {
            length: 1,
            message_id: Some(MessageIds::NotInterested),
            payload: None,
        }
    }

    pub fn have(index: u32) -> Self {
        Self {
            length: 5,
            message_id: Some(MessageIds::NotInterested),
            payload: Some(u32::to_be_bytes(index).to_vec()),
        }
    }

    pub fn bitfield(bitfield: &[u8]) -> Self {
        Self {
            length: 1 + (bitfield.len() as u32),
            message_id: Some(MessageIds::BitField),
            payload: Some(bitfield.to_vec()),
        }
    }

    pub fn request(index: u32, begin: u32, length: u32) -> Self {
        let mut payload: Vec<u8> = Vec::with_capacity(12);
        payload.extend_from_slice(&u32::to_be_bytes(index));
        payload.extend_from_slice(&u32::to_be_bytes(begin));
        payload.extend_from_slice(&u32::to_be_bytes(length));
        Self {
            length: 13,
            message_id: Some(MessageIds::Request),
            payload: Some(payload),
        }
    }

    pub fn piece(index: u32, begin: u32, block: &[u8]) -> Self {
        let mut payload: Vec<u8> = Vec::with_capacity(12);
        payload.extend_from_slice(&u32::to_be_bytes(index));
        payload.extend_from_slice(&u32::to_be_bytes(begin));
        payload.extend_from_slice(block);
        Self {
            length: 9 + block.len() as u32,
            message_id: Some(MessageIds::Piece),
            payload: Some(payload),
        }
    }

    pub fn cancel(index: u32, begin: u32, length: u32) -> Self {
        let mut payload: Vec<u8> = Vec::with_capacity(12);
        payload.extend_from_slice(&u32::to_be_bytes(index));
        payload.extend_from_slice(&u32::to_be_bytes(begin));
        payload.extend_from_slice(&u32::to_be_bytes(length));
        Self {
            length: 13,
            message_id: Some(MessageIds::Cancel),
            payload: Some(payload),
        }
    }

    pub fn port(listen_port: u16) -> Self {
        Self {
            length: 3,
            message_id: Some(MessageIds::Port),
            payload: Some(u16::to_be_bytes(listen_port).to_vec()),
        }
    }
}

/// Enum representing the type of messages supported by the BitTorrent protocol
#[repr(u8)]
#[derive(Debug)]
enum MessageIds {
    /// Indicates that the peer is choking the client
    Choke = 0,
    /// Indicates that the peer is unchoking the client
    UnChoke = 1,
    /// Indicates that a peer is interested in something that the client has (and vice versa)
    Interested = 2,
    /// Indicates that a peer is not interested in aynthing that the client has to offer (and vice
    /// versa)
    NotInterested = 3,
    /// Indicates that a peer/client has the piece indicated in the message payload
    Have = 4,
    /// Indicates a message containing a BitField representing the pieces that have been
    /// successfully downloaded.
    ///
    /// The high bit in the first byte of the payload corresponds to piece index 0. Bits that
    /// are cleared indicate a missing piece, and set bits indicate a valid and available piece.
    /// Spare bits at the end are set to zero.
    ///
    /// BitField messages can only be sent immediately after the peer handshake is completed,
    /// before any other messages are sent. It is optional, and need not be sent if a client has no
    /// pieces
    BitField = 5,
    /// Indicates a fixed-length message used to request a block from a peer/client.
    ///
    /// The payload contains the following information in order:
    ///   index  : u32 integer specifying the zero-based piece index
    ///   begin  : u32 integer specifying the zero-based byte offset within the piece
    ///   length : u32 integer specifying the requested length
    ///
    /// For more information about Request messages, see here:
    /// https://wiki.theory.org/BitTorrentSpecification#request:_.3Clen.3D0013.3E.3Cid.3D6.3E.3Cindex.3E.3Cbegin.3E.3Clength.3E
    Request = 6,
    /// Indicates a message containing piece data
    ///
    /// The payload contains the following information in order:
    ///   index : u32 integer specifying the zero-based piece index
    ///   begin : u32 integer specifying the zero-based byte offset within the piece
    ///   block : block of data, which is a subset of the piece specified by index
    Piece = 7,
    /// Indicates a fixed-length message to cancel block requests
    ///
    /// The payload is identical to that of the "Request" message
    ///
    /// It is typically used during "End Game". TODO
    Cancel = 8,
    /// Indicates the port that this peer's DHT node is listening on.
    /// Typically sent by newer versions of the Mainline that implements a DHT tracker.
    ///
    /// This peer should be inserted in the local routing table if DHT tracker is supported.
    Port = 9,
}
