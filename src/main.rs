use sha1::{Digest, Sha1};
use std::collections::HashMap;
use std::path::PathBuf;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::task::JoinSet;
use tokio_util::bytes::{Buf, BytesMut};
use tokio_util::codec::Decoder;

use tracing::{Instrument, debug, error, info, warn};
use tracing_subscriber::{self, EnvFilter};

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
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("debug"))
        .init();

    // let testing_string: String = "d7:meaningi42e4:wiki7:bencodee".into();
    let torr_path: PathBuf = PathBuf::from("sample.torrent");

    if !torr_path.exists() {
        error!("file does not exist");
        return;
    }

    let data: &[u8] = &std::fs::read(torr_path).expect("unable to read file");
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
            error!("{failure_reason}");
            return;
        }
    };
    debug!("tracker supplied {} peers", peers.len());
    // now to connect to a peer

    let handshake = PeerHandshake::v1(info_hash, peer_id);

    // TODO: maybe specify a limit of peers we can connect to at once?
    let mut set = JoinSet::new();

    // TODO: this should probably use threads to download files in parallel
    for peer in &peers {
        let stream = tokio::net::TcpStream::connect(peer.socket_addr)
            .await
            .expect("unable to connect to peer");

        let metadata = metadata.clone();
        let handshake = handshake.clone();
        let peer_addr = peer.socket_addr;

        set.spawn(async move {
            handle_peer(metadata, handshake, stream, peer_addr).await;
        });
    }

    while set.join_next().await.is_some() {}
}

use tokio::net::TcpStream;

async fn handle_peer(
    metadata: Metadata,
    handshake: PeerHandshake,
    mut stream: TcpStream,
    peer_addr: std::net::SocketAddr,
) {
    let span = tracing::info_span!("peer", peer_addr = %peer_addr);
    async move {
        debug!("handling peer connection");

        let handshake_bytes = handshake.to_bytes();

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
        if peer_response.info_hash != handshake.info_hash {
            warn!("invalid info_hash received. closing connection");
            stream.shutdown().await.expect("shutdown call failed");
            return;
        }

        // TODO: helper function for making bitfield
        let length = match metadata.info.torr_type {
            parsing::FileTypes::SingleFile { length } => length,
            _ => unimplemented!("don't have support for multiple files yet"),
        };
        // round up number of pieces
        let mut message_decoder = MessageDecoder {};
        /// open file, check if each piece has been fully downloaded? probably should have a parent
        /// thread be in charge of this part though
        let mut buf = BytesMut::new();
        loop {
            let len = stream
                .read_buf(&mut buf)
                .await
                .expect("didn't receive all bytes");
            let Some(message) = message_decoder.decode_eof(&mut buf).expect("invalid read") else {
                continue;
            };
            if message.message_id.is_none() {
                continue;
            }
            debug!("message header received: {message:?}");
            let payload = buf[..message.length as usize - 1].to_vec();
            buf.advance(message.length as usize - 1);
            debug!("message payload: {payload:?}");
        }

        stream.shutdown().await.expect("shutdown call failed");
    }
    .instrument(span)
    .await
}

/// A struct representing message headers between peers
/// All communication between peers in the BitTorrent protocol is communicated in Messages of this
/// format
#[repr(C)]
#[derive(Debug, Default)]
struct Message {
    /// The length of the entire message being transmitted
    length: u32,
    /// Optional parameter indicating the type of message being communciated
    /// This value is only None in a keep-alive message (i.e. length == 0)
    message_id: Option<MessageId>,
}

/// Enum representing the type of messages supported by the BitTorrent protocol
#[repr(u8)]
#[derive(Debug)]
enum MessageId {
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

impl TryFrom<u8> for MessageId {
    type Error = ();
    fn try_from(val: u8) -> Result<Self, Self::Error> {
        match val {
            0 => Ok(MessageId::Choke),
            1 => Ok(MessageId::UnChoke),
            2 => Ok(MessageId::Interested),
            3 => Ok(MessageId::NotInterested),
            4 => Ok(MessageId::Have),
            5 => Ok(MessageId::BitField),
            6 => Ok(MessageId::Request),
            7 => Ok(MessageId::Piece),
            8 => Ok(MessageId::Cancel),
            9 => Ok(MessageId::Port),
            _ => Err(()),
        }
    }
}

struct MessageDecoder {}

const MAX_MESSAGE_LEN: usize = 1 << 16;

impl Decoder for MessageDecoder {
    type Item = Message;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            // Not enough data to read length marker
            return Ok(None);
        }

        let mut length_bytes = [0u8; 4];
        length_bytes.copy_from_slice(&src[..4]);
        let length = u32::from_be_bytes(length_bytes);
        if length == 0 {
            return Ok(Some(Message {
                length: 0,
                message_id: None,
            }));
        }
        if src.len() < 4 + 1 {
            src.reserve(4 + 1);
            return Ok(None);
        }

        if length as usize > MAX_MESSAGE_LEN {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Frame of length {} is too large.", length),
            ));
        }

        let Ok(message_id) = MessageId::try_from(src[4]) else {
            return Err(Self::Error::from(std::io::ErrorKind::Other));
        };

        src.advance(4 + 1);

        Ok(Some(Message {
            length,
            message_id: Some(message_id),
        }))
    }
}
