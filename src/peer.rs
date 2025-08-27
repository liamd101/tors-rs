use std::io::SeekFrom;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, RwLock};

use rand::seq::IndexedRandom;

use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio_util::bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder};

use tracing::{debug, info};

use crate::{
    message::{BitField, Message, MessageCodec, MessageId},
    parsing::Metadata,
    ThreadUpdate,
};

const BLOCK_SIZE: u32 = 1 << 14;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct Peer {
    pub socket_addr: SocketAddr,
    pub peer_id: String,
    pub am_choking: bool,
    pub am_interested: bool,
    pub peer_choking: bool,
    pub peer_interested: bool,
}
impl Default for Peer {
    fn default() -> Self {
        Self {
            socket_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
            am_choking: true,
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
            peer_id: String::new(),
        }
    }
}

impl Peer {
    pub fn new(socket_addr: SocketAddr) -> Self {
        Self {
            socket_addr,
            ..Default::default()
        }
    }

    pub fn from_bytes(b: &[u8]) -> Option<Self> {
        if b.len() < 6 {
            return None;
        }
        let ip_addr = std::net::Ipv4Addr::new(b[0], b[1], b[2], b[3]);
        let port = u16::from_be_bytes([b[4], b[5]]);
        Some(Peer::new(SocketAddr::new(IpAddr::V4(ip_addr), port)))
    }
}

/// The handshake is a required message and must be the first message transmitted by the client
/// It is (49+len(pstr)) bytes long
#[derive(Debug, Clone)]
pub struct PeerHandshake {
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

impl PeerHandshake {
    pub fn to_bytes(&self) -> Vec<u8> {
        let total_len = 1 + self.pstr.len() + 8 + 20 + 20;
        let mut bytes: Vec<u8> = Vec::with_capacity(total_len);
        bytes.push(self.pstrlen);
        bytes.extend_from_slice(self.pstr.as_bytes());
        bytes.extend_from_slice(&self.reserved);
        bytes.extend_from_slice(&self.info_hash);
        bytes.extend_from_slice(&self.peer_id);
        bytes
    }

    pub fn from_bytes(value: &[u8]) -> Result<Self, String> {
        if value.is_empty() {
            return Err("Input cannot be empty".to_string());
        }
        let pstrlen = value[0];
        if value.len() != 49 + (pstrlen as usize) {
            return Err(format!("pstrlen/pstr is incorrect: {pstrlen}"));
        }
        let pstr = String::from_utf8_lossy(&value[1..=(pstrlen as usize)]).to_string();
        let reserved: [u8; 8] = value[(pstrlen as usize + 1)..(pstrlen as usize + 9)]
            .try_into()
            .map_err(|_| "Invalid reserved field length")?;
        let info_hash: [u8; 20] = value[(pstrlen as usize + 9)..(pstrlen as usize + 29)]
            .try_into()
            .map_err(|_| "Invalid info_hash field length")?;
        let peer_id: [u8; 20] = value[(pstrlen as usize + 29)..(pstrlen as usize + 49)]
            .try_into()
            .map_err(|_| "Invalid peer_id field length")?;
        Ok(PeerHandshake {
            pstrlen,
            pstr,
            reserved,
            info_hash,
            peer_id,
        })
    }

    pub fn v1(info_hash: [u8; 20], peer_id: [u8; 20]) -> Self {
        Self {
            info_hash,
            peer_id,
            pstrlen: 19,
            pstr: "BitTorrent protocol".to_string(),
            reserved: [0u8; 8],
        }
    }
}

pub async fn try_handshake(
    stream: &mut TcpStream,
    handshake: &PeerHandshake,
) -> Result<bool, std::io::Error> {
    let handshake_bytes = handshake.to_bytes();

    stream.write_all(&handshake_bytes).await?;

    // first read will be 68 bytes the majority of the time according to the bittorrent spec
    let mut parts = vec![0u8; 68];

    stream.read_exact(&mut parts).await?;

    let peer_response = PeerHandshake::from_bytes(&parts).expect("invalid peer response");
    Ok(peer_response.info_hash == handshake.info_hash)
}

struct Pieces {
    requested: Vec<Vec<bool>>,
}
impl Pieces {
    pub fn new(num_pieces: usize, num_blocks: usize) -> Self {
        Self {
            requested: vec![vec![false; num_blocks]; num_pieces],
        }
    }

    pub fn request(&mut self, piece: usize, block: usize) -> Option<bool> {
        let piece = self.requested.get_mut(piece)?;
        if piece.len() >= block {
            None
        } else {
            piece[block] = true;
            Some(true)
        }
    }
}

#[allow(unreachable_code)]
pub async fn handle_peer(
    _tx: Sender<ThreadUpdate>,
    mut _rx: Receiver<ThreadUpdate>,
    mut stream: TcpStream,
    metadata: Metadata,
    my_bitfield: Arc<RwLock<BitField>>,
) -> Result<(), std::io::Error> {
    debug!("handling peer connection");

    let mut choked = true;
    let mut am_interested = false;

    let num_blocks = metadata.info.piece_length / (1 << 14);
    let blocks_vec = (0..num_blocks).collect::<Vec<usize>>();

    let num_pieces: u64 = metadata.info.torr_type.len();
    let num_pieces = (num_pieces as usize).div_ceil(metadata.info.piece_length);

    // TODO: this will definitely need to be redone with mrsw design
    let mut peer_bitfield = BitField::with_settable(num_pieces);

    let mut message_codec = MessageCodec {};
    let mut write_buf = BytesMut::new();
    // this should be a Vec<(u32, u32)>
    // that way we can cancel previous requests that are no longer needed

    // This is a vector of pieces that have previously been requested
    let mut _requested: Pieces = Pieces::new(num_pieces, num_blocks);
    let mut requested: Option<(u32, u32)> = None;

    loop {
        let mut read_buf = BytesMut::zeroed(5);
        stream.read_exact(&mut read_buf).await?;
        let Some(message) = message_codec.decode(&mut read_buf).expect("invalid read") else {
            continue;
        };
        debug!("message received: {message:?}");
        let Some(message_id) = message.message_id else {
            continue;
        };

        match message_id {
            MessageId::Piece => {
                info!("peer is sending us piece data");
                let piece_index: u32 = stream.read_u32().await?;
                let begin: u32 = stream.read_u32().await?;
                info!("receiving piece={piece_index} offset={begin}");
                let mut file = tokio::fs::File::options()
                    .create(true)
                    .write(true)
                    .truncate(false)
                    .open(&metadata.info.name)
                    .await?;

                let current_len = file.metadata().await?.len();
                if current_len != 0 {
                    file.set_len(metadata.info.torr_type.len()).await?;
                }

                let piece_start = piece_index as u64 * metadata.info.piece_length as u64;
                file.seek(SeekFrom::Start(piece_start + begin as u64))
                    .await?;

                let reader = BufReader::new(&mut stream);
                let mut writer = BufWriter::new(&mut file);
                let mut piece_reader = reader.take(message.length as u64 - 9);
                let bytes_copied = tokio::io::copy_buf(&mut piece_reader, &mut writer).await?;
                writer.flush().await?;
                debug!(
                    "wrote {bytes_copied} bytes to {}",
                    piece_start + begin as u64
                );
                requested = None;
            }

            MessageId::BitField => {
                let mut payload = vec![0u8; message.length as usize - 1];
                stream.read_exact(&mut payload).await?;
                peer_bitfield =
                    BitField::new(payload, num_pieces).expect("peer has an impossible bitfield");
                debug!("bitfield={peer_bitfield:?}");
            }

            MessageId::Interested => {
                // TODO: send message to parent receiver that they are interested in a piece
                // from us
            }

            MessageId::Have => {
                let index: u32 = stream.read_u32().await?;
                peer_bitfield
                    .set(index as usize, true)
                    .expect("invalid index");
            }

            MessageId::UnChoke => choked = false,

            MessageId::Choke => choked = true,

            _ => todo!(),
        }

        /* If the peer has something that we want, have not sent
         * the peer an Interested message, send an interested message. */
        if my_bitfield
            .read()
            .unwrap()
            .has_other(&peer_bitfield)
            .expect("one of us has incorrect bitfield")
            && !am_interested
        {
            let message = Message {
                length: 1,
                message_id: Some(MessageId::Interested),
            };
            debug!("sending message: {message:?}");

            message_codec.encode(message, &mut write_buf)?;
            stream.write_all(&write_buf).await?;

            am_interested = true;
        }

        if !my_bitfield
            .read()
            .unwrap()
            .has_other(&peer_bitfield)
            .expect("one of us has incorrect bitfield")
        {
            let message = Message {
                length: 1,
                message_id: Some(MessageId::NotInterested),
            };
            debug!("sending message: {message:?}");

            message_codec.encode(message, &mut write_buf)?;
            stream.write_all(&write_buf).await?;

            am_interested = false;
        }

        if !choked && requested.is_none() {
            // want to send a single request message
            info!("peer has piece we want");
            let message = Message {
                length: 13,
                message_id: Some(MessageId::Request),
            };
            debug!("sending message: {message:?}");
            message_codec.encode(message, &mut write_buf)?;
            stream.write_all(&write_buf).await?;
            // now want to select random piece / block that we do not currently have
            let piece_options = peer_bitfield.set_bits();
            let piece = *piece_options.choose(&mut rand::rng()).unwrap() as u32;
            let block_num = *blocks_vec.choose(&mut rand::rng()).unwrap() as u32;
            requested = Some((piece, block_num));
            let block_size = calculate_block_size(
                piece,
                block_num,
                metadata.info.piece_length as u64,
                metadata.info.torr_type.len(),
                BLOCK_SIZE,
            );

            if block_size != BLOCK_SIZE {
                // TODO: may need to update block_num
            }

            let block_begin = block_num * BLOCK_SIZE;
            info!("requesting piece={piece} block={block_num} length={block_size}");
            stream.write_u32(piece).await?;
            stream.write_u32(block_begin).await?;
            stream.write_u32(block_size).await?;
        }
    }

    stream.shutdown().await
}

/// Helper function for computing the correct blocksize for a given piece and block pair
fn calculate_block_size(
    piece_index: u32,
    block_index: u32,
    piece_length: u64,
    total_file_size: u64,
    block_size: u32,
) -> u32 {
    let total_pieces = total_file_size.div_ceil(piece_length) as u32;
    let is_last_piece = piece_index == total_pieces - 1;

    let actual_piece_size = if is_last_piece {
        let last_piece_size = total_file_size % piece_length;
        if last_piece_size == 0 {
            piece_length
        } else {
            last_piece_size
        }
    } else {
        piece_length
    };

    let blocks_in_piece = actual_piece_size.div_ceil(block_size as u64);
    let is_last_block = block_index == blocks_in_piece as u32 - 1;

    if is_last_block {
        let remaining_bytes = actual_piece_size % block_size as u64;
        if remaining_bytes == 0 {
            block_size
        } else {
            remaining_bytes as u32
        }
    } else {
        block_size
    }
}
