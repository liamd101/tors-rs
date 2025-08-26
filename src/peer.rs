use std::io::SeekFrom;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use rand::seq::IndexedRandom;

use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio_util::bytes::{Buf, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use tracing::{Instrument, debug, error, info};

use crate::{
    message::{BitField, Message, MessageCodec, MessageId},
    parsing::Metadata,
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

async fn try_handshake(
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

#[allow(unreachable_code)]
pub async fn handle_peer(
    metadata: Metadata,
    handshake: PeerHandshake,
    my_bitfield: Arc<BitField>,
    mut stream: TcpStream,
    peer_addr: std::net::SocketAddr,
) -> Result<(), std::io::Error> {
    let span = tracing::info_span!("peer", peer_addr = %peer_addr);
    async move {
        debug!("handling peer connection");

        match try_handshake(&mut stream, &handshake).await {
            Ok(true) => {}
            Ok(false) => {
                info!("peer handshake failed. severing connection");
                return stream.shutdown().await;
            }
            Err(e) => {
                error!("{e}");
                info!("peer handshake failed. severing connection");
                return stream.shutdown().await;
            }
        }

        let mut choked = true;
        let mut am_interested = false;
        let num_blocks = metadata.info.piece_length / (1 << 14);
        let num_pieces = (metadata.info.torr_type.len() as usize + metadata.info.piece_length - 1)
            / metadata.info.piece_length;
        debug!("num_blocks={num_blocks}");
        let blocks_vec = (0..num_blocks).collect::<Vec<usize>>();

        // TODO: this will definitely need to be redone with mrsw design
        let mut peer_bitfield: Option<BitField> = None;

        let mut message_codec = MessageCodec {};
        let mut write_buf = BytesMut::new();
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
                    // this probably goes to its own function
                    info!("peer is sending us piece data");
                    let piece_index: u32 = stream.read_u32().await?;
                    let begin: u32 = stream.read_u32().await?;
                    info!("receiving piece={piece_index} offset={begin}");
                    /* let filename = format!("{}-{}", metadata.info.name, piece_index); */
                    let mut file = tokio::fs::File::options()
                        .create(true)
                        .write(true)
                        .truncate(false)
                        .open(&metadata.info.name)
                        /* .open(filename) */
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
                    if peer_bitfield.is_some() {
                        // TODO: error handle this case
                        // BitField message can only be sent once
                        continue;
                    }
                    let mut read_buf = BytesMut::zeroed(message.length as usize - 1);
                    stream.read_exact(&mut read_buf).await?;
                    let payload = read_buf.to_vec();
                    read_buf.advance(message.length as usize - 1);
                    let bitfield = BitField::new(payload, num_pieces)
                        .expect("invalid payload and settable bits combo");
                    peer_bitfield = Some(bitfield);
                    debug!("bitfield={peer_bitfield:?}");
                }

                MessageId::Interested => {
                    // TODO: send message to parent receiver that they are interested in a piece
                    // from us
                }

                MessageId::Have => {
                    let index: u32 = stream.read_u32().await?;
                    let p_bitfield = peer_bitfield.unwrap_or(BitField::with_settable(num_pieces));
                    peer_bitfield =
                        Some(p_bitfield.set(index as usize, true).expect("invalid index"));
                }

                MessageId::UnChoke => choked = false,

                MessageId::Choke => choked = true,

                _ => continue,
            }

            debug!("peer_bitfield={peer_bitfield:?}");

            /* If the peer has something that we want, have not sent
             * the peer an Interested message, send an interested message. */
            if peer_bitfield.is_some()
                && my_bitfield
                    .has_other(peer_bitfield.as_ref().unwrap())
                    .expect("one of us has incorrect bitfield")
                && !am_interested
            /* && !peer.am_interested */
            {
                let message = Message {
                    length: 1,
                    message_id: Some(MessageId::Interested),
                };
                debug!("sending message: {message:?}");

                message_codec.encode(message, &mut write_buf)?;
                stream.write_all(&write_buf).await?;

                /* peer.am_interested = true; */
                am_interested = true;
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
                let piece_options = peer_bitfield.as_ref().unwrap().set_bits();
                let piece = *piece_options.choose(&mut rand::rng()).unwrap() as u32;
                let block_num = *blocks_vec.choose(&mut rand::rng()).unwrap() as u32;
                requested = Some((piece, block_num));
                let block_size = calculate_block_size(
                    piece,
                    block_num,
                    metadata.info.piece_length,
                    metadata.info.torr_type.len(),
                    BLOCK_SIZE,
                );
                if block_size != BLOCK_SIZE {
                    // TODO: may need to update block_num
                }

                let block_begin = block_num * BLOCK_SIZE;
                info!("requesting piece={piece} block={block_num} length={block_size}");
                let mut payload = vec![];
                payload.extend_from_slice(&u32::to_be_bytes(piece));
                payload.extend_from_slice(&u32::to_be_bytes(block_begin));
                payload.extend_from_slice(&u32::to_be_bytes(block_size));
                stream.write_all(&payload).await?;
            }
        }

        stream.shutdown().await
    }
    .instrument(span)
    .await
}

/// Helper function for computing the correct blocksize for a given piece and block pair
fn calculate_block_size(
    piece_index: u32,
    block_index: u32,
    piece_length: usize,
    total_file_size: u64,
    block_size: u32,
) -> u32 {
    let total_pieces = ((total_file_size + piece_length as u64 - 1) / piece_length as u64) as u32;
    let is_last_piece = piece_index == total_pieces - 1;

    let actual_piece_size = if is_last_piece {
        let last_piece_size = total_file_size % piece_length as u64;
        if last_piece_size == 0 {
            piece_length as u64
        } else {
            last_piece_size
        }
    } else {
        piece_length as u64
    };

    let blocks_in_piece = (actual_piece_size + block_size as u64 - 1) / block_size as u64;
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
