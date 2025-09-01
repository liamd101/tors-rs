use std::io::SeekFrom;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, RwLock};

use rand::seq::IndexedRandom;

use anyhow::{Context, Error};

use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_util::bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder};

use tracing::{Instrument, debug, error, info, warn};

use crate::{
    ThreadUpdate,
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

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BlockState {
    UnRequested,
    Requested,
    Completed,
}

#[allow(dead_code)]
struct Pieces {
    num_total_blocks: usize,
    num_requested: usize,
    num_downloaded: usize,
    request_limit: usize,
    blocks: Vec<Vec<BlockState>>,
}
impl Pieces {
    pub fn from_file_info(file_length: u64, piece_length: u64) -> Self {
        let num_pieces = file_length.div_ceil(piece_length);
        let last_piece_begin = (num_pieces - 1) * piece_length;
        let last_piece_size = file_length - last_piece_begin;
        let last_piece_num_blocks = last_piece_size.div_ceil(BLOCK_SIZE as u64);
        let standard_num_blocks = piece_length.div_ceil(BLOCK_SIZE as u64);
        let mut blocks = vec![
            vec![BlockState::UnRequested; standard_num_blocks as usize];
            num_pieces as usize - 1
        ];
        blocks.push(vec![
            BlockState::UnRequested;
            last_piece_num_blocks as usize
        ]);
        Self {
            num_requested: 0,
            num_downloaded: 0,
            request_limit: 3,
            num_total_blocks: ((num_pieces - 1) * standard_num_blocks + last_piece_num_blocks)
                as usize,
            blocks,
        }
    }

    pub fn request(&mut self, piece: usize, block: usize) -> Option<bool> {
        let piece = self.blocks.get_mut(piece)?;
        if block >= piece.len() {
            None
        } else {
            piece[block] = BlockState::Requested;
            self.num_requested += 1;
            Some(true)
        }
    }

    pub fn finish_request(&mut self, piece: usize, block: usize) -> Option<bool> {
        let piece = self.blocks.get_mut(piece)?;
        if block >= piece.len() {
            None
        } else {
            if piece[block] == BlockState::Requested {
                self.num_requested -= 1;
            }
            piece[block] = BlockState::Completed;
            Some(true)
        }
    }

    pub fn request_new(&mut self, pieces: Vec<usize>) -> Option<(u32, u32)> {
        if self.num_requested >= self.request_limit {
            return None;
        }
        // want to create a Vec<(usize, usize)> where first index is in pieces and select randomly from there
        let blocks: Vec<Vec<BlockState>> = pieces
            .iter()
            .map(|&piece_idx| self.blocks[piece_idx].clone())
            .collect();
        let blocks: Vec<Vec<usize>> = blocks
            .iter()
            .map(|piece| {
                piece
                    .iter()
                    .enumerate()
                    .filter_map(|(block_idx, block_state)| match block_state {
                        BlockState::UnRequested => Some(block_idx),
                        _ => None,
                    })
                    .collect()
            })
            .collect();
        let blocks: Vec<(u32, u32)> = blocks
            .iter()
            .enumerate()
            .flat_map(|(piece_idx, piece)| {
                piece
                    .iter()
                    .map(move |&block_idx| (piece_idx as u32, block_idx as u32))
            })
            .collect();
        let out = blocks.choose(&mut rand::rng()).copied()?;
        self.blocks[out.0 as usize][out.1 as usize] = BlockState::Requested;
        self.num_requested += 1;
        Some(out)
    }

    pub fn complete_piece(&mut self, piece: u32) -> Option<bool> {
        let piece = self.blocks.get_mut(piece as usize)?;
        for block in piece.iter_mut() {
            if block == &BlockState::Requested {
                self.num_requested -= 1;
            }
            *block = BlockState::Completed;
        }
        Some(true)
    }
}

#[allow(unreachable_code)]
pub async fn handle_peer(
    tx: Sender<ThreadUpdate>,
    mut _rx: Receiver<ThreadUpdate>,
    stream: TcpStream,
    metadata: Metadata,
    my_bitfield: Arc<RwLock<BitField>>,
) -> Result<(), std::io::Error> {
    debug!("handling peer connection");

    let num_pieces: u64 = metadata
        .info
        .torr_type
        .len()
        .div_ceil(metadata.info.piece_length);

    // TODO: this will definitely need to be redone with mrsw design
    let peer_bitfield = Arc::new(RwLock::new(BitField::with_settable(num_pieces)));
    let choked = Arc::new(RwLock::new(true));

    let (read_stream, write_stream) = tokio::io::split(stream);

    let mut set = JoinSet::new();

    let current_span = tracing::Span::current();

    let read_tx = tx.clone();
    let read_rx = tx.subscribe();
    let read_metadata = metadata.clone();
    let read_choked = choked.clone();
    let read_peer_bitfield = peer_bitfield.clone();
    set.spawn(async move {
        match read_peer(
            read_tx,
            read_rx,
            read_stream,
            read_metadata,
            read_peer_bitfield,
            read_choked,
        )
        .instrument(current_span)
        .await
        {
            Ok(stream) => Ok(stream),
            Err(e) => {
                error!("{e}");
                Err(e)
            }
        }
    });

    let current_span = tracing::Span::current();
    let write_tx = tx.clone();
    let write_rx = tx.subscribe();
    let write_metadata = metadata.clone();
    let write_bitfield = my_bitfield.clone();
    let write_choked = choked.clone();
    let write_peer_bitfield = peer_bitfield.clone();
    set.spawn(async move {
        match write_peer(
            write_tx,
            write_rx,
            write_stream,
            write_metadata,
            write_bitfield,
            write_peer_bitfield,
            write_choked,
        )
        .instrument(current_span)
        .await
        {
            Ok(stream) => Ok(stream),
            Err(e) => {
                error!("{e}");
                Err(e)
            }
        }
    });

    let mut read_stream: Option<tokio::io::ReadHalf<TcpStream>> = None;
    let mut write_stream: Option<tokio::io::WriteHalf<TcpStream>> = None;

    while let Some(result) = set.join_next().await {
        if let Ok(Ok(stream)) = result {
            match stream {
                SplitStream::Write(stream) => write_stream = Some(stream),
                SplitStream::Read(stream) => read_stream = Some(stream),
            }
        }
    }

    if let (Some(read), Some(write)) = (read_stream, write_stream) {
        let mut stream = read.unsplit(write);
        stream.shutdown().await
    } else {
        Err(std::io::Error::other("Read/Write thread failed"))
    }
}

enum SplitStream {
    Read(tokio::io::ReadHalf<TcpStream>),
    Write(tokio::io::WriteHalf<TcpStream>),
}

#[allow(unreachable_code)]
async fn read_peer(
    tx: Sender<ThreadUpdate>,
    mut _rx: Receiver<ThreadUpdate>,
    mut stream: tokio::io::ReadHalf<TcpStream>,
    metadata: Metadata,
    peer_bitfield: Arc<RwLock<BitField>>,
    choked: Arc<RwLock<bool>>,
) -> Result<SplitStream, Error> {
    let num_pieces = metadata
        .info
        .torr_type
        .len()
        .div_ceil(metadata.info.piece_length);

    let mut message_codec = MessageCodec {};
    // this should be a Vec<(u32, u32)>
    // that way we can cancel previous requests that are no longer needed

    loop {
        let mut read_buf = BytesMut::zeroed(5);
        let bytes_read = stream
            .read_exact(&mut read_buf)
            .await
            .context("reading message header")?;
        if bytes_read != 5 {
            warn!("read {bytes_read} of message header");
            continue;
        }
        let Some(message) = message_codec.decode(&mut read_buf).expect("invalid read") else {
            continue;
        };
        info!("message received: {message:?}");
        let Some(message_id) = message.message_id else {
            continue;
        };

        match message_id {
            MessageId::Piece => {
                let piece_index: u32 = stream.read_u32().await.context("reading piece index")?;
                let begin: u32 = stream.read_u32().await.context("reading block offset")?;
                info!("receiving piece={piece_index} offset={begin}");
                let mut file = tokio::fs::File::options()
                    .create(true)
                    .write(true)
                    .truncate(false)
                    .open(&metadata.info.name)
                    .await
                    .with_context(|| "opening output file")?;

                let current_len = file.metadata().await?.len();
                if current_len != 0 {
                    file.set_len(metadata.info.torr_type.len()).await?;
                }

                let piece_start = piece_index as u64 * metadata.info.piece_length;
                file.seek(SeekFrom::Start(piece_start + begin as u64))
                    .await
                    .with_context(|| "seeking to {piece_start}")?;

                let mut piece_data = vec![0u8; message.length as usize - 9];
                let bytes_read = stream
                    .read_exact(&mut piece_data)
                    .await
                    .context("reading piece data")?;
                file.write_all(&piece_data)
                    .await
                    .context("writing piece data to file")?;

                info!("wrote {bytes_read} bytes to {}", piece_start + begin as u64);

                tx.send(ThreadUpdate::Downloaded(piece_index, begin / BLOCK_SIZE))?;
            }

            MessageId::BitField => {
                let mut payload = vec![0u8; message.length as usize - 1];
                stream.read_exact(&mut payload).await?;
                let sent_bitfield =
                    BitField::new(payload, num_pieces).expect("peer has an impossible bitfield");
                peer_bitfield
                    .write()
                    .unwrap()
                    .update(&sent_bitfield)
                    .context("updating peer bitfield from sent")?;
                debug!("bitfield={peer_bitfield:?}");
            }

            MessageId::Interested => {
                // TODO: send message to parent receiver that they are interested in a piece
                // from us
            }

            MessageId::Have => {
                let index: u32 = stream.read_u32().await?;
                peer_bitfield
                    .write()
                    .unwrap()
                    .set(index as usize, true)
                    .expect("invalid index");
            }

            MessageId::UnChoke => {
                *choked.write().unwrap() = false;
            }

            MessageId::Choke => *choked.write().unwrap() = true,

            _ => todo!(),
        }
    }
    Ok(SplitStream::Read(stream))
}

#[allow(unreachable_code)]
async fn write_peer(
    _tx: Sender<ThreadUpdate>,
    mut rx: Receiver<ThreadUpdate>,
    mut stream: tokio::io::WriteHalf<TcpStream>,
    metadata: Metadata,
    my_bitfield: Arc<RwLock<BitField>>,
    peer_bitfield: Arc<RwLock<BitField>>,
    choked: Arc<RwLock<bool>>,
) -> Result<SplitStream, Error> {
    let mut am_interested = false;

    let mut message_codec = MessageCodec {};

    // TODO: support `update(&BitField)`
    let mut requested: Pieces =
        Pieces::from_file_info(metadata.info.torr_type.len(), metadata.info.piece_length);

    loop {
        let mut write_buf = BytesMut::new();

        match rx.try_recv() {
            Err(tokio::sync::broadcast::error::TryRecvError::Empty) => {}
            Err(tokio::sync::broadcast::error::TryRecvError::Closed) => break,
            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => {
                todo!()
            }
            Ok(ThreadUpdate::Downloaded(piece, block)) => {
                match requested.finish_request(piece as usize, block as usize) {
                    Some(_) => debug!("updated requested pieces"),
                    None => warn!("unable to process piece={piece} block={block}"),
                }
            }
            Ok(ThreadUpdate::Completed(piece)) => {
                requested.complete_piece(piece);
                let message = Message {
                    length: 1,
                    message_id: Some(MessageId::Have),
                };
                info!("sending message: {message:?}");

                message_codec.encode(message, &mut write_buf)?;
                stream
                    .write_all(&write_buf)
                    .await
                    .context("writing Interested message")?;
                stream
                    .write_u32(piece)
                    .await
                    .context("writing Have payload")?;
            }
        }

        let choked = *choked.read().unwrap();
        /* If the peer has something that we want, have not sent
         * the peer an Interested message, send an interested message. */
        if my_bitfield
            .read()
            .unwrap()
            .has_other(&peer_bitfield.read().unwrap())
            .expect("one of us has incorrect bitfield")
            && !am_interested
        {
            let message = Message {
                length: 1,
                message_id: Some(MessageId::Interested),
            };
            debug!("sending message: {message:?}");

            message_codec.encode(message, &mut write_buf)?;
            stream
                .write_all(&write_buf)
                .await
                .context("writing Interested message")?;

            am_interested = true;
        }

        if !my_bitfield
            .read()
            .unwrap()
            .has_other(&peer_bitfield.read().unwrap())
            .expect("one of us has incorrect bitfield")
            && am_interested
        {
            let message = Message {
                length: 1,
                message_id: Some(MessageId::NotInterested),
            };
            debug!("sending message: {message:?}");

            message_codec.encode(message, &mut write_buf)?;
            stream
                .write_all(&write_buf)
                .await
                .context("writing NotInterested message")?;

            am_interested = false;
        }

        let peer_has = peer_bitfield.read().unwrap().set_bits();
        if !choked && let Some((piece, block_num)) = requested.request_new(peer_has) {
            // want to send a single request message
            let message = Message {
                length: 13,
                message_id: Some(MessageId::Request),
            };
            debug!("sending message: {message:?}");
            message_codec.encode(message, &mut write_buf)?;
            stream
                .write_all(&write_buf)
                .await
                .context("writing Request header")?;
            // now want to select random piece / block that we do not currently have
            let block_size = calculate_block_size(
                piece,
                block_num,
                metadata.info.piece_length,
                metadata.info.torr_type.len(),
            );

            let block_begin = block_num * BLOCK_SIZE;
            info!("requesting piece={piece} block_begin={block_begin} length={block_size}");
            stream
                .write_u32(piece)
                .await
                .context("writing piece number")?;
            stream
                .write_u32(block_begin)
                .await
                .context("writing byte offset")?;
            stream
                .write_u32(block_size)
                .await
                .context("writing block_size")?;
        }
    }
    Ok(SplitStream::Write(stream))
}

/// Helper function for computing the correct blocksize for a given piece and block pair
fn calculate_block_size(
    piece_index: u32,
    block_index: u32,
    piece_length: u64,
    total_file_size: u64,
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

    let blocks_in_piece = actual_piece_size.div_ceil(BLOCK_SIZE as u64);
    let is_last_block = block_index == blocks_in_piece as u32 - 1;

    if is_last_block {
        let remaining_bytes = actual_piece_size % BLOCK_SIZE as u64;
        if remaining_bytes == 0 {
            BLOCK_SIZE
        } else {
            remaining_bytes as u32
        }
    } else {
        BLOCK_SIZE
    }
}
