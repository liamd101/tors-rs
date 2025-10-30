use super::{
    BLOCK_SIZE, ChildUpdates, PeerState,
    message::{Message, MessageId},
};
use crate::ThreadUpdate;

use anyhow::{Context, Result};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::select;
use tokio::sync::broadcast;

use tracing::debug;

pub(super) async fn read_peer(
    tx: broadcast::Sender<ThreadUpdate>,
    mut rx: broadcast::Receiver<ThreadUpdate>,
    mut stream: tokio::io::ReadHalf<TcpStream>,
    mut peer_state: PeerState,
) -> Result<ChildUpdates> {
    loop {
        select! {
            channel_result = rx.recv() => {
                if read_peer::handle_channel_message(channel_result)? {
                    break
                }
            }

            peer_result = read_peer::read_message(&mut stream) => {
                if let Some(message) = peer_result? {
                    read_peer::process_message(
                        message,
                        &tx,
                        &mut stream,
                        &mut peer_state,
                    )
                    .await?;
                }
            }
        }
    }

    Ok(ChildUpdates::Read(stream))
}

pub(super) async fn write_peer(
    tx: broadcast::Sender<ThreadUpdate>,
    mut rx: broadcast::Receiver<ThreadUpdate>,
    mut stream: tokio::io::WriteHalf<TcpStream>,
    mut peer_state: PeerState,
) -> Result<ChildUpdates> {
    let mut am_interested = false;
    let mut requested = super::pieces::PieceTracker::from_file_info(
        peer_state.metadata.info.torr_type.len(),
        peer_state.metadata.info.piece_length,
    );
    let my_bitfield = peer_state.my_bitfield.read().await;
    requested.update(&*my_bitfield);

    if peer_state.reserved.supports_fast() {
        if my_bitfield.all() {
            stream.write_all(&Message::have_all().as_bytes()).await?;
            debug!("sent HaveAll");
        } else if !my_bitfield.any() {
            stream.write_all(&Message::have_none().as_bytes()).await?;
            debug!("sent HaveNone");
        }
    } else if my_bitfield.any() {
        write_peer::send_bitfield(&mut stream, &peer_state).await?;
        debug!("sending my bitfield");
    }

    // explicitly drop my_bitfield here so that other threads can actually access it
    drop(my_bitfield);
    stream
        .write_all(&Message::unchoke().as_bytes())
        .await
        .context("unchoking peer")?;
    peer_state
        .am_choking
        .store(false, std::sync::atomic::Ordering::Release);

    loop {
        select! {
            channel_result = rx.recv() => {
                if write_peer::process_message(channel_result, &mut stream, &peer_state, &mut requested).await? {
                    break;
                }
            }

            // if peer has requested something from us, and we have it, send the piece data
            Some((piece_idx, begin, data_len)) = peer_state.request_queue.recv() => {
                if peer_state.am_choking.load(std::sync::atomic::Ordering::Relaxed) {
                    continue;
                }
                // using this crate is so fucking stupid sometimes
                if !peer_state.my_bitfield.read().await.get(piece_idx as usize).as_deref().unwrap_or(&false) {
                    // if the peer requests something we don't have, we should sever the connection
                    tx.send(ThreadUpdate::Disconnect)?;
                    break;
                }
                let data = peer_state
                    .metadata
                    .get_piece_data(piece_idx as u64, begin as u64, data_len as u64)
                    .await
                    .context("getting piece data")?;
                stream.write_all(&Message {
                    length: data.len() as u32 + 9,
                    message_id: Some(MessageId::Piece),
                }.as_bytes()).await?;
                stream.write_u32(piece_idx).await.context("sending piece index")?;
                stream.write_u32(begin).await.context("sending begin")?;
                stream.write_all(&data).await.context("writing piece data")?;
                debug!("sent piece_idx={piece_idx} begin={begin} data_len={data_len}");
            }

            _ = tokio::time::sleep(tokio::time::Duration::from_millis(10)) => {
                if *peer_state.peer_choking.0.lock().await {
                    continue;
                }
                write_peer::send_one_round(
                   &mut stream,
                   &peer_state,
                   &mut requested,
                ).await?;
            }

            _ = peer_state.am_interested.1.notified() => {
                let new_interest = peer_state.am_interested.0.lock().await;
                if *new_interest ^ am_interested {
                    if *new_interest {
                        stream.write_all(&Message::interested().as_bytes()).await.context("Writing interested")?;
                        stream.flush().await.context("Flushing interested")?;
                        debug!("sending interested message");
                    } else {
                        stream.write_all(&Message::not_interested().as_bytes()).await.context("Writing not interested")?;
                        stream.flush().await.context("Flushing interested")?;
                        debug!("sending not-interested message");
                    }
                }
                am_interested = *new_interest;
            }
        }
    }

    Ok(ChildUpdates::Write(stream))
}

#[allow(unreachable_patterns)]
mod read_peer {
    use crate::{
        ThreadUpdate,
        parsing::TorrentType,
        peer::{
            BLOCK_SIZE,
            message::{Message, MessageId},
            state::PeerState,
        },
        torrent::OUTPUT_DIR,
    };

    use std::io::SeekFrom;
    use std::sync::atomic::Ordering;

    use anyhow::{Context, Result};
    use bitvec::prelude::*;
    use tokio::{
        io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
        net::TcpStream,
        sync::broadcast,
    };
    use tracing::{debug, error, warn};

    pub(super) fn handle_channel_message(
        message: Result<ThreadUpdate, broadcast::error::RecvError>,
    ) -> anyhow::Result<bool> {
        match message {
            Err(broadcast::error::RecvError::Lagged(_)) => {
                error!("we are lagged somehow");
                Err(anyhow::anyhow!("Stream lagged"))
            }
            Err(broadcast::error::RecvError::Closed) => {
                warn!("Write half exited. Read half exiting.");
                Ok(true)
            }
            Ok(ThreadUpdate::Disconnect) => Ok(true),
            _ => Ok(false),
        }
    }

    pub(super) async fn read_message(
        stream: &mut tokio::io::ReadHalf<TcpStream>,
    ) -> anyhow::Result<Option<Message>> {
        let mut length_bytes = [0u8; 4];

        let bytes_read = stream.read_exact(&mut length_bytes).await?;
        if bytes_read != 4 {
            warn!("read {bytes_read} of message header");
            return Ok(None);
        }

        let length = u32::from_be_bytes(length_bytes);
        if length == 0 {
            debug!("Keep alive reeived. continuing");
            return Ok(None);
        }

        let message_id = stream.read_u8().await?;
        let message_id = MessageId::try_from(message_id).with_context(|| {
            format!("message with length {length} and MessageID {message_id} received")
        })?;

        Ok(Some(Message {
            length,
            message_id: Some(message_id),
        }))
    }

    pub(super) async fn process_message(
        message: Message,
        tx: &broadcast::Sender<ThreadUpdate>,
        stream: &mut tokio::io::ReadHalf<TcpStream>,
        peer_state: &mut PeerState,
    ) -> anyhow::Result<()> {
        let Some(message_id) = message.message_id else {
            return Ok(());
        };

        debug!("Received message: {message:?}");

        match message_id {
            MessageId::Choke | MessageId::UnChoke => {
                *peer_state.peer_choking.0.lock().await = matches!(message_id, MessageId::Choke);
                peer_state.peer_choking.1.notify_one();
            }

            MessageId::Interested | MessageId::NotInterested => {
                peer_state.peer_interested.store(
                    matches!(message_id, MessageId::Interested),
                    Ordering::Relaxed,
                );
            }

            MessageId::Have => {
                let index: u32 = stream.read_u32().await?;
                let prev = peer_state
                    .peer_bitfield
                    .write()
                    .await
                    .replace(index as usize, true);
                if !prev {
                    peer_state.am_interested.1.notify_one();
                    *peer_state.am_interested.0.lock().await = true;
                }
            }

            MessageId::BitField => {
                let mut payload = vec![0u8; message.length as usize - 1];
                stream.read_exact(&mut payload).await?;
                let received_bitfield = BitVec::<u8, Msb0>::from_slice(&payload)
                    [..peer_state.metadata.num_pieces()]
                    .to_bitvec();
                let mut peer_bitfield = peer_state.peer_bitfield.write().await;
                // if peer has pieces we don't, notify write half
                for set_bit in received_bitfield.iter_ones() {
                    peer_bitfield.set(set_bit, true);
                }
                drop(peer_bitfield);
                if peer_state.should_be_interested().await {
                    *peer_state.am_interested.0.lock().await = true;
                    peer_state.am_interested.1.notify_one();
                }
            }

            MessageId::Request => {
                let piece_index: u32 = stream.read_u32().await.context("reading piece index")?;
                let begin: u32 = stream.read_u32().await.context("reading piece index")?;
                let data_len: u32 = stream.read_u32().await.context("reading piece index")?;
                peer_state
                    .request_queue
                    .send((piece_index, begin, data_len))
                    .await?;
                debug!(
                    "peer requested piece_index={piece_index} begin={begin} data_len={data_len}"
                );
            }

            MessageId::Cancel => {
                let piece_index: u32 = stream.read_u32().await.context("reading piece index")?;
                let begin: u32 = stream.read_u32().await.context("reading block begin")?;
                stream.read_u32().await.context("reading data len")?;
                if peer_state.am_choking.load(Ordering::Relaxed) {
                    return Ok(());
                }
                peer_state
                    .request_queue
                    .send((piece_index, begin, 0))
                    .await
                    .context("Sending cancel message")?;
            }

            MessageId::Piece => {
                let output_dir = match OUTPUT_DIR.get().unwrap() {
                    Some(dir) => dir.clone(),
                    None => match peer_state.metadata.info.torr_type {
                        TorrentType::MultiFile { .. } => peer_state.metadata.info.name.clone(),
                        TorrentType::SingleFile { .. } => "out".to_string(),
                    },
                };

                let piece_index: u32 = stream.read_u32().await.context("reading piece index")?;
                let begin: u32 = stream.read_u32().await.context("reading block offset")?;
                let data_len: u32 = message.length - 9;
                let mut piece_data = vec![0u8; data_len as usize];
                let bytes_read = stream
                    .read_exact(&mut piece_data)
                    .await
                    .context("reading piece data")?;
                if bytes_read != data_len as usize {
                    error!(
                        "read {bytes_read} bytes instead of {data_len} bytes. closing connection."
                    );
                    return Ok(()); // TODO: return error here
                }

                let mut piece_position = 0;
                for (file, file_offset, bytes_to_write) in peer_state
                    .metadata
                    .file_info_from_piece_block(piece_index as u64, begin as u64, data_len as u64)?
                {
                    let mut filename = vec![output_dir.clone()];
                    filename.extend_from_slice(&file.path);
                    let filename = filename.join(std::path::MAIN_SEPARATOR_STR);
                    let mut out_file = tokio::fs::File::options()
                        .create(true)
                        .write(true)
                        .truncate(false)
                        .open(&filename)
                        .await
                        .with_context(|| format!("opening output file {filename}"))?;
                    if out_file.metadata().await?.len() == 0 {
                        out_file.set_len(file.length).await?;
                    }
                    out_file
                        .seek(SeekFrom::Start(file_offset))
                        .await
                        .with_context(|| format!("seeking to {file_offset}"))?;
                    out_file
                        .write_all(&piece_data[piece_position..][..bytes_to_write as usize])
                        .await
                        .context("writing piece data to file")?;
                    debug!(filename=?filename,"wrote {bytes_to_write} bytes to {file_offset}");
                    piece_position += bytes_to_write as usize;
                }
                tx.send(ThreadUpdate::Downloaded(piece_index, begin / BLOCK_SIZE))?;
            }

            MessageId::Port => {
                warn!("Port message received. This is unhandled.");
                stream.read_u16().await.context("Reading peer DHT port")?;
            }

            MessageId::HaveAll => {
                if !peer_state.reserved.supports_fast() {
                    tx.send(ThreadUpdate::Disconnect)
                        .context("Sending disconnect message")?;
                }
                // if the peer has the entire file and we are missing some, then we should be
                // interested
                peer_state.peer_bitfield.write().await.fill(true);
                if !peer_state.my_bitfield.read().await.all() {
                    peer_state.am_interested.1.notify_one();
                    *peer_state.am_interested.0.lock().await = true;
                }
            }

            MessageId::HaveNone => {
                if !peer_state.reserved.supports_fast() {
                    tx.send(ThreadUpdate::Disconnect)
                        .context("Sending disconnect message")?;
                }
                peer_state.peer_bitfield.write().await.fill(false);
                // no matter what, we are not interested in this peer
                peer_state.am_interested.1.notify_one();
                *peer_state.am_interested.0.lock().await = false;
            }

            MessageId::SuggestPiece => {
                if !peer_state.reserved.supports_fast() {
                    tx.send(ThreadUpdate::Disconnect)
                        .context("Sending disconnect message")?;
                }
            }

            MessageId::RejectRequest => {
                if !peer_state.reserved.supports_fast() {
                    tx.send(ThreadUpdate::Disconnect)
                        .context("Sending disconnect message")?;
                }
            }

            MessageId::AllowedFast => {
                if !peer_state.reserved.supports_fast() {
                    tx.send(ThreadUpdate::Disconnect)
                        .context("Sending disconnect message")?;
                }
            }

            _ => todo!(),
        }

        Ok(())
    }
}

mod write_peer {

    use super::calculate_block_size;
    use crate::{
        ThreadUpdate,
        peer::{
            BLOCK_SIZE,
            message::{Message, MessageId},
            pieces::PieceTracker,
            state::PeerState,
        },
    };

    use anyhow::{Context, Result};
    use tokio::{io::AsyncWriteExt, net::TcpStream, sync::broadcast};
    use tracing::{debug, error, info, warn};

    pub(super) async fn process_message(
        update: Result<ThreadUpdate, broadcast::error::RecvError>,
        stream: &mut tokio::io::WriteHalf<TcpStream>,
        peer_state: &PeerState,
        requested: &mut PieceTracker,
    ) -> Result<bool> {
        match update {
            Err(broadcast::error::RecvError::Lagged(_)) => {
                error!("we are lagged somehow");
                todo!();
            }
            Err(broadcast::error::RecvError::Closed) => {
                info!("Write half exited. Read half exiting.");
                Ok(true)
            }
            Ok(ThreadUpdate::Downloaded(piece, block)) => {
                debug!("write thread recieved message");
                if requested
                    .mark_block_as_downloaded(piece as usize, block as usize)
                    .is_none()
                {
                    warn!("unable to process piece={piece} block={block}");
                }
                Ok(false)
            }
            Ok(ThreadUpdate::Completed(piece)) => {
                requested.mark_piece_as_downloaded(piece as usize);
                let message = Message::have();
                debug!("sending message: {message:?}");
                stream
                    .write_all(&message.as_bytes())
                    .await
                    .context("writing Interested message")?;
                stream
                    .write_u32(piece)
                    .await
                    .context("writing Have payload")?;
                stream.flush().await.context("Flushing interested")?;
                if !peer_state.should_be_interested().await {
                    *peer_state.am_interested.0.lock().await = false;
                    peer_state.am_interested.1.notify_one();
                }
                Ok(false)
            }
            Ok(ThreadUpdate::FileComplete) => {
                cancel_requests(stream, peer_state, requested.pending_requests())
                    .await
                    .context("Writing cancel messages")?;
                Ok(false)
            }
            Ok(ThreadUpdate::Disconnect) => Ok(true),
        }
    }

    pub async fn send_one_round(
        stream: &mut tokio::io::WriteHalf<TcpStream>,
        peer_state: &PeerState,
        requested: &mut PieceTracker,
    ) -> Result<()> {
        /* If the peer has something that we ?want, have not sent
         * the peer an Interested message, send an interested message. */
        // TODO: change this to only request *NEW* pieces

        let peer_has = peer_state.peer_bitfield.read().await.iter_ones().collect();
        let request = requested.request(peer_has);
        if *peer_state.am_interested.0.lock().await
            && let Some((piece, block_num)) = request
        {
            let message = Message::request();
            // debug!("sending message: {message:?}");
            stream
                .write_all(&message.as_bytes())
                .await
                .context("writing Request header")?;

            let block_size = calculate_block_size(
                piece,
                block_num,
                peer_state.metadata.info.piece_length,
                peer_state.metadata.info.torr_type.len(),
            );

            let block_begin = block_num * BLOCK_SIZE;
            debug!("requesting piece={piece} block_begin={block_begin}, block_size={block_size}");
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
            stream.flush().await.context("Flushing interested")?;
        }

        Ok(())
    }

    async fn cancel_requests(
        stream: &mut tokio::io::WriteHalf<TcpStream>,
        peer_state: &PeerState,
        requests: Vec<(u32, u32)>,
    ) -> Result<()> {
        let message_header = Message::cancel();
        debug!("sending message: {message_header:?}");
        for (piece, block) in requests {
            let block_size = super::calculate_block_size(
                piece,
                block,
                peer_state.metadata.info.piece_length,
                peer_state.metadata.info.torr_type.len(),
            );
            stream
                .write_all(&message_header.as_bytes())
                .await
                .context("writing Cancel header")?;
            stream
                .write_u32(piece)
                .await
                .context("writing Cancel piece")?;
            stream
                .write_u32(block * BLOCK_SIZE)
                .await
                .context("writing Cancel begin")?;
            stream
                .write_u32(block_size)
                .await
                .context("writing Cancel data len")?;
        }
        Ok(())
    }

    #[allow(dead_code)]
    /// Send our bitfield to a peer
    /// For some reason, sending this as our first message seems to break a lot of peer
    /// connections. Not sure what the reason is though
    pub async fn send_bitfield(
        stream: &mut tokio::io::WriteHalf<TcpStream>,
        peer_state: &PeerState,
    ) -> Result<()> {
        let bitfield = peer_state.my_bitfield.read().await;
        let payload: &[u8] = bitfield.as_raw_slice();
        let header = Message {
            length: 1 + payload.len() as u32,
            message_id: Some(MessageId::BitField),
        };
        debug!("header={header:?}");
        stream
            .write_all(&header.as_bytes())
            .await
            .context("writing BitField header")?;
        stream
            .write_all(payload)
            .await
            .context("writing BitField payload")?;
        Ok(())
    }
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
