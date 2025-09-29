use crate::{ThreadUpdate, parsing::TorrentType};

use super::{
    BLOCK_SIZE, ChildUpdates,
    message::{Message, MessageId},
    pieces::PieceTracker,
    state::PeerState,
};

use std::io::SeekFrom;
use std::sync::atomic::Ordering;

use anyhow::{Context, Result};
use bitvec::prelude::*;
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    net::TcpStream,
    select,
    sync::broadcast,
};
use tracing::{debug, error, info, warn};

pub(super) async fn read_peer(
    tx: broadcast::Sender<ThreadUpdate>,
    mut rx: broadcast::Receiver<ThreadUpdate>,
    mut stream: tokio::io::ReadHalf<TcpStream>,
    mut peer_state: PeerState,
) -> Result<ChildUpdates> {
    loop {
        select! {
            channel_result = rx.recv() => {
                if handle_channel_message(channel_result)? {
                    break
                }
            }

            peer_result = read_message(&mut stream) => {
                if let Some(message) = peer_result? {
                    process_message(
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

fn handle_channel_message(
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

async fn read_message(
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
        info!("Keep alive reeived. continuing");
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

async fn process_message(
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
            peer_state
                .choked
                .store(matches!(message_id, MessageId::Choke), Ordering::Relaxed);
        }

        MessageId::Interested | MessageId::NotInterested => {
            peer_state.interested.store(
                matches!(message_id, MessageId::Interested),
                Ordering::Relaxed,
            );
        }

        MessageId::Have => {
            let index: u32 = stream.read_u32().await?;
            peer_state
                .peer_bitfield
                .write()
                .await
                .set(index as usize, true)
        }

        MessageId::BitField => {
            let mut payload = vec![0u8; message.length as usize - 1];
            stream.read_exact(&mut payload).await?;
            let received_bitfield =
                &BitVec::<u8, Msb0>::from_slice(&payload)[..peer_state.metadata.num_pieces()];
            let mut peer_bitfield = peer_state.peer_bitfield.write().await;
            for set_bit in received_bitfield.iter_ones() {
                peer_bitfield.set(set_bit, true);
            }
            debug!("bitfield={peer_bitfield}");
        }

        MessageId::Request => {
            let piece_index: u32 = stream.read_u32().await.context("reading piece index")?;
            let begin: u32 = stream.read_u32().await.context("reading piece index")?;
            let data_len: u32 = stream.read_u32().await.context("reading piece index")?;
            peer_state
                .request_queue
                .lock()
                .await
                .push_back((piece_index, begin, data_len));
        }

        MessageId::Cancel => {
            let piece_index: u32 = stream.read_u32().await.context("reading piece index")?;
            let begin: u32 = stream.read_u32().await.context("reading piece index")?;
            stream.read_u32().await.context("reading piece index")?;
            peer_state
                .request_queue
                .lock()
                .await
                .retain(|&(req_piece_index, req_begin, _)| {
                    !(req_piece_index == piece_index && req_begin == begin)
                });
        }

        MessageId::Piece => {
            let output_dir = match peer_state.metadata.info.torr_type {
                TorrentType::MultiFile { .. } => peer_state.metadata.info.name.clone(),
                TorrentType::SingleFile { .. } => "out".to_string(),
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
                error!("read {bytes_read} bytes instead of {data_len} bytes. closing connection.");
                return Ok(()); // TODO: return error here
            }

            let mut piece_position = 0;
            for (file, file_offset, bytes_to_write) in peer_state.metadata.from_piece_block(
                piece_index as u64,
                begin as u64,
                data_len as u64,
            )? {
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
            debug!("Downloaded part of piece={piece_index}");
            tx.send(ThreadUpdate::Downloaded(piece_index, begin / BLOCK_SIZE))?;
        }

        _ => todo!(),
    }

    Ok(())
}

pub(super) async fn write_peer(
    _tx: broadcast::Sender<ThreadUpdate>,
    mut rx: broadcast::Receiver<ThreadUpdate>,
    mut stream: tokio::io::WriteHalf<TcpStream>,
    peer_state: PeerState,
) -> Result<ChildUpdates> {
    let mut am_interested = false;
    let mut requested = PieceTracker::from_file_info(
        peer_state.metadata.info.torr_type.len(),
        peer_state.metadata.info.piece_length,
    );
    requested.update(&*peer_state.my_bitfield.read().await);

    loop {
        match rx.try_recv() {
            Err(broadcast::error::TryRecvError::Lagged(_)) => {
                error!("we are lagged somehow");
                todo!();
            }
            Err(broadcast::error::TryRecvError::Closed) => {
                info!("Write half exited. Read half exiting.");
                break;
            }
            Err(broadcast::error::TryRecvError::Empty) => {}
            Ok(ThreadUpdate::Downloaded(piece, block)) => {
                if requested
                    .mark_block_as_downloaded(piece as usize, block as usize)
                    .is_none()
                {
                    warn!("unable to process piece={piece} block={block}");
                }
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
            }
            Ok(ThreadUpdate::FileComplete) => {
                cancel_requests(&mut stream, &peer_state, requested.pending_requests())
                    .await
                    .context("Writing cancel messages")?;
            }
            Ok(ThreadUpdate::Disconnect) => {
                break;
            }
        }

        let choked = peer_state.choked.load(Ordering::Relaxed);
        /* If the peer has something that we ?want, have not sent
         * the peer an Interested message, send an interested message. */
        let peer_has_new_piece = (!peer_state.my_bitfield.read().await.clone()
            & peer_state.peer_bitfield.read().await.clone())
        .any();

        if peer_has_new_piece && !am_interested {
            let message = Message::interested();
            debug!("sending message: {message:?}");

            stream
                .write_all(&message.as_bytes())
                .await
                .context("writing Interested message")?;

            am_interested = true;
        }

        if !peer_has_new_piece && am_interested {
            let message = Message::not_interested();
            debug!("sending message: {message:?}");

            stream
                .write_all(&message.as_bytes())
                .await
                .context("writing NotInterested message")?;

            am_interested = false;
        }

        let peer_has = peer_state.peer_bitfield.read().await.iter_ones().collect();
        if !choked && let Some((piece, block_num)) = requested.request(peer_has) {
            let message = Message::request();
            debug!("sending message: {message:?}");
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
            debug!("requesting piece={piece} block_begin={block_begin}");
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

        // if peer is interested in what we have, we want to send them some piece data
        let mut req_queue = peer_state.request_queue.lock().await;
        if peer_state.interested.load(Ordering::Relaxed) && !req_queue.is_empty() {
            let (piece_index, begin, data_len) = req_queue
                .pop_front()
                .context("Reading from non-empty queue failed.")
                .unwrap();

            if !peer_state
                .my_bitfield
                .read()
                .await
                .get(piece_index as usize)
                .as_deref()
                .unwrap_or(&false)
            {
                warn!("Don't have requested piece");
                continue;
            }

            let piece_data = peer_state
                .metadata
                .get_piece_data(piece_index as u64, begin as u64, data_len as u64)
                .await
                .context("Reading piece data from files")?;

            stream
                .write_all(&piece_data)
                .await
                .context("Writing piece data to stream")?;
        }
    }
    Ok(ChildUpdates::Write(stream))
}

async fn cancel_requests(
    stream: &mut tokio::io::WriteHalf<TcpStream>,
    peer_state: &PeerState,
    requests: Vec<(u32, u32)>,
) -> Result<()> {
    let message_header = Message::cancel();
    debug!("sending message: {message_header:?}");
    for (piece, block) in requests {
        let block_size = calculate_block_size(
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
