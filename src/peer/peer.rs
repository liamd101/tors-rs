use std::io::SeekFrom;
use std::sync::{Arc, RwLock};

use anyhow::{Context, Error};

use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::select;
use tokio::sync::broadcast;
use tokio::task::JoinSet;

use tracing::{Instrument, debug, error, info, warn};

use crate::{
    ThreadUpdate,
    message::{BitField, Message, MessageId},
    parsing::Metadata,
};

use super::{BLOCK_SIZE, pieces::PieceTracker};

pub async fn handle_peer(
    parent_tx: broadcast::Sender<ThreadUpdate>,
    mut parent_rx: broadcast::Receiver<ThreadUpdate>,
    stream: TcpStream,
    metadata: Metadata,
    my_bitfield: Arc<RwLock<BitField>>,
) -> Result<(), std::io::Error> {
    debug!("handling peer connection");

    let num_pieces: u64 = metadata.num_pieces() as u64;

    // TODO: this will definitely need to be redone with mrsw design
    let peer_bitfield = Arc::new(RwLock::new(BitField::with_settable(num_pieces)));
    let choked = Arc::new(RwLock::new(true));

    let (read_stream, write_stream) = tokio::io::split(stream);

    let mut set = JoinSet::new();
    let (child_tx, mut child_rx) = broadcast::channel::<ThreadUpdate>(32);

    let current_span = tracing::Span::current();

    let read_tx = child_tx.clone();
    let read_rx = child_tx.subscribe();
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
    let write_tx = child_tx.clone();
    let write_rx = child_tx.subscribe();
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

    set.spawn(
        async move {
            loop {
                select! {
                result = child_rx.recv() => {
                    match result {
                        Ok(ThreadUpdate::Downloaded(piece, block)) => {
                            parent_tx.send(ThreadUpdate::Downloaded(piece, block))?;
                        }
                        Ok(ThreadUpdate::FileComplete) => break,
                        Ok(_) => {},
                        Err(broadcast::error::RecvError::Closed) => break,
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            warn!("child_rx Lagged by {n}. ignoring for now.");
                        },
                    }
                }

                result = parent_rx.recv() => {
                    match result {
                        Ok(ThreadUpdate::Downloaded(_, _)) => {}
                        Ok(update) => {
                            child_tx.send(update)?;
                        },
                        Err(broadcast::error::RecvError::Closed) => break,
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            warn!("parent_rx: Lagged by {n}. ignoring for now.");
                        },
                    }
                }
                };
            }

            Ok(ChildUpdates::ParentClosed)
        }
        .instrument(tracing::Span::current()),
    );

    let mut read_stream: Option<tokio::io::ReadHalf<TcpStream>> = None;
    let mut write_stream: Option<tokio::io::WriteHalf<TcpStream>> = None;

    while let Some(result) = set.join_next().await {
        if let Ok(Ok(stream)) = result {
            match stream {
                ChildUpdates::Write(stream) => write_stream = Some(stream),
                ChildUpdates::Read(stream) => read_stream = Some(stream),
                ChildUpdates::ParentClosed => {}
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

enum ChildUpdates {
    Read(tokio::io::ReadHalf<TcpStream>),
    Write(tokio::io::WriteHalf<TcpStream>),
    ParentClosed,
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
        Ok(ThreadUpdate::FileComplete) => Ok(true),
        _ => Ok(false),
    }
}

async fn read_peer(
    tx: broadcast::Sender<ThreadUpdate>,
    mut rx: broadcast::Receiver<ThreadUpdate>,
    mut stream: tokio::io::ReadHalf<TcpStream>,
    metadata: Metadata,
    peer_bitfield: Arc<RwLock<BitField>>,
    choked: Arc<RwLock<bool>>,
) -> anyhow::Result<ChildUpdates, anyhow::Error> {
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
                        metadata.clone(),
                        &peer_bitfield,
                        &choked,
                    )
                    .await?;
                }
            }
        }
    }

    Ok(ChildUpdates::Read(stream))
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
    let message_id = MessageId::try_from(message_id)?;

    Ok(Some(Message {
        length,
        message_id: Some(message_id),
    }))
}

async fn process_message(
    message: Message,
    tx: &broadcast::Sender<ThreadUpdate>,
    stream: &mut tokio::io::ReadHalf<TcpStream>,
    metadata: Metadata,
    peer_bitfield: &Arc<RwLock<BitField>>,
    choked: &Arc<RwLock<bool>>,
) -> anyhow::Result<()> {
    let Some(message_id) = message.message_id else {
        return Ok(());
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

            debug!("wrote {bytes_read} bytes to {}", piece_start + begin as u64);

            tx.send(ThreadUpdate::Downloaded(piece_index, begin / BLOCK_SIZE))?;
        }

        MessageId::BitField => {
            let mut payload = vec![0u8; message.length as usize - 1];
            stream.read_exact(&mut payload).await?;
            let sent_bitfield = BitField::new(payload, metadata.num_pieces() as u64)
                .expect("peer has an impossible bitfield");
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

    Ok(())
}

async fn write_peer(
    _tx: broadcast::Sender<ThreadUpdate>,
    mut rx: broadcast::Receiver<ThreadUpdate>,
    mut stream: tokio::io::WriteHalf<TcpStream>,
    metadata: Metadata,
    my_bitfield: Arc<RwLock<BitField>>,
    peer_bitfield: Arc<RwLock<BitField>>,
    choked: Arc<RwLock<bool>>,
) -> Result<ChildUpdates, Error> {
    let mut am_interested = false;
    let mut requested =
        PieceTracker::from_file_info(metadata.info.torr_type.len(), metadata.info.piece_length);
    requested.update(&my_bitfield.read().unwrap());

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
                match requested.mark_block_as_downloaded(piece as usize, block as usize) {
                    Some(_) => debug!("updated requested pieces"),
                    None => warn!("unable to process piece={piece} block={block}"),
                }
            }
            Ok(ThreadUpdate::Completed(piece)) => {
                requested.mark_piece_as_downloaded(piece as usize);
                let message = Message::have();
                info!("sending message: {message:?}");

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
                let message_header = Message::cancel();
                for (piece, block) in requested.pending_requests() {
                    let block_size = calculate_block_size(
                        piece,
                        block,
                        metadata.info.piece_length,
                        metadata.info.torr_type.len(),
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
                        .context("writing Cancel piece")?;
                    stream
                        .write_u32(block_size)
                        .await
                        .context("writing Cancel piece")?;
                }
                // TODO: implement seeding to peers so we don't just disconnect like an ass
                break;
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
            let message = Message::interested();
            debug!("sending message: {message:?}");

            stream
                .write_all(&message.as_bytes())
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
            let message = Message::not_interested();
            debug!("sending message: {message:?}");

            stream
                .write_all(&message.as_bytes())
                .await
                .context("writing NotInterested message")?;

            am_interested = false;
        }

        let peer_has = peer_bitfield.read().unwrap().set_bits();
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
                metadata.info.piece_length,
                metadata.info.torr_type.len(),
            );

            let block_begin = block_num * BLOCK_SIZE;
            info!("requesting piece={piece} block_begin={block_begin}");
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
    Ok(ChildUpdates::Write(stream))
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
