mod connection;
pub mod handshake;
mod message;
mod pieces;

const BLOCK_SIZE: u32 = 1 << 14;

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BlockState {
    UnRequested,
    Requested,
    Completed,
}

use std::sync::{Arc, RwLock};

use bitvec::prelude::*;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::select;
use tokio::sync::broadcast;
use tokio::task::JoinSet;
use tracing::{Instrument, debug, error, warn};

use crate::{ThreadUpdate, parsing::Metadata};

enum ChildUpdates {
    Read(tokio::io::ReadHalf<TcpStream>),
    Write(tokio::io::WriteHalf<TcpStream>),
    ParentClosed,
}

pub async fn handle_peer(
    parent_tx: broadcast::Sender<ThreadUpdate>,
    mut parent_rx: broadcast::Receiver<ThreadUpdate>,
    stream: TcpStream,
    metadata: Metadata,
    my_bitfield: Arc<RwLock<BitVec<u8, Msb0>>>,
) -> Result<(), std::io::Error> {
    debug!("handling peer connection");

    let num_pieces: u64 = metadata.num_pieces() as u64;

    let peer_bitfield = Arc::new(RwLock::new(BitVec::<u8, Msb0>::repeat(
        false,
        num_pieces as usize,
    )));
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
        match connection::read_peer(
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
        match connection::write_peer(
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
