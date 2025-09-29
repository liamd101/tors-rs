mod connection;
pub mod handshake;
mod message;
mod pieces;
mod state;

const BLOCK_SIZE: u32 = 1 << 14;

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BlockState {
    UnRequested,
    Requested,
    Completed,
}

use std::sync::Arc;

use bitvec::prelude::*;
use tokio::{
    io::AsyncWriteExt,
    net::TcpStream,
    select,
    sync::{RwLock, broadcast, watch},
    task::JoinSet,
};
use tracing::{Instrument, debug, warn};

use crate::{ThreadUpdate, parsing::Metadata};
use state::PeerState;

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

    let (read_stream, write_stream) = tokio::io::split(stream);
    let mut set = JoinSet::new();
    let (child_tx, mut child_rx) = broadcast::channel::<ThreadUpdate>(32);

    let peer_state = PeerState::new(metadata, my_bitfield);
    let (choked_tx, choked_rx) = watch::channel(true);

    let current_span = tracing::Span::current();
    let read_tx = child_tx.clone();
    let read_rx = child_tx.subscribe();
    let read_state = peer_state.clone();
    set.spawn(async move {
        connection::read_peer(read_tx, read_rx, read_stream, read_state, choked_tx)
            .instrument(current_span)
            .await
    });

    let current_span = tracing::Span::current();
    let write_tx = child_tx.clone();
    let write_rx = child_tx.subscribe();
    let write_state = peer_state.clone();
    set.spawn(async move {
        connection::write_peer(write_tx, write_rx, write_stream, write_state, choked_rx)
            .instrument(current_span)
            .await
    });

    // Filter some messages to/from parent threads to children
    set.spawn(
        async move {
            loop {
                select! {
                result = child_rx.recv() => {
                    match result {
                        Ok(ThreadUpdate::Downloaded(piece, block)) => {
                            parent_tx.send(ThreadUpdate::Downloaded(piece, block))?;
                        }
                        Ok(ThreadUpdate::Disconnect) => break,
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
