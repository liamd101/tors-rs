use crate::parsing::Metadata;

use std::collections::VecDeque;
use std::sync::{Arc, atomic::AtomicBool};

use bitvec::prelude::*;
use tokio::sync::{Mutex, Notify, RwLock};

#[derive(Clone)]
pub(super) struct PeerState {
    /// Metadata for the Torrent file we are downloading from
    pub metadata: Metadata,
    /// Threadsafe pointer to the bitfield of completed pieces
    pub my_bitfield: Arc<RwLock<BitVec<u8, Msb0>>>,
    /// Threadsafe pointer to a bitfield containing this peer's completed pieces
    pub peer_bitfield: Arc<RwLock<BitVec<u8, Msb0>>>,
    /// A threadsafe boolean containing whether or not this peer is interested in some data that we
    /// have
    pub interested: Arc<AtomicBool>,
    /// A thread safe VecDeque containing information of Block data that the peer has requested
    /// from us. Each entry is of form (piece_index, block_offset, block_len)
    pub request_queue: Arc<Mutex<VecDeque<(u32, u32, u32)>>>,

    pub interest_changed: Arc<Notify>,
}

impl PeerState {
    pub fn new(metadata: Metadata, my_bitfield: Arc<RwLock<BitVec<u8, Msb0>>>) -> Self {
        let num_pieces = metadata.num_pieces();
        Self {
            metadata,
            my_bitfield,
            peer_bitfield: Arc::new(RwLock::new(BitVec::repeat(false, num_pieces))),
            interested: Arc::new(AtomicBool::new(false)),
            request_queue: Arc::new(Mutex::new(VecDeque::new())),
            interest_changed: Arc::new(Notify::new()),
        }
    }

    /// Returns a boolean indicating whether the peer has something that we want
    /// True if the peer has a piece we don't have
    /// False if the peer has only pieces we have
    pub async fn should_be_interested(&self) -> bool {
        (!self.my_bitfield.read().await.clone() & self.peer_bitfield.read().await.clone()).any()
    }
}
