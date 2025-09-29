use crate::parsing::Metadata;

use std::sync::{Arc, atomic::AtomicBool};
use std::collections::VecDeque;

use bitvec::prelude::*;
use tokio::sync::{RwLock, Mutex};

#[derive(Clone)]
pub(super) struct PeerState {
    pub metadata: Metadata,
    pub my_bitfield: Arc<RwLock<BitVec<u8, Msb0>>>,
    pub peer_bitfield: Arc<RwLock<BitVec<u8, Msb0>>>,
    pub interested: Arc<AtomicBool>,
    pub request_queue: Arc<Mutex<VecDeque<(u32, u32, u32)>>>,
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
        }
    }
}
