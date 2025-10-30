use super::handshake::Reserved;
use crate::parsing::Metadata;

use std::sync::{Arc, atomic::AtomicBool};

use anyhow::{Context, Result};
use bitvec::prelude::*;
use tokio::sync::{Mutex, Notify, RwLock, mpsc};

pub(super) struct PeerState {
    pub reserved: Reserved,
    /// Metadata for the Torrent file we are downloading from
    pub metadata: Metadata,
    /// Threadsafe pointer to the bitfield of completed pieces
    pub my_bitfield: Arc<RwLock<BitVec<u8, Msb0>>>,
    /// Threadsafe pointer to a bitfield containing this peer's completed pieces
    pub peer_bitfield: Arc<RwLock<BitVec<u8, Msb0>>>,

    pub request_queue: ChannelHalf,

    /// Effectively a Condvar
    /// This is to prevent spinning, so we only check to write messages when the peer unchokes us
    pub peer_choking: Arc<(Mutex<bool>, Notify)>,
    pub am_interested: Arc<(Mutex<bool>, Notify)>,

    /// Whether we are choking the peer
    pub am_choking: Arc<AtomicBool>,
    /// Whether the peer is interested in something we have
    pub peer_interested: Arc<AtomicBool>,
}
pub(super) enum ChannelHalf {
    Sender(mpsc::Sender<(u32, u32, u32)>),
    Receiver(mpsc::Receiver<(u32, u32, u32)>),
}
impl ChannelHalf {
    pub async fn recv(&mut self) -> Option<(u32, u32, u32)> {
        match self {
            Self::Sender(_) => None,
            Self::Receiver(receiver) => receiver.recv().await,
        }
    }

    pub async fn send(&self, value: (u32, u32, u32)) -> Result<()> {
        match self {
            Self::Sender(sender) => sender.send(value).await.context("Sending value"),
            Self::Receiver(_) => Err(anyhow::anyhow!("Invalid call on Receiver")),
        }
    }
}

impl PeerState {
    pub fn channel(
        reserved: Reserved,
        metadata: Metadata,
        my_bitfield: Arc<RwLock<BitVec<u8, Msb0>>>,
    ) -> (Self, Self) {
        let num_pieces = metadata.num_pieces();

        let (sender, receiver) = mpsc::channel(16);
        let peer_bitfield = Arc::new(RwLock::new(BitVec::repeat(false, num_pieces)));

        let peer_interested = Arc::new(AtomicBool::new(false));
        let am_choking = Arc::new(AtomicBool::new(true));

        let am_interested_tmp = Arc::new((Mutex::new(false), Notify::new()));
        let peer_choking_tmp = Arc::new((Mutex::new(false), Notify::new()));

        let sender = Self {
            reserved,
            metadata: metadata.clone(),
            my_bitfield: my_bitfield.clone(),
            peer_bitfield: peer_bitfield.clone(),
            request_queue: ChannelHalf::Sender(sender),
            peer_interested: peer_interested.clone(),
            am_choking: am_choking.clone(),
            am_interested: am_interested_tmp.clone(),
            peer_choking: peer_choking_tmp.clone(),
        };

        let receiver = Self {
            reserved,
            metadata: metadata.clone(),
            my_bitfield: my_bitfield.clone(),
            peer_bitfield: peer_bitfield.clone(),
            request_queue: ChannelHalf::Receiver(receiver),
            peer_interested: peer_interested.clone(),
            am_choking: am_choking.clone(),
            am_interested: am_interested_tmp.clone(),
            peer_choking: peer_choking_tmp.clone(),
        };
        (sender, receiver)
    }

    /// Returns a boolean indicating whether the peer has something that we want
    /// True if the peer has a piece we don't have
    /// False if the peer has only pieces we have
    pub async fn should_be_interested(&self) -> bool {
        (!self.my_bitfield.read().await.clone() & self.peer_bitfield.read().await.clone()).any()
    }
}
