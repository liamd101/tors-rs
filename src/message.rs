use tokio_util::bytes::{Buf, BytesMut};
use tokio_util::codec::Decoder;

use tracing::error;

use anyhow::Result;

#[derive(Clone, Eq, PartialEq)]
pub struct BitField {
    /// The actual value of the BitField. Stored as `Vec<u8>` since these BitFields can be anylength
    bits: Vec<u8>,
    /// The number of valid/settable bits
    pub settable: u64,
}
impl BitField {
    pub fn new(bytes: Vec<u8>, settable: u64) -> Result<Self, &'static str> {
        if settable as usize > 8 * bytes.len() {
            return Err("not enough bytes supplied in bitfield");
        } else if (settable as usize) < bytes.len() / 8 {
            return Err("not enough settable bytes");
        }
        let mut ones = 0;
        let mut furthest_left = 0;
        for (i, &byte) in bytes.iter().enumerate() {
            for bit in 0..8 {
                if (1 << bit) as u8 & byte != 0 {
                    furthest_left = (8 * i) + (7 - bit);
                    ones += 1;
                }
            }
        }

        if furthest_left >= settable as usize || ones > settable {
            error!("furthest_left={furthest_left}");
            error!("settable={settable}");
            return Err("too many ones set in bitfield");
        }

        Ok(Self {
            bits: bytes,
            settable,
        })
    }

    pub fn update(&mut self, other: &Self) -> Result<(), std::io::Error> {
        if self.settable != other.settable {
            return Err(std::io::Error::other("Number of settable bits differ"));
        }

        for (my_bit, other_bit) in self.bits.iter_mut().zip(other.bits.iter()) {
            *my_bit |= other_bit;
        }

        Ok(())
    }

    pub fn from_slice(bits: &[u8]) -> Self {
        Self {
            bits: bits.to_vec(),
            settable: bits.len() as u64 * 8,
        }
    }

    pub fn with_settable(settable: u64) -> Self {
        Self {
            settable,
            bits: vec![0u8; settable.div_ceil(8) as usize], /* computes the number of bytes necessary for the required number of bits */
        }
    }

    pub fn is_set(&self, index: usize) -> Result<bool, &'static str> {
        if index > self.settable as usize {
            return Err("Index out of bounds");
        }
        let byte_index = index / 8;
        let bit_index = index % 8;
        if byte_index >= self.bits.len() {
            return Err("Index out of bounds");
        }
        let mask = 1 << (7 - bit_index);
        Ok(self.bits[byte_index] & mask != 0)
    }

    pub fn set(&mut self, index: usize, value: bool) -> Result<bool, &'static str> {
        if index > self.settable as usize {
            return Err("Index out of bounds");
        }
        let byte_index = index / 8;
        let bit_index = index % 8;
        if byte_index >= self.bits.len() {
            return Err("Index out of bounds");
        }
        let mask = 1 << (7 - bit_index);
        let prev = (self.bits[byte_index] & mask) == mask;
        if value {
            self.bits[byte_index] |= mask;
        } else {
            self.bits[byte_index] &= !mask;
        }
        Ok(prev)
    }

    /// Determines if another set has any bits set that `self` does not have set
    /// This returns an error if the bitfields are of different sizes
    pub fn has_other(&self, other: &BitField) -> Result<bool, &'static str> {
        if self.settable != other.settable {
            return Err("BitFields do not have the same number of settable bits");
        }
        for (&self_byte, &other_byte) in self.bits.iter().zip(other.bits.iter()) {
            if (!self_byte) & other_byte != 0 {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Returns a vector containing all indices that are set in the piece
    pub fn set_bits(&self) -> Vec<usize> {
        let mut indices = vec![];
        for (i, &byte) in self.bits.iter().enumerate() {
            for bit in (0..8).rev() {
                if (1 << bit) & byte != 0 {
                    indices.push((8 * i) + (7 - bit));
                }
            }
        }
        indices
    }

    /// Determines if `self` is set for at least every bit that `other` is set for
    /// This fails if the bitfields are of different sizes
    pub fn contains(&self, other: &BitField) -> Result<bool, &'static str> {
        if self.settable != other.settable {
            return Err("BitFields do not have the same number of bytes");
        }
        for (&self_byte, &other_byte) in self.bits.iter().zip(other.bits.iter()) {
            if self_byte & other_byte != other_byte {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

impl std::fmt::Debug for BitField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BitField {{ bits: ")?;
        for (i, byte) in self.bits.iter().enumerate() {
            if i > 0 {
                write!(f, "_")?;
            }
            write!(f, "{:08b}", byte)?;
        }
        write!(f, " }}")
    }
}

impl std::ops::Not for BitField {
    type Output = Self;

    fn not(self) -> Self::Output {
        let mut bits = self.bits.clone();
        for (i, &byte) in self.bits.iter().enumerate() {
            if i == self.bits.len() - 1 {
                let mut mask = 0u8;
                for b in 0..self.settable % 8 {
                    mask |= 0b10000000 >> b;
                }
                bits.push(mask);
            } else {
                bits.push(!byte);
            }
        }
        BitField::new(bits, self.settable).expect("this should never fail")
    }
}

/// A struct representing message headers between peers
/// All communication between peers in the BitTorrent protocol is communicated in Messages of this
/// format
#[repr(C)]
#[derive(Debug, Default)]
pub struct Message {
    /// The length of the entire message being transmitted
    pub length: u32,
    /// Optional parameter indicating the type of message being communciated
    /// This value is only None in a keep-alive message (i.e. length == 0)
    pub message_id: Option<MessageId>,
}

impl Message {
    pub fn as_bytes(&self) -> Vec<u8> {
        let mut out = u32::to_be_bytes(self.length).to_vec();
        if let Some(message_id) = self.message_id {
            out.push(message_id as u8)
        };
        out
    }

    pub fn cancel() -> Self {
        Self {
            length: 13,
            message_id: Some(MessageId::Cancel),
        }
    }

    pub fn have() -> Self {
        Self {
            length: 5,
            message_id: Some(MessageId::Have),
        }
    }

    pub fn not_interested() -> Self {
        Self {
            length: 1,
            message_id: Some(MessageId::NotInterested),
        }
    }

    pub fn interested() -> Self {
        Self {
            length: 1,
            message_id: Some(MessageId::Interested),
        }
    }

    pub fn request() -> Self {
        Self {
            length: 13,
            message_id: Some(MessageId::Request),
        }
    }
}

/// Enum representing the type of messages supported by the BitTorrent protocol
#[repr(u8)]
#[derive(Debug, Copy, Clone)]
pub enum MessageId {
    /// Indicates that the peer is choking the client
    Choke = 0,
    /// Indicates that the peer is unchoking the client
    UnChoke = 1,
    /// Indicates that a peer is interested in something that the client has (and vice versa)
    Interested = 2,
    /// Indicates that a peer is not interested in aynthing that the client has to offer (and vice
    /// versa)
    NotInterested = 3,
    /// Indicates that a peer/client has the piece indicated in the message payload
    ///
    /// Payload is u32 indicating the index of the piece
    Have = 4,
    /// Indicates a message containing a BitField representing the pieces that have been
    /// successfully downloaded.
    ///
    /// The high bit in the first byte of the payload corresponds to piece index 0. Bits that
    /// are cleared indicate a missing piece, and set bits indicate a valid and available piece.
    /// Spare bits at the end are set to zero.
    ///
    /// BitField messages can only be sent immediately after the peer handshake is completed,
    /// before any other messages are sent. It is optional, and need not be sent if a client has no
    /// pieces
    BitField = 5,
    /// Indicates a fixed-length message used to request a block from a peer/client.
    ///
    /// The payload contains the following information in order:
    ///   index  : u32 integer specifying the zero-based piece index
    ///   begin  : u32 integer specifying the zero-based byte offset within the piece
    ///   length : u32 integer specifying the requested length
    ///
    /// For more information about Request messages, see here:
    /// https://wiki.theory.org/BitTorrentSpecification#request:_.3Clen.3D0013.3E.3Cid.3D6.3E.3Cindex.3E.3Cbegin.3E.3Clength.3E
    Request = 6,
    /// Indicates a message containing piece data
    ///
    /// The payload contains the following information in order:
    ///   index : u32 integer specifying the zero-based piece index
    ///   begin : u32 integer specifying the zero-based byte offset within the piece
    ///   block : block of data, which is a subset of the piece specified by index
    Piece = 7,
    /// Indicates a fixed-length message to cancel block requests
    ///
    /// The payload contains the following information in order:
    ///   index  : u32 integer specifying the zero-based piece index
    ///   begin  : u32 integer specifying the zero-based byte offset within the piece
    ///   length : u32 integer specifying the requested length
    ///
    /// For more information about Request messages, see here:
    /// https://wiki.theory.org/BitTorrentSpecification#request:_.3Clen.3D0013.3E.3Cid.3D6.3E.3Cindex.3E.3Cbegin.3E.3Clength.3E
    ///
    /// It is typically used during "End Game". TODO
    Cancel = 8,
    /// Indicates the port that this peer's DHT node is listening on.
    /// Typically sent by newer versions of the Mainline that implements a DHT tracker.
    ///
    /// This peer should be inserted in the local routing table if DHT tracker is supported.
    Port = 9,
}

impl TryFrom<u8> for MessageId {
    type Error = anyhow::Error;
    fn try_from(val: u8) -> Result<Self, Self::Error> {
        match val {
            0 => Ok(MessageId::Choke),
            1 => Ok(MessageId::UnChoke),
            2 => Ok(MessageId::Interested),
            3 => Ok(MessageId::NotInterested),
            4 => Ok(MessageId::Have),
            5 => Ok(MessageId::BitField),
            6 => Ok(MessageId::Request),
            7 => Ok(MessageId::Piece),
            8 => Ok(MessageId::Cancel),
            9 => Ok(MessageId::Port),
            _ => Err(anyhow::anyhow!("Invalid MessageId")),
        }
    }
}

pub struct MessageCodec {}

const MAX_MESSAGE_LEN: usize = 1 << 16;

impl Decoder for MessageCodec {
    type Item = Message;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            // Not enough data to read length marker
            return Ok(None);
        }

        let mut length_bytes = [0u8; 4];
        length_bytes.copy_from_slice(&src[..4]);
        let length = u32::from_be_bytes(length_bytes);
        if length == 0 {
            return Ok(Some(Message {
                length: 0,
                message_id: None,
            }));
        }
        if src.len() < 4 + 1 {
            src.reserve(4 + 1);
            return Ok(None);
        }

        if length as usize > MAX_MESSAGE_LEN {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Frame of length {} is too large.", length),
            ));
        }

        let Ok(message_id) = MessageId::try_from(src[4]) else {
            return Err(Self::Error::from(std::io::ErrorKind::Other));
        };

        src.advance(4 + 1);

        Ok(Some(Message {
            length,
            message_id: Some(message_id),
        }))
    }
}
