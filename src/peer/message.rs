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
            _ => Err(anyhow::anyhow!("Invalid MessageId {val}")),
        }
    }
}
