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

    pub fn unchoke() -> Self {
        Self {
            length: 1,
            message_id: Some(MessageId::UnChoke),
        }
    }

    #[allow(dead_code)]
    pub fn choke() -> Self {
        Self {
            length: 1,
            message_id: Some(MessageId::Choke),
        }
    }

    pub fn have_none() -> Self {
        Self {
            length: 1,
            message_id: Some(MessageId::HaveNone),
        }
    }

    pub fn have_all() -> Self {
        Self {
            length: 1,
            message_id: Some(MessageId::HaveAll),
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
    /// Indicates that a peer is not interested in what the client has to offer (and vice versa)
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
    ///   index : u32 specifying the zero-based piece index
    ///   begin : u32 specifying the zero-based byte offset within the piece
    ///   block : block of data, which is a subset of the piece specified by index
    Piece = 7,
    /// Indicates a fixed-length message to cancel block requests
    ///
    /// The payload contains the following information in order:
    ///   index  : u32 specifying the zero-based piece index
    ///   begin  : u32 specifying the zero-based byte offset within the piece
    ///   length : u32 specifying the requested length
    Cancel = 8,
    /// Indicates the port that this peer's DHT node is listening on.
    /// Typically sent by newer versions of the Mainline that implements a DHT tracker.
    ///
    /// This peer should be inserted in the local routing table if DHT tracker is supported.
    Port = 9,

    // Fast Extension Messages
    // If the Fast Extension is disabled, then we must close the connection upon receiving any of
    // these messages.
    // More detailed descriptions of these message tags and their use cases can be found at
    // www.bittorrent.org/beps/bep_0006.html
    /// Advisory message, meaning "you might like to download this piece"
    /// Intended for "super-seeding", to avoid redundant downloads, and so I/O bound seeds can
    /// upload multiple pieces without having to do excessive disk reads.
    ///
    /// Payload:
    ///   index : u32 integer specifying the zero-based piece index
    SuggestPiece = 0x0D,
    /// This client is a seed and contains all pieces.
    /// This should be preferred over sending the BitField when possible, since there is less
    /// message overhead
    HaveAll = 0x0E,
    /// This client is a leech and contains no pieces.
    /// This should be preferred over sending the BitField when possible, since there is less
    /// message overhead
    HaveNone = 0x0F,
    /// Notifies a requesting peer that its request will not be satisfied
    ///
    /// Payload:
    ///   index  : u32 specifying the zero-based piece index
    ///   begin  : u32 specifying the zero-based byte offset within the piece
    ///   length : u32 specifying the requested length
    RejectRequest = 0x10,
    /// Advisory message indicating that if a client requests for a piece *even when choked*, the
    /// peer will send it
    AllowedFast = 0x11,
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
            0x0D => Ok(MessageId::SuggestPiece),
            0x0E => Ok(MessageId::HaveAll),
            0x0F => Ok(MessageId::HaveNone),
            0x10 => Ok(MessageId::RejectRequest),
            0x11 => Ok(MessageId::AllowedFast),
            _ => Err(anyhow::anyhow!("Invalid MessageId {val}")),
        }
    }
}
