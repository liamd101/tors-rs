use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// The handshake is a required message and must be the first message transmitted by the client
/// It is (49+len(pstr)) bytes long
#[derive(Debug, Clone)]
pub struct Handshake {
    /// String length of pstr as a single raw byte
    pub pstrlen: u8,
    /// String identifier of the protocol
    pub pstr: String,
    /// 8 reserved bits. All current implementations use all zeroes. Each bit in these bytes can be
    /// used to change the behavior of the protocol
    pub reserved: [u8; 8],
    /// 20-byte SHA1 hash of the info key in the metainfo file. Same info_hash that is transmitted
    /// in tracker requests
    pub info_hash: [u8; 20],
    /// 20-byte string used as a unique ID for the client. This is usually the same peer_id that
    /// is transmitted in tracker requests, but not always.
    pub peer_id: [u8; 20],
}

impl Handshake {
    pub fn to_bytes(&self) -> Vec<u8> {
        let total_len = 1 + self.pstr.len() + 8 + 20 + 20;
        let mut bytes: Vec<u8> = Vec::with_capacity(total_len);
        bytes.push(self.pstrlen);
        bytes.extend_from_slice(self.pstr.as_bytes());
        bytes.extend_from_slice(&self.reserved);
        bytes.extend_from_slice(&self.info_hash);
        bytes.extend_from_slice(&self.peer_id);
        bytes
    }

    pub fn from_bytes(value: &[u8]) -> Result<Self, String> {
        if value.is_empty() {
            return Err("Input cannot be empty".to_string());
        }
        let pstrlen = value[0];
        if value.len() != 49 + (pstrlen as usize) {
            return Err(format!("pstrlen/pstr is incorrect: {pstrlen}"));
        }
        let pstr = String::from_utf8_lossy(&value[1..=(pstrlen as usize)]).to_string();
        let reserved: [u8; 8] = value[(pstrlen as usize + 1)..(pstrlen as usize + 9)]
            .try_into()
            .map_err(|_| "Invalid reserved field length")?;
        let info_hash: [u8; 20] = value[(pstrlen as usize + 9)..(pstrlen as usize + 29)]
            .try_into()
            .map_err(|_| "Invalid info_hash field length")?;
        let peer_id: [u8; 20] = value[(pstrlen as usize + 29)..(pstrlen as usize + 49)]
            .try_into()
            .map_err(|_| "Invalid peer_id field length")?;
        Ok(Handshake {
            pstrlen,
            pstr,
            reserved,
            info_hash,
            peer_id,
        })
    }

    pub fn v1(info_hash: [u8; 20], peer_id: [u8; 20]) -> Self {
        Self {
            info_hash,
            peer_id,
            pstrlen: 19,
            pstr: "BitTorrent protocol".to_string(),
            reserved: [0u8; 8],
        }
    }
}

pub async fn try_handshake(
    stream: &mut TcpStream,
    handshake: &Handshake,
) -> Result<bool, std::io::Error> {
    let handshake_bytes = handshake.to_bytes();

    stream.write_all(&handshake_bytes).await?;

    // first read will be 68 bytes the majority of the time according to the bittorrent spec
    let mut parts = vec![0u8; 68];

    stream.read_exact(&mut parts).await?;

    let peer_response = Handshake::from_bytes(&parts).expect("invalid peer response");
    Ok(peer_response.info_hash == handshake.info_hash)
}
