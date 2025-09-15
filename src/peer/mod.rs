pub mod handshake;
mod connection;
pub use connection::handle_peer;
mod pieces;

#[derive(Debug, Clone)]
pub struct Peer {
    pub socket_addr: std::net::SocketAddr,
    pub peer_id: String,
}
impl Default for Peer {
    fn default() -> Self {
        Self {
            socket_addr: std::net::SocketAddr::new(
                std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
                0,
            ),
            peer_id: String::new(),
        }
    }
}

impl Peer {
    pub fn new(socket_addr: std::net::SocketAddr) -> Self {
        Self {
            socket_addr,
            ..Default::default()
        }
    }

    pub fn from_bytes(b: &[u8]) -> Option<Self> {
        if b.len() < 6 {
            return None;
        }
        let ip_addr = std::net::Ipv4Addr::new(b[0], b[1], b[2], b[3]);
        let port = u16::from_be_bytes([b[4], b[5]]);
        Some(Peer::new(std::net::SocketAddr::new(
            std::net::IpAddr::V4(ip_addr),
            port,
        )))
    }
}

const BLOCK_SIZE: u32 = 1 << 14;

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BlockState {
    UnRequested,
    Requested,
    Completed,
}

