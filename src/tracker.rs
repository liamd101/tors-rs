use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer};

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum TrackerResponse {
    Success {
        /// Similar to failure reason, but response still gets processed normally. Message is shown
        /// just like an error.
        #[serde(rename = "warning message")]
        warning_message: Option<String>,

        /// Interval in seconds that the client should wait between sending regular requests to the tracker
        interval: usize,

        /// Minimum announce interval. If present clients must not reannounce more frequently than this.
        #[serde(rename = "min interval")]
        min_interval: Option<usize>,

        /// A string that the client should send back on its next announcements. If absent and a
        /// previous announce sent a tracker id, do not discard the old value; keep using it.
        tracker_id: Option<String>,

        peers: Peers,
        /// The number of peers with the entire file, i.e. seeders
        complete: usize,

        /// The number of non-seeder peers, aka "leechers"
        incomplete: usize,
    },
    Error {
        /// The value is a human-readable error message as to why the request failed (string).
        #[serde(rename = "failure reason")]
        failure_reason: String,
    },
}

#[derive(Debug)]
pub struct Peers(pub Vec<Peer>);

#[derive(Debug)]
pub struct Peer {
    pub socket_addr: std::net::SocketAddr,
    am_choking: bool,
    am_interested: bool,
    peer_choking: bool,
    peer_interested: bool,
}
impl Default for Peer {
    fn default() -> Self {
        Self {
            socket_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
            am_choking: true,
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
        }
    }
}

struct PeersVisitor;
impl<'de> Visitor<'de> for PeersVisitor {
    type Value = Peers;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a byte string of peers or a list of dictionary entries")
    }

    fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if value.len() % 6 != 0 {
            return Err(E::custom(
                "byte string length must be multiple of 6".to_string(),
            ));
        }
        Ok(Peers(
            value
                .chunks_exact(6)
                .map(|b| {
                    let ip_addr = std::net::Ipv4Addr::new(b[0], b[1], b[2], b[3]);
                    let port = u16::from_be_bytes([b[4], b[5]]);
                    Peer {
                        socket_addr: SocketAddr::new(std::net::IpAddr::V4(ip_addr), port),
                        ..Default::default()
                    }
                })
                .collect(),
        ))
    }

    fn visit_seq<A>(self, mut _seq: A) -> Result<Self::Value, A::Error>
    where
        A: de::SeqAccess<'de>,
    {
        let mut _peers = vec![];
        todo!();
        Ok(Peers(_peers))
    }
}

impl<'de> Deserialize<'de> for Peers {
    fn deserialize<D>(deserializer: D) -> Result<Peers, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_bytes(PeersVisitor)
    }
}
