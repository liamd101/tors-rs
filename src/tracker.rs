use std::collections::HashMap;
use std::net::SocketAddr;

use crate::parsing::Metadata;

use serde::de::{self, Error, Visitor};
use serde::{Deserialize, Deserializer};

use anyhow::{Context, Result};
use tokio::net::TcpListener;

use tracing::debug;

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum Response {
    /// Successful response from the tracker. Contains necessary info for communicating with peers
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

        /// A list of Peers that the Tracker is aware of. This is the list that the client will
        /// communicate with at all times.
        peers: Peers,

        /// The number of peers with the entire file, i.e. seeders
        complete: Option<usize>,

        /// The number of non-seeder peers, aka "leechers"
        incomplete: Option<usize>,
    },
    /// Unsuccessful response from the tracker. Contains an error message
    Error {
        /// The value is a human-readable error message as to why the request failed.
        #[serde(rename = "failure reason")]
        failure_reason: String,
    },
}

#[derive(Debug)]
pub struct Peers(pub Vec<SocketAddr>);

#[derive(Deserialize, Debug)]
struct PeerDict {
    ip: String,
    port: usize,
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
        if !value.len().is_multiple_of(6) {
            return Err(E::custom(
                "byte string length must be multiple of 6".to_string(),
            ));
        }
        let peers = value
            .chunks_exact(6)
            .filter_map(|b| {
                if b.len() < 6 {
                    return None;
                }
                let ip_addr = std::net::Ipv4Addr::new(b[0], b[1], b[2], b[3]);
                let port = u16::from_be_bytes([b[4], b[5]]);
                Some(std::net::SocketAddr::new(
                    std::net::IpAddr::V4(ip_addr),
                    port,
                ))
            })
            .collect();
        Ok(Peers(peers))
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: de::SeqAccess<'de>,
    {
        let mut peers = Vec::new();
        while let Some(peer_dict) = seq.next_element::<PeerDict>()? {
            let ip_addr = peer_dict
                .ip
                .parse::<std::net::Ipv4Addr>()
                .map_err(|_| A::Error::custom(format!("Invalid IP address: {}", peer_dict.ip)))?;
            let port: u16 = peer_dict.port.try_into().unwrap();
            let socket_addr = std::net::SocketAddr::new(std::net::IpAddr::V4(ip_addr), port);
            peers.push(socket_addr);
        }
        Ok(Peers(peers))
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

#[allow(dead_code)]
enum TrackerEvent {
    Started,
    Completed,
    Stopped,
}
impl std::fmt::Display for TrackerEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TrackerEvent::Started => write!(f, "started"),
            TrackerEvent::Completed => write!(f, "completed"),
            TrackerEvent::Stopped => write!(f, "stopped"),
        }
    }
}

fn format_tracker_params(metadata: &Metadata, listener: &TcpListener) -> anyhow::Result<String> {
    let peer_id: [u8; 20] = std::env::var("USER_PEER_ID")
        .expect("USER_PEER_ID must be set.")
        .as_bytes()
        .try_into()
        .context("invalid USER_PEER_ID.")?;
    let port = listener.local_addr().context("getting addr")?.port();

    let mut params: HashMap<String, String> = HashMap::new();
    params.insert("port".into(), format!("{port}"));
    params.insert("event".into(), TrackerEvent::Started.to_string());
    params.insert("compact".into(), "1".into());
    params.insert("uploaded".into(), "0".into());
    params.insert("downloaded".into(), "0".into());
    params.insert(
        "peer_id".into(),
        urlencoding::encode_binary(&peer_id).to_string(),
    );
    params.insert("left".into(), format!("{}", metadata.info.torr_type.len()));

    let info_hash = metadata.info_hash();
    params.insert(
        "info_hash".into(),
        urlencoding::encode_binary(&info_hash).to_string(),
    );

    // what the fuck
    let params = params
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<String>>()
        .join("&");

    Ok(params)
}

pub async fn make_request(metadata: &Metadata, listener: &TcpListener) -> anyhow::Result<Response> {
    let http_params = format_tracker_params(metadata, listener)?;

    match &metadata.announce_list {
        Some(announce_list) => {
            for announce in announce_list {
                let announce_url: reqwest::Url =
                    announce.parse().context("couldn't parse into URL")?;

                match announce_url.scheme() {
                    "http" | "https" => {}
                    _ => continue,
                }
                let announce = format!("{}?{}", announce, http_params);
                let res = reqwest::get(announce)
                    .await
                    .context("invalid tracker URL")?;
                debug!("announce list");
                let body = res.bytes().await.context("error reading body")?;

                return Ok(serde_bencode::from_bytes(&body)?);
            }
        }
        None => {
            let announce: reqwest::Url = metadata
                .announce
                .parse()
                .context("couldn't parse into URL")?;
            match announce.scheme() {
                "http" | "https" => {}
                _ => anyhow::bail!("Unsupported Tracker scheme"),
            }

            debug!("announce");

            let announce = format!("{}?{}", metadata.announce, http_params);
            debug!("{announce}");
            let res = reqwest::get(announce)
                .await
                .context("invalid tracker URL")?;

            let body = res.bytes().await.context("error reading body")?;
            let decoded: Response = serde_bencode::from_bytes(&body)?;

            return Ok(decoded);
        }
    }

    Err(anyhow::anyhow!("No valid URLs found"))
}
