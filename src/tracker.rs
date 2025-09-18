use std::collections::HashMap;

use crate::{parsing::Metadata, peer::Peer};

use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer};

use anyhow::Result;
use tokio::net::TcpListener;
use tracing::error;

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
        complete: usize,

        /// The number of non-seeder peers, aka "leechers"
        incomplete: usize,
    },
    /// Unsuccessful response from the tracker. Contains an error message
    Error {
        /// The value is a human-readable error message as to why the request failed.
        #[serde(rename = "failure reason")]
        failure_reason: String,
    },
}

#[derive(Debug)]
pub struct Peers(pub Vec<Peer>);

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
            value.chunks_exact(6).filter_map(Peer::from_bytes).collect(),
        ))
    }

    fn visit_seq<A>(self, mut _seq: A) -> Result<Self::Value, A::Error>
    where
        A: de::SeqAccess<'de>,
    {
        todo!();
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

/// Creates a populated Tracker URL for sending an initial request
pub fn create_tracker_url(metadata: &Metadata, listener: &TcpListener) -> Result<String> {
    let announce: reqwest::Url = metadata.announce.parse()?;
    match announce.scheme() {
        "http" | "https" => {}
        _ => {
            error!("invalid scheme");
            return Err(anyhow::Error::msg("Invalid Scheme".to_string()));
        }
    }
    let peer_id: [u8; 20] = std::env::var("USER_PEER_ID")
        .expect("USER_PEER_ID must be set.")
        .as_bytes()
        .try_into()
        .expect("invalid USER_PEER_ID.");
    let port = listener.local_addr().expect("getting addr").port();

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
    match metadata.info.torr_type {
        crate::parsing::TorrentType::SingleFile { length, .. } => {
            params.insert("left".into(), format!("{length}"));
        }
        _ => unimplemented!("don't have support for multiple files yet"),
    }

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

    Ok(format!("{}?{params}", metadata.announce))
}

pub async fn make_request(metadata: &Metadata, listener: &TcpListener) -> anyhow::Result<Response> {
    let announce = create_tracker_url(metadata, listener).expect("valid tracker URL");

    // let announce = reqwest::Url::parse_with_params(announce.as_str(), params).expect("unable to create tracker URL");
    let res = reqwest::get(announce).await.expect("invalid tracker URL");
    let body = res.bytes().await.expect("error reading body");

    Ok(serde_bencode::from_bytes(&body)?)
}
