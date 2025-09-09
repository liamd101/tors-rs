pub mod http;
pub mod udp;

use anyhow::Context;

use rand::prelude::*;

pub async fn get_peers(
    metadata: &crate::parsing::Metadata,
    listener: &tokio::net::TcpListener,
) -> anyhow::Result<Vec<crate::peer::Peer>> {
    match reqwest::Url::parse(&metadata.announce)?.scheme() {
        "http" | "https" => {
            let announce =
                http::create_tracker_url(metadata, listener).context("Invalid tracker URL.")?;

            let res = reqwest::get(announce)
                .await
                .context("Unable to reach tracker.")?;

            let body = res
                .bytes()
                .await
                .context("Couldn't read tracker response.")?;

            let body: http::Response = serde_bencode::from_bytes(&body)?;

            match body {
                http::Response::Success { peers, .. } => Ok(peers.0),
                http::Response::Error { failure_reason } => {
                    tracing::error!("tracker request failed: {failure_reason}");
                    Err(anyhow::anyhow!(failure_reason))
                }
            }
        }
        "udp" => {
            let connect_req = udp::Request::connect();
            todo!()
        }
        _ => Err(anyhow::anyhow!("Invalid tracker URL.")),
    }
}
