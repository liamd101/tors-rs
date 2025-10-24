mod config;
mod download;
mod parsing;
mod peer;
mod torrent;
mod tracker;
pub use config::{Config, init_logging};
pub use torrent::Client;

#[allow(dead_code)]
#[derive(Clone, Debug)]
/// Enum representing some messages that get sent between peer threads and parent threads.
pub(crate) enum ThreadUpdate {
    /// `Downloaded(piece_idx, block_idx)`
    /// Indicates that the given piece/block pair have been downloaded successfully.
    /// Sent from peer threads after successfully reading a block from a peer.
    /// Upon receiving this message, the parent thread checks if the any new pieces have been
    /// successfully downloaded.
    Downloaded(u32, u32),
    /// Indicates that the inner piece has been successfully downloaded, as confirmed by the parent
    /// thread computing the downloaded piece hash and comparing against the piece hash that is
    /// present in the `.torrent` file.
    Completed(u32),
    /// Indicates that the entire file has been completed, and that any requests for the file
    /// should be cancelled.
    FileComplete,
    /// Indicates that the program is exiting, and we should close all peer connections
    Disconnect,
}

/// Looks for and binds a TcpListener to an open port between 6881 and 6889 inclusive.
/// This is per the BitTorrent spec, where these are the list of recommended ports.
pub(crate) async fn bind_port() -> anyhow::Result<tokio::net::TcpListener> {
    for port_num in 6881..=6889 {
        match tokio::net::TcpListener::bind(format!("0.0.0.0:{port_num}")).await {
            Ok(out) => return Ok(out),
            Err(_) => continue,
        }
    }
    Err(anyhow::anyhow!("Unable to find an open port"))
}
