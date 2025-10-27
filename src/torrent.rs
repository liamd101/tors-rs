use crate::{
    ThreadUpdate,
    config::Config,
    download::{Download, monitor_file_progress},
    parsing::Metadata,
    peer::{handle_peer, handshake::Handshake, handshake::try_handshake},
    tracker,
};

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use bitvec::prelude::*;
use once_cell::sync::OnceCell;
use rand::seq::SliceRandom;
use tokio::sync::RwLock;
use tokio::{net::TcpListener, select, sync::broadcast, task::JoinSet};
use tracing::{Instrument, debug, error, info, warn};

pub(crate) static OUTPUT_DIR: OnceCell<Option<String>> = OnceCell::new();

/// Instance of a BitTorrent Client. This struct handles downloading/seeding for a specific
/// `.torrent` file.
pub struct Client {
    /// Configuration settings for the client.
    config: Config,
    /// Metadata of the `.torrent` file being downloaded.
    metadata: Metadata,
    /// Socket we are listening on.
    listener: TcpListener,
    /// Current download status of the `.torrent` file.
    download: Download,
}
impl Client {
    /// Create a new instance of a `Client` from a `Config` object.
    pub async fn new(config: Config) -> Result<Self> {
        let listener = crate::bind_port()
            .await
            .context("Unable to find open port.")?;

        match OUTPUT_DIR.set(config.args.dir.clone()) {
            Ok(()) => {}
            Err(e) => {
                error!("{e:?}");
                anyhow::bail!("Unable to initialize global variable");
            }
        }

        let metadata = Metadata::new(&config.args.file).context("Unable to parse torrent file.")?;

        let download = Download::new(&metadata)
            .await
            .context("Couldn't create download struct")?;

        Ok(Self {
            config,
            listener,
            metadata,
            download,
        })
    }

    /// Begins running the client on the specified `.torrent` file.
    pub async fn run(self) -> Result<()> {
        info!("Running BitTorrent client");

        let (tx, rx) = broadcast::channel::<ThreadUpdate>(32);
        let mut set: JoinSet<Result<()>> = JoinSet::new();

        let my_bitfield = self.download.bitfield();

        debug!("my_bitfield={my_bitfield:?}");

        let mut download = self.download.clone();
        let file_tx = tx.clone();
        set.spawn(async move {
            let span = tracing::info_span!("file_download");
            monitor_file_progress(&mut download, file_tx, rx)
                .instrument(span)
                .await
        });

        // if we still need to download stuff, then we'll reach out to peers
        let peers = self.discover_peers().await?;
        self.spawn_peer_connections(&mut set, peers, tx.clone(), my_bitfield.clone())
            .await?;

        info!("Listening on port {}", self.listener.local_addr()?.port());
        loop {
            select! {
                Ok((mut stream, socket_addr)) = self.listener.accept() => {
                    let handshake = Handshake::v1(self.metadata.info_hash(), self.config.peer_id);
                    info!("Received connection from peer {socket_addr}");
                    let Ok(handshake) = try_handshake(&mut stream, &handshake).await else {
                        warn!("Peer handshake failed");
                        continue;
                    };
                    if let Some(handshake) = handshake {
                        let metadata = self.metadata.clone();
                        let tx = tx.clone();
                        let thread_rx = tx.subscribe();
                        let bitfield = my_bitfield.clone();

                        set.spawn(async move {
                            let span = tracing::info_span!("peer", peer_id=%String::from_utf8_lossy(&handshake.peer_id));
                            handle_peer(tx, thread_rx, stream, metadata, bitfield)
                                .instrument(span)
                                .await
                        });
                    } else {
                        warn!("Could not connect to peer {socket_addr}");
                    }
                }
                Some(join_result) = set.join_next() => {
                    info!("Thread closed: {join_result:?}");
                }
                else => {
                    info!("broken :(");
                    break;
                }
            }
        }

        Ok(())
    }

    /// Contacts the tracker and returns a list of peers to contact
    async fn discover_peers(&self) -> Result<Vec<SocketAddr>> {
        let res = tracker::make_request(
            self.config.peer_id,
            &self.metadata,
            &self.listener,
            self.download.num_completed_pieces().await,
        )
        .await
        .context("Unable to contact tracker.")?;
        debug!("response={res:?}");
        match res {
            tracker::Response::Success { peers, .. } => Ok(peers.0),
            tracker::Response::Error { failure_reason } => {
                anyhow::bail!("Making request to tracker failed: {failure_reason}")
            }
        }
    }

    async fn spawn_peer_connections(
        &self,
        task_set: &mut JoinSet<Result<()>>,
        mut peers: Vec<SocketAddr>,
        tx: broadcast::Sender<ThreadUpdate>,
        bitfield: Arc<RwLock<BitVec<u8, Msb0>>>,
    ) -> Result<()> {
        info!("peers={peers:?}");
        peers.shuffle(&mut rand::rng());
        for peer in peers.iter().take(self.config.args.max_peers) {
            let mut stream = match tokio::net::TcpStream::connect(peer)
                .await
                .context("couldn't connect to peer")
            {
                Ok(stream) => stream,
                Err(e) => {
                    warn!("peer connection failed: {e}");
                    continue;
                }
            };

            let handshake = Handshake::v1(self.metadata.info_hash(), self.config.peer_id);

            match try_handshake(&mut stream, &handshake).await {
                Ok(Some(handshake)) => {
                    let metadata = self.metadata.clone();
                    let tx = tx.clone();
                    let thread_rx = tx.subscribe();
                    let bitfield = bitfield.clone();

                    task_set.spawn(async move {
                        let span = tracing::info_span!("peer", peer_id = %String::from_utf8_lossy(&handshake.peer_id));
                        handle_peer(tx, thread_rx, stream, metadata, bitfield)
                            .instrument(span)
                            .await
                    });
                }
                Ok(None) => continue,
                Err(e) => warn!("Failed to connect to {peer}: {e:?}"),
            }
        }
        Ok(())
    }
}
