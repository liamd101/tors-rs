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
use tokio::sync::RwLock;

use anyhow::{Context, Result};
use bitvec::prelude::*;
use tokio::{net::TcpListener, select, sync::broadcast, task::JoinSet};
use tracing::{Instrument, debug, info, warn};

pub struct Client {
    config: Config,
    metadata: Metadata,
    listener: TcpListener,
    download: Download,
}
impl Client {
    pub async fn new(config: Config) -> Result<Self> {
        let listener = crate::bind_port()
            .await
            .context("Unable to find open port.")?;

        let metadata = Metadata::new(&config.file).context("Unable to parse torrent file.")?;

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

    pub async fn run(self) -> Result<()> {
        let (tx, rx) = broadcast::channel::<ThreadUpdate>(32);
        let mut set: JoinSet<Result<()>> = JoinSet::new();

        let my_bitfield = self.download.bitfield();

        let mut download = self.download.clone();
        let file_tx = tx.clone();
        set.spawn(async move {
            let span = tracing::info_span!("file_download");
            monitor_file_progress(&mut download, file_tx, rx)
                .instrument(span)
                .await
        });

        // if we still need to download stuff, then we'll reach out to peers
        if self.download.is_downloaded().await {
            info!("File is downloaded already. Seeding to peers");
            tracker::make_request(
                self.config.peer_id,
                &self.metadata,
                &self.listener,
                self.metadata.num_pieces(),
            )
            .await
            .context("Unable to contact tracker.")?;
        } else {
            let peers = self.discover_peers().await?;
            debug!("tracker supplied {} peers", peers.len());
            self.spawn_peer_connections(&mut set, peers, tx.clone(), my_bitfield.clone())
                .await?;
        }

        info!("starting listening loop");

        loop {
            info!("inside the loop");
            let handshake = Handshake::v1(self.metadata.info_hash(), self.config.peer_id);
            select! {
                Ok((mut stream, socket_addr)) = self.listener.accept() => {
                    info!("Received connection from peer {socket_addr}");
                    if try_handshake(&mut stream, &handshake).await? {
                        let metadata = self.metadata.clone();
                        let tx = tx.clone();
                        let thread_rx = tx.subscribe();
                        let bitfield = my_bitfield.clone();

                        set.spawn(async move {
                            let span = tracing::info_span!("peer", peer_addr = %socket_addr);
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
        info!("ending listening loop");

        Ok(())
    }

    async fn discover_peers(&self) -> Result<Vec<SocketAddr>> {
        let res = tracker::make_request(
            self.config.peer_id,
            &self.metadata,
            &self.listener,
            self.download.num_completed_pieces().await,
        )
        .await
        .context("Unable to contact tracker.")?;
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
        peers: Vec<SocketAddr>,
        tx: broadcast::Sender<ThreadUpdate>,
        bitfield: Arc<RwLock<BitVec<u8, Msb0>>>,
    ) -> Result<()> {
        for peer in peers.iter().take(self.config.max_peers) {
            let Ok(mut stream) = tokio::net::TcpStream::connect(peer)
                .await
                .context("couldn't connect to peer")
            else {
                continue;
            };

            let handshake = Handshake::v1(self.metadata.info_hash(), self.config.peer_id);

            match try_handshake(&mut stream, &handshake)
                .await
                .with_context(|| "sending handshake")
            {
                Ok(true) => {
                    let metadata = self.metadata.clone();
                    let tx = tx.clone();
                    let thread_rx = tx.subscribe();
                    let peer = *peer;
                    let bitfield = bitfield.clone();

                    task_set.spawn(async move {
                        let span = tracing::info_span!("peer", peer_addr = %peer);
                        handle_peer(tx, thread_rx, stream, metadata, bitfield)
                            .instrument(span)
                            .await
                    });
                }
                Ok(false) => warn!("{peer} failed handshake"),
                Err(e) => warn!("Failed to connect to {peer}: {e:?}"),
            }
        }
        Ok(())
    }
}
