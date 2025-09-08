use crate::{
    ThreadUpdate,
    config::Config,
    download::{Download, monitor_file_progress},
    message::BitField,
    parsing::Metadata,
    peer::{Handshake, Peer, handle_peer, try_handshake},
    tracker,
};

use anyhow::{Context, Result};
use tokio::{sync::broadcast, task::JoinSet};
use tracing::{Instrument, debug, error, info, warn};

use std::sync::{Arc, RwLock};

pub struct Client {
    config: Config,
    metadata: Metadata,
    listener: tokio::net::TcpListener,
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
        if self.download.is_downloaded() {
            info!("file is downloaded already!!");
            return Ok(());
        }

        let peers = self.discover_peers().await?;
        debug!("tracker supplied {} peers", peers.len());

        self.start_download(peers).await
    }

    async fn connect_to_peer(&self, peer: &Peer) -> Result<tokio::net::TcpStream> {
        let mut stream = tokio::net::TcpStream::connect(peer.socket_addr)
            .await
            .expect("couldn't connect to peer");

        let handshake = Handshake::v1(self.metadata.info_hash(), self.config.peer_id);

        match try_handshake(&mut stream, &handshake).await {
            Ok(true) => Ok(stream),
            Ok(false) => anyhow::bail!("Peer failed handshake"),
            Err(e) => Err(e).context("handshake error"),
        }
    }

    async fn discover_peers(&self) -> Result<Vec<Peer>> {
        let res = tracker::http::make_request(&self.metadata, &self.listener)
            .await
            .context("Unable to contact tracker.")?;
        match res {
            tracker::http::Response::Success { peers, .. } => Ok(peers.0),
            tracker::http::Response::Error { failure_reason } => {
                anyhow::bail!("Making request to tracker failed: {failure_reason}")
            }
        }
    }

    async fn start_download(self, peers: Vec<Peer>) -> Result<()> {
        let my_bitfield = self.download.bitfield();
        let (tx, rx) = broadcast::channel::<ThreadUpdate>(32);
        let mut set = JoinSet::new();

        self.spawn_file_monitor(&mut set, self.download.clone(), tx.clone(), rx);

        self.spawn_peer_connections(&mut set, peers, tx, my_bitfield)
            .await;

        while set.join_next().await.is_some() {}

        Ok(())
    }

    async fn spawn_peer_connections(
        &self,
        task_set: &mut JoinSet<()>,
        peers: Vec<Peer>,
        tx: broadcast::Sender<ThreadUpdate>,
        bitfield: Arc<RwLock<BitField>>,
    ) {
        for peer in peers.iter().take(self.config.max_peers) {
            match self.connect_to_peer(peer).await {
                Ok(stream) => self.spawn_peer_handler(
                    task_set,
                    peer.clone(),
                    stream,
                    tx.clone(),
                    bitfield.clone(),
                ),
                Err(e) => {
                    warn!("Failed to connect to peer {}: {}", peer.socket_addr, e);
                }
            }
        }
    }

    fn spawn_peer_handler(
        &self,
        task_set: &mut JoinSet<()>,
        peer: Peer,
        stream: tokio::net::TcpStream,
        tx: broadcast::Sender<ThreadUpdate>,
        bitfield: Arc<RwLock<BitField>>,
    ) {
        let metadata = self.metadata.clone();
        let thread_rx = tx.subscribe();

        task_set.spawn(async move {
            let span = tracing::info_span!("peer", peer_addr = %peer.socket_addr);
            match handle_peer(tx, thread_rx, stream, metadata, bitfield)
                .instrument(span)
                .await
            {
                Ok(()) => {}
                Err(e) => error!("{e}"),
            }
        });
    }

    fn spawn_file_monitor(
        &self,
        task_set: &mut JoinSet<()>,
        mut download: Download,
        tx: broadcast::Sender<ThreadUpdate>,
        rx: broadcast::Receiver<ThreadUpdate>,
    ) {
        task_set.spawn(async move {
            let span = tracing::info_span!("file_download");
            if let Err(e) = monitor_file_progress(&mut download, tx, rx)
                .instrument(span)
                .await
            {
                error!("File monitoring error: {e}");
            };
            info!("Finished downloading file!");
        });
    }
}
