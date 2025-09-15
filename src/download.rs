use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use crate::{ThreadUpdate, message::BitField, parsing::Hashes, parsing::Metadata};

use anyhow::Result;
use sha1::{Digest, Sha1};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
};

use tracing::{debug, info, warn};

/// A data structure representing information for downloading a file from a `.torrent` file.
#[derive(Debug, Clone)]
pub struct Download {
    /// The location of the file being downloaded
    pub name: PathBuf,
    piece_length: u64,
    length: u64,
    /// The number of pieces in the file being downloaded
    num_pieces: usize,
    /// The piece hashes of the torrent file being downloaded. Derived from the `.torrent` file
    piece_hashes: Hashes,
    /// A BitField of the currently downloaded pieces. Read from left-to-right with a 1 set if the
    /// piece is downloaded and verified. 0 otherwise
    bitfield: Arc<RwLock<BitField>>,
}

impl Download {
    // TODO: update this error type to something more robust
    pub async fn new(metadata: &Metadata) -> anyhow::Result<Self> {
        let num_pieces = metadata.num_pieces();
        let bitfield = Arc::new(RwLock::new(BitField::with_settable(num_pieces as u64)));

        let mut out = Self {
            name: PathBuf::from(&metadata.info.name),
            piece_length: metadata.info.piece_length,
            length: metadata.info.torr_type.len(),
            num_pieces,
            piece_hashes: metadata.info.pieces.clone(),
            bitfield,
        };
        out.update_downloads().await?;
        Ok(out)
    }

    pub fn is_downloaded(&self) -> bool {
        self.bitfield.read().unwrap().set_bits().len() == self.num_pieces
    }

    /// Iterates through all pieces of the file, computes their SHA1 hash, and then sets their
    /// correspondign bits in the BitField to true/false accordingly
    pub async fn update_downloads(&mut self) -> Result<Vec<usize>, std::io::Error> {
        let mut file: File = tokio::fs::File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&self.name)
            .await?;

        if file.metadata().await?.len() == 0 {
            file.set_len(self.length).await?;
            return Ok(vec![]); /* no need to check pieces since the file has not been written to */
        }

        let mut changed = vec![];

        for piece in 0..self.num_pieces {
            let piece_len = if piece == (self.num_pieces - 1) {
                self.length % self.piece_length
            } else {
                self.piece_length
            };
            let mut piece_data: Vec<u8> = vec![0u8; piece_len as usize];
            file.seek(SeekFrom::Start(self.piece_length * piece as u64))
                .await?;
            file.read_exact(&mut piece_data).await?;

            let mut hasher = Sha1::new();
            hasher.update(piece_data);
            let piece_hash: [u8; 20] = hasher.finalize().into();
            let hash: [u8; 20] = self.piece_hashes.0[piece];

            let finished_download: bool = piece_hash == hash;

            let prev = self
                .bitfield
                .write()
                .unwrap()
                .set(piece, finished_download)
                .expect("index out of bounds");

            if finished_download && !prev {
                changed.push(piece);
            }
        }

        Ok(changed)
    }

    pub fn bitfield(&self) -> Arc<RwLock<BitField>> {
        self.bitfield.clone()
    }
}

pub async fn monitor_file_progress(
    download: &mut Download,
    tx: tokio::sync::broadcast::Sender<ThreadUpdate>,
    mut rx: tokio::sync::broadcast::Receiver<ThreadUpdate>,
) -> anyhow::Result<()> {
    loop {
        match rx.recv().await {
            Ok(ThreadUpdate::Downloaded(piece, block)) => {
                debug!("downloaded piece={piece} block={block}");
                let changed = download.update_downloads().await?;
                debug!("changed pieces={changed:?}");
                for changed_piece in changed {
                    tx.send(ThreadUpdate::Completed(changed_piece as u32))?;
                }
                if download.is_downloaded() {
                    tx.send(ThreadUpdate::FileComplete)?;
                }
            }
            Ok(ThreadUpdate::Completed(_)) => {
                continue;
            }
            Ok(ThreadUpdate::FileComplete) => {
                info!("file complete received");
                break;
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                warn!("watch_download: Lagged {n} messages.");
            }
        }
    }
    info!("watch_download: finished");
    Ok(())
}
