use crate::{
    ThreadUpdate,
    message::BitField,
    parsing::Hashes,
    parsing::Metadata,
    parsing::{File, TorrentType},
};

use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};
use sha1::{Digest, Sha1};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tracing::{debug, info, instrument, warn};

/// A data structure representing information for downloading a file from a `.torrent` file.
#[derive(Debug, Clone)]
pub struct Download {
    /// The location of the file being downloaded
    pub name: PathBuf,
    piece_length: u64,
    length: u64,

    files: Vec<File>,

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
    pub async fn new(metadata: &Metadata) -> Result<Self> {
        let num_pieces = metadata.num_pieces();
        let bitfield = Arc::new(RwLock::new(BitField::with_settable(num_pieces as u64)));

        let files = match &metadata.info.torr_type {
            &TorrentType::SingleFile { length, .. } => {
                let path: Vec<String> = metadata
                    .info
                    .name
                    .split(std::path::MAIN_SEPARATOR)
                    .map(|s| s.to_string())
                    .collect();
                vec![File { length, path }]
            }
            TorrentType::MultiFile { files } => files
                .iter()
                .map(|file| {
                    let mut path = vec![metadata.info.name.clone()];
                    path.extend_from_slice(&file.path);
                    File {
                        length: file.length,
                        path,
                    }
                })
                .collect(),
        };

        let mut out = Self {
            name: PathBuf::from(&metadata.info.name),
            piece_length: metadata.info.piece_length,
            length: metadata.info.torr_type.len(),
            num_pieces,
            piece_hashes: metadata.info.pieces.clone(),
            bitfield,
            files,
        };

        debug!("initializing files");
        out.initialize_files().await?;
        debug!("checking downloaded pieces");
        out.update_downloads().await?;
        Ok(out)
    }

    async fn initialize_files(&self) -> Result<()> {
        for file in &self.files {
            let path = file.path.iter().collect::<PathBuf>();
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .context("Failed to create directories for {parent:?}")?;
            }
            let file_handle = tokio::fs::File::options()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(&path)
                .await
                .context("Failed to initialize file {path:?}")?;
            if file_handle.metadata().await?.len() == 0 {
                file_handle.set_len(file.length).await?;
                debug!("initialized file {path:?} (size: {})", file.length);
            }
        }
        Ok(())
    }

    pub fn is_downloaded(&self) -> bool {
        self.bitfield.read().unwrap().set_bits().len() == self.num_pieces
    }

    /// Iterates through all pieces of the file, computes their SHA1 hash, and then sets their
    /// correspondign bits in the BitField to true/false accordingly
    pub async fn update_downloads(&mut self) -> Result<Vec<usize>> {
        let mut changed = vec![];

        for piece in 0..self.num_pieces {
            if self.update_piece(piece).await? {
                changed.push(piece);
            }
        }

        Ok(changed)
    }

    async fn update_piece(&mut self, piece: usize) -> Result<bool> {
        let piece_data = self
            .get_piece_data(piece as u64)
            .await
            .context("Couldn't get piece data.")?;
        let mut hasher = Sha1::new();
        hasher.update(piece_data);
        let piece_hash: [u8; 20] = hasher.finalize().into();
        let hash: [u8; 20] = self.piece_hashes.0[piece];

        let finished_download = piece_hash == hash;

        let prev = self
            .bitfield
            .write()
            .unwrap()
            .set(piece, finished_download)
            .expect("index out of bounds");

        // return whether the piece is updated or not
        Ok(finished_download && !prev)
    }

    async fn get_piece_data(&self, piece_idx: u64) -> Result<Bytes> {
        let piece_start = piece_idx * self.piece_length;
        let piece_end = std::cmp::min(piece_start + self.piece_length, self.length);
        let mut piece_data = BytesMut::with_capacity((piece_end - piece_start) as usize);

        let mut seen_length = 0;
        for file in &self.files {
            let file_start = seen_length;
            let file_end = seen_length + file.length;

            if piece_start < file_end && file_start < piece_end {
                let seek_offset = piece_start.saturating_sub(file_start);
                let read_start_in_torrent = std::cmp::max(piece_start, file_start);
                let read_end_in_torrent = std::cmp::min(piece_end, file_end);
                let bytes_to_read = read_end_in_torrent - read_start_in_torrent;

                let mut file_data = tokio::fs::File::options()
                    .create(true)
                    .read(true)
                    .write(true)
                    .truncate(false)
                    .open(file.path.join(std::path::MAIN_SEPARATOR_STR))
                    .await
                    .context("Couldn't open file.")?;
                if file_data.metadata().await?.len() == 0 {
                    file_data.set_len(file.length).await?;
                }

                file_data.seek(SeekFrom::Start(seek_offset)).await?;
                let mut file_bytes = vec![0u8; bytes_to_read as usize];
                file_data.read_exact(&mut file_bytes).await?;
                piece_data.extend_from_slice(&file_bytes);
            }
            seen_length += file.length;
        }

        if piece_data.len() != self.piece_length as usize
            && piece_data.len() != (self.length % self.piece_length) as usize
        {
            debug!("piece_data.len={}", piece_data.len());
            debug!("piece_length={}", self.piece_length);
            anyhow::bail!("Piece data has incorrect size.")
        } else {
            Ok(piece_data.freeze())
        }
    }

    pub fn bitfield(&self) -> Arc<RwLock<BitField>> {
        self.bitfield.clone()
    }
}

#[instrument(skip(download, tx, rx))]
pub async fn monitor_file_progress(
    download: &mut Download,
    tx: tokio::sync::broadcast::Sender<ThreadUpdate>,
    mut rx: tokio::sync::broadcast::Receiver<ThreadUpdate>,
) -> Result<()> {
    loop {
        match rx.recv().await {
            Ok(ThreadUpdate::Downloaded(piece, _)) => {
                let new_download = download.update_piece(piece as usize).await?;
                if new_download {
                    tx.send(ThreadUpdate::Completed(piece))?;
                }
                if download.is_downloaded() {
                    tx.send(ThreadUpdate::FileComplete)?;
                }
            }
            Ok(ThreadUpdate::Completed(piece)) => {
                info!("Downloaded piece {piece}");
            }
            Ok(ThreadUpdate::FileComplete) => {
                info!("file complete received");
                break;
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                warn!("Lagged {n} messages.");
            }
        }
    }
    Ok(())
}
