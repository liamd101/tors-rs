use bitvec::prelude::*;
use rand::seq::IndexedRandom;

use super::{BLOCK_SIZE, BlockState};

pub(crate) struct PieceTracker {
    num_requested: usize,
    pipeline_len: usize,
    blocks: Vec<Vec<BlockState>>,
    requested: Option<usize>,
}
impl PieceTracker {
    pub fn from_file_info(file_length: u64, piece_length: u64) -> Self {
        let num_pieces = file_length.div_ceil(piece_length);
        let last_piece_begin = (num_pieces - 1) * piece_length;
        let last_piece_size = file_length - last_piece_begin;
        let last_piece_num_blocks = last_piece_size.div_ceil(BLOCK_SIZE as u64);
        let standard_num_blocks = piece_length.div_ceil(BLOCK_SIZE as u64);
        let mut blocks = vec![
            vec![BlockState::UnRequested; standard_num_blocks as usize];
            num_pieces as usize - 1
        ];
        blocks.push(vec![
            BlockState::UnRequested;
            last_piece_num_blocks as usize
        ]);
        Self {
            num_requested: 0,
            pipeline_len: 5,
            blocks,
            requested: None,
        }
    }

    /// Updates `self` so that all blocks and pieces that are set true in the bitfield are set to `Completed`
    /// in `self.blocks`
    pub fn update(&mut self, bitfield: &BitVec<u8, Msb0>) {
        for piece_idx in bitfield.iter_ones() {
            let piece = self.blocks.get_mut(piece_idx).unwrap();
            *piece = vec![BlockState::Completed; piece.len()];
        }
    }

    /// Requests a new (piece,block) combo.
    /// If a piece has already been requested, but not completed, then we will first try to request
    /// a block from that piece.
    /// Otherwise, we will pick a new random piece and add it to a stack of pieces to request.
    ///
    /// if we shouldn't request a specific piece, request a random piece and update requested piece
    pub fn request(&mut self, pieces: Vec<usize>) -> Option<(u32, u32)> {
        if self.num_requested >= self.pipeline_len {
            return None;
        }

        // If we need to request a new piece, pick a random piece and select a the first
        // unrequested block
        if self.requested.is_none() {
            let requestable_pieces: Vec<usize> = pieces
                .iter()
                .filter_map(|&piece_idx| {
                    let piece = self.blocks.get(piece_idx)?;
                    piece
                        .iter()
                        .any(|block| block == &BlockState::UnRequested)
                        .then_some(piece_idx)
                })
                .collect();

            let piece_idx = requestable_pieces.choose(&mut rand::rng()).copied()?;
            self.requested = Some(piece_idx);

            let piece = self.blocks.get_mut(piece_idx)?;
            for (block_idx, block) in piece.iter_mut().enumerate() {
                if block == &BlockState::UnRequested {
                    *block = BlockState::Requested;
                    self.num_requested += 1;
                    return Some((piece_idx as u32, block_idx as u32));
                }
            }

            return None;
        }

        let requested = self.requested.unwrap();

        let requested_piece = self.blocks.get_mut(requested)?;
        let Some((block_idx, _)) = requested_piece
            .iter()
            .enumerate()
            .find(|&(_, block)| block == &BlockState::UnRequested)
        else {
            // if we have requested all blocks of a piece, then we want to find a new piece to
            // request blocks from
            self.requested = None;
            return self.request(pieces);
        };

        requested_piece[block_idx] = BlockState::Requested;
        self.num_requested += 1;
        Some((requested as u32, block_idx as u32))
    }

    pub fn mark_block_as_downloaded(&mut self, piece: usize, block: usize) -> Option<bool> {
        let piece = self.blocks.get_mut(piece)?;
        let block = piece.get_mut(block)?;
        if block == &BlockState::Requested {
            self.num_requested -= 1;
        }
        *block = BlockState::Completed;
        Some(true)
    }

    // have a function to request a new block
    // need a function to mark a block as downloaded so we can download a new piece
    // probably a better name for this function
    pub fn mark_piece_as_downloaded(&mut self, piece: usize) -> Option<bool> {
        let piece = self.blocks.get_mut(piece)?;
        for block in piece.iter_mut() {
            if block == &BlockState::Requested {
                self.num_requested -= 1;
            }
            *block = BlockState::Completed;
        }
        Some(true)
    }

    pub fn pending_requests(&self) -> Vec<(u32, u32)> {
        let mut requests = Vec::with_capacity(self.num_requested);
        for (piece_idx, piece) in self.blocks.iter().enumerate() {
            for (block_idx, block) in piece.iter().enumerate() {
                if block == &BlockState::Requested {
                    requests.push((piece_idx as u32, block_idx as u32));
                }
            }
        }
        requests
    }
}
