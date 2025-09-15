use rand::seq::IndexedRandom;

use super::{BLOCK_SIZE, BlockState};
use crate::message::BitField;

pub(crate) struct PieceTracker {
    num_requested: usize,
    pipeline_len: usize,
    blocks: Vec<Vec<BlockState>>,
    requested_piece: Option<usize>,
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
            requested_piece: None,
        }
    }

    /// Updates `self` so that all blocks and pieces that are set true in the bitfield are set to `Completed`
    /// in `self.blocks`
    pub fn update(&mut self, bitfield: &BitField) {
        for piece in bitfield.set_bits() {
            let piece = self.blocks.get_mut(piece).unwrap();
            piece.iter_mut().map(|b| *b = BlockState::Completed).count();
        }
    }

    /// Returns a Vec of `(piece_idx, block_idx)` pairs for every requested (but not downloaded)
    /// block
    pub fn pending_requests(&self) -> Vec<(u32, u32)> {
        let mut out = Vec::with_capacity(self.num_requested);
        for (piece_idx, piece) in self.blocks.iter().enumerate() {
            for (block_idx, &block) in piece.iter().enumerate() {
                if block == BlockState::Requested {
                    out.push((piece_idx as u32, block_idx as u32))
                }
            }
        }
        out
    }

    /// Marks a block as Completed and decrements the number of requested blocks
    pub fn finish_request(&mut self, piece: usize, block: usize) -> Option<bool> {
        if self.num_requested == 0 {
            return None;
        }
        let piece = self.blocks.get_mut(piece)?;
        let state = piece.get_mut(block)?;
        match state {
            BlockState::Completed => None,
            _ => {
                *state = BlockState::Completed;
                self.num_requested -= 1;
                Some(true)
            }
        }
    }

    pub fn request_new(&mut self, pieces: Vec<usize>) -> Option<(u32, u32)> {
        if self.num_requested >= self.pipeline_len {
            return None;
        }
        // want to create a Vec<(usize, usize)> where first index is in pieces and select randomly from there
        let blocks: Vec<Vec<BlockState>> = pieces
            .iter()
            .map(|&piece_idx| self.blocks[piece_idx].clone())
            .collect();
        let blocks: Vec<Vec<usize>> = blocks
            .iter()
            .map(|piece| {
                piece
                    .iter()
                    .enumerate()
                    .filter_map(|(block_idx, block_state)| match block_state {
                        BlockState::UnRequested => Some(block_idx),
                        _ => None,
                    })
                    .collect()
            })
            .collect();
        let blocks: Vec<(u32, u32)> = blocks
            .iter()
            .enumerate()
            .flat_map(|(piece_idx, piece)| {
                piece
                    .iter()
                    .map(move |&block_idx| (piece_idx as u32, block_idx as u32))
            })
            .collect();
        let out = blocks.choose(&mut rand::rng()).copied()?;
        self.blocks[out.0 as usize][out.1 as usize] = BlockState::Requested;
        self.num_requested += 1;
        Some(out)
    }

    pub fn set(&mut self, piece: usize, block: usize, state: BlockState) -> Option<BlockState> {
        let piece = self.blocks.get_mut(piece)?;
        let old_state = piece.get_mut(block)?;
        let out = *old_state;
        *old_state = state;
        Some(out)
    }

    pub fn complete_piece(&mut self, piece: u32) -> Option<bool> {
        let piece = self.blocks.get_mut(piece as usize)?;
        for block in piece.iter_mut() {
            if block == &BlockState::Requested {
                self.num_requested -= 1;
            }
            *block = BlockState::Completed;
        }
        Some(true)
    }
}

impl PieceTracker {
    /// Requests a new (piece,block) combo.
    /// If a piece has already been requested, but not completed, then we will first try to request
    /// a blcok from that piece.
    /// Otherwise, we will pick a new random piece and add it to a stack of pieces to request.
    ///
    /// check if we can request another piece (i.e. not at pipeline limit)
    /// if we can, we want to check if we should request from a specific piece.
    /// if we should request from a specific piece, try to find a block to request, otherwise,
    /// request a random block and update the requested piece
    ///
    /// if we shouldn't request a specific piece, request a random piece and update requested piece
    pub fn request(&mut self, pieces: Vec<usize>) -> Option<(usize, usize)> {
        if self.num_requested >= self.pipeline_len {
            return None;
        }

        match self.requested_piece {
            Some(piece) => {
                if self.all_requested(piece)? {
                    self.requested_piece = None;
                } else {
                }
            }
            None => {}
        }

        // self.requested_piece = Some(piece /* TODO */);
        todo!()
    }

    fn get_unrequested_block(&self, pieces: Vec<usize>) -> Option<(usize, usize)> {
        let blocks: Vec<Vec<BlockState>> = pieces
            .iter()
            .map(|&piece_idx| self.blocks[piece_idx].clone())
            .collect();

        let blocks: Vec<Vec<usize>> = blocks
            .iter()
            .map(|piece| {
                piece
                    .iter()
                    .enumerate()
                    .filter_map(|(block_idx, block_state)| match block_state {
                        BlockState::UnRequested => Some(block_idx),
                        _ => None,
                    })
                    .collect()
            })
            .collect();

        let blocks: Vec<(usize, usize)> = blocks
            .iter()
            .enumerate()
            .flat_map(|(piece_idx, piece)| {
                piece.iter().map(move |&block_idx| (piece_idx, block_idx))
            })
            .collect();

        blocks.choose(&mut rand::rng()).copied()
    }

    /// Marks the provided block as downloaded.
    /// If the piece has been completely downloaded, then we remove it from the queue of requested
    /// pieces
    ///
    /// Returns Some(()) on success. None on failure
    pub fn download(&mut self, piece: usize, block: usize) -> Option<()> {
        let piece_vec = self.blocks.get_mut(piece)?;
        let block_state = piece_vec.get_mut(block)?;
        *block_state = BlockState::Completed;
        if self.all_requested(piece)? {
            self.requested_piece = None;
        }
        Some(())
    }

    /// Returns a Some(true) if all blocks of a piece have been requested/downloaded
    ///           Some(false) if there is a block that has not been requested yet
    ///           None if the piece does not exist (although this should never be the case)
    fn all_requested(&self, piece: usize) -> Option<bool> {
        let piece = self.blocks.get(piece)?;
        Some(piece.iter().any(|b| b == &BlockState::UnRequested))
    }
}
