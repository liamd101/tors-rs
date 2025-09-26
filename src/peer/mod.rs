mod connection;
pub mod handshake;
pub use connection::handle_peer;
mod message;
mod pieces;

const BLOCK_SIZE: u32 = 1 << 14;

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BlockState {
    UnRequested,
    Requested,
    Completed,
}
