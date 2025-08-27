pub mod message;
pub mod parsing;
pub mod peer;
pub mod tracker;


#[derive(Clone, Debug)]
/// Enum representing some messages that get sent between peer threads and parent threads.
pub enum ThreadUpdate {
    /// `Downloaded(piece_idx, block_idx)`
    /// Indicates that the given piece/block pair have been downloaded successfully.
    /// Sent from peer threads after successfully reading a block from a peer.
    /// Upon receiving this message, the parent thread checks if the any new pieces have been
    /// successfully downloaded.
    Downloaded(usize, usize),
    /// Indicates that the inner piece has been successfully downloaded, as confirmed by the parent
    /// thread computing the downloaded piece hash and comparing against the piece hash that is
    /// present in the `.torrent` file.
    Completed(usize),
}
