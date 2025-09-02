use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha1::{Digest, Sha1};
use std::fmt;

#[allow(dead_code)]
#[derive(Debug, Deserialize, Clone)]
pub struct Metadata {
    /// URL of the tracker
    // kind of annoying, but need to then convert this into a valid URL to check instead of
    // deserializing directly to URL
    pub announce: String,

    /// This is an extention to the official specification, offering backwards-compatibility. (list
    /// of lists of strings)
    #[serde(rename = "announce list")]
    announce_list: Option<Vec<String>>,

    /// A dictionary that describes the file(s) of the torrent.
    /// There are two possible forms: one for the case of a 'single-file' torrent with no directory
    /// structure, and one for the case of a 'multi-file' torrent
    pub info: TorrInfo,

    /// The creation time of the torrent, in standard UNIX epoch format (seconds since 1-Jan-1970
    /// 00:00:00 UTC)
    #[serde(rename = "creation date")]
    creation_date: Option<u64>,

    /// The string encoding format used to generate the pieces part of the info dictionary in the
    /// .torrent metafile
    encoding: Option<String>,

    /// Free-form textual comments of the author
    comment: Option<String>,

    /// Name and version of the program used to create the .torrent
    #[serde(rename = "created by")]
    created_by: Option<String>,
}

impl Metadata {
    pub fn num_pieces(&self) -> usize {
        (self.info.torr_type.len() as usize).div_ceil(self.info.piece_length as usize)
    }

    pub fn info_hash(&self) -> [u8; 20] {
        let serialized = serde_bencode::to_bytes(&self.info).expect("could not serialize metadata");
        let mut hasher = Sha1::new();
        hasher.update(serialized);
        hasher.finalize().into()
    }

    pub fn new(file: &String) -> anyhow::Result<Self> {
        let data: &[u8] = &std::fs::read(file)?;
        Ok(serde_bencode::from_bytes(data)?)
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TorrInfo {
    /// Number of bytes in each piece the file is split into. Usually a power of two, most commonly
    /// 2^18 = 256K
    #[serde(rename = "piece length")]
    pub piece_length: u64,

    /// string whose length is a multiple of 20. Subdivided into strings of length 20,
    /// each of which is the SHA1 hash of the piece at the corresponding index.
    pub pieces: Hashes,

    /// This field is an integer. If it is set to "1", the client MUST publish its presence to get
    /// other peers ONLY via the trackers explicitly described in the metainfo file. If this field
    /// is set to "0" or is not present, the client may obtain peer from other means, e.g. PEX peer
    /// exchange, dht. Here, "private" may be read as "no external peer source".
    ///
    /// There is much debate surrounding private trackers.
    /// Request for spec change can be found in BEP_0027
    pub private: Option<usize>,

    /// The name field is present in both the `SingleFile` and `MultiFile` cases.
    ///
    /// In the `SingleFile` case, it respresents the suggested file name of the output file.
    /// In the `MultiFile` case, it represents the suggested directory name that all output files
    /// should be stored in.
    pub name: String,

    /// An enum containing the distinctive attributes of the Info dictionary that correspond to a
    /// `MultiFile` .torrent file and a `SingleFile` .torrent file.
    #[serde(flatten)]
    pub torr_type: FileTypes,

}

#[derive(Debug, Clone)]
pub struct Hashes(pub Vec<[u8; 20]>);
struct HashesVisitor;

impl<'de> Visitor<'de> for HashesVisitor {
    type Value = Hashes;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a byte string whose length is a multiple of 20")
    }

    fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if value.len() % 20 != 0 {
            return Err(E::custom("invalid length of hash string".to_string()));
        }
        Ok(Hashes(
            value
                .chunks_exact(20)
                .map(|sli| sli.try_into().expect("should be length 20"))
                .collect(),
        ))
    }
}

impl<'de> Deserialize<'de> for Hashes {
    fn deserialize<D>(deserializer: D) -> Result<Hashes, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_bytes(HashesVisitor)
    }
}

impl Serialize for Hashes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let bytes = self.0.concat();
        serializer.serialize_bytes(&bytes)
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(untagged)]
pub enum FileTypes {
    SingleFile {
        /// The length of the file in bytes
        length: usize,

        ///  a 32-character hexadecimal string corresponding to the MD5 sum of the file. This is
        ///  not used by BitTorrent at all, but it is included by some programs for greater
        ///  compatibility.
        md5sum: Option<String>,
    },
    MultiFile {
        files: Vec<File>,
    },
}
impl FileTypes {
    pub fn len(&self) -> u64 {
        match self {
            &Self::SingleFile { length, .. } => length as u64,
            Self::MultiFile { files } => files.iter().map(|f| f.length as u64).sum(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct File {
    /// The length of the file in bytes
    length: usize,

    /// A list of UTF-8 encoded strings corresponding to subdirectory names, the last of which is
    /// the actual file name (a zero length list is an error case).
    path: Vec<String>,
}
