use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

#[allow(dead_code)]
#[derive(Debug, Deserialize, Clone)]
pub struct Metadata {
    /// URL of the tracker
    // kind of annoying, but need to then convert this into a valid URL to check instead of
    // deserializing directly to URL
    pub announce: String,

    #[serde(rename = "announce list")]
    announce_list: Option<Vec<String>>,

    pub info: TorrInfo,

    #[serde(rename = "creation date")]
    creation_date: Option<u64>,

    encoding: Option<String>,

    comment: Option<String>,

    #[serde(rename = "created by")]
    created_by: Option<String>,

    #[serde(rename = "url-list")]
    url_list: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TorrInfo {
    /// The suggested name of the file being downloaded
    pub name: String,
    /// Number of bytes in each piece the file is split into. Usually a power of two, most commonly
    /// 2^18 = 256K
    #[serde(rename = "piece length")]
    pub piece_length: usize,

    /// string whose length is a multiple of 20. Subdivided into strings of length 20,
    /// each of which is the SHA1 hash of the piece at the corresponding index.
    pub pieces: Hashes,

    #[serde(flatten)]
    pub torr_type: FileTypes,

    pub private: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct Hashes(Vec<[u8; 20]>);
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
    },
    MultiFile {
        files: Vec<File>,
    },
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct File {
    /// The length of the file in bytes
    length: usize,

    // A list of UTF-8 encoded strings corresponding to subdirectory names, the last of which is the actual file name (a zero length list is an error case).
    path: Vec<String>,
}
