use crate::{
    parsing::Metadata,
    peer::Peer,
};

use std::net::SocketAddrV4;

use tokio_util::bytes::{Buf, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use anyhow::{Context, Result};

#[allow(dead_code)]
#[repr(u32)]
#[derive(Debug, Copy, Clone)]
pub(crate) enum Action {
    Connect = 0,
    Announce = 1,
    Scrape = 2,
    Error = 3,
}
impl TryFrom<u32> for Action {
    type Error = anyhow::Error;

    fn try_from(val: u32) -> anyhow::Result<Self> {
        match val {
            0 => Ok(Self::Connect),
            1 => Ok(Self::Announce),
            2 => Ok(Self::Scrape),
            3 => Ok(Action::Error),
            _ => anyhow::bail!("Invalid action received."),
        }
    }
}

#[allow(dead_code)]
#[repr(u32)]
#[derive(Debug, Copy, Clone)]
enum Event {
    None = 0,
    Completed = 1,
    Started = 2,
    Stopped = 3,
}

#[allow(dead_code)]
#[derive(Debug, Copy, Clone)]
pub(crate) enum Request {
    Connect {
        /// 0x41727101980
        protocol_id: u64,
        action: Action,
        transaction_id: u32,
    },
    AnnounceV4 {
        connection_id: u64,
        action: Action,
        transaction_id: u32,
        info_hash: [u8; 20],
        peer_id: [u8; 20],
        downloaded: u64,
        left: u64,
        uploaded: u64,
        event: Event,
        ip_addr: u32,
        key: u32,
        num_want: i32,
        port: u16,
    },
}

impl Request {
    /// Creates a new Request::Connect object with a random transaction ID.
    pub fn connect() -> Self {
        Self::Connect {
            protocol_id: 0x41727101980,
            action: Action::Connect,
            transaction_id: rand::random(),
        }
    }

    pub fn announce(connection_id: u64, metadata: &Metadata, peer_id: [u8; 20], listener_addr: SocketAddrV4) -> anyhow::Result<Self> {
        Ok(Self::AnnounceV4 {
            connection_id,
            action: Action::Announce,
            transaction_id: rand::random(),
            info_hash: metadata.info_hash()?,
            peer_id,
            downloaded: 0, /* TODO */
            left: metadata.info.torr_type.len(),
            uploaded: 0, /* TODO */
            event: Event::Started, /* TODO */
            ip_addr: listener_addr.ip().to_bits(),
            key: 0, /* TODO */
            num_want: -1,
            port: listener_addr.port(),
        })
    }
}

#[derive(Debug, Clone)]
enum Response {
    Connect {
        action: Action,
        transaction_id: u32,
        connection_id: u64,
    },
    AnnounceV4 {
        action: Action,
        transaction_id: u32,
        interval: u32,
        leechers: u32,
        seeders: u32,
        ip_addrs: Vec<SocketAddrV4>, // TODO: this should(?) be a slice
    },
}

struct RequestEncoder {}
impl Encoder<Request> for RequestEncoder {
    type Error = anyhow::Error;

    fn encode(&mut self, item: Request, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            Request::Connect {
                protocol_id,
                action,
                transaction_id,
            } => {
                dst.reserve(16);
                dst.extend_from_slice(&protocol_id.to_be_bytes());
                dst.extend_from_slice(&(action as u32).to_be_bytes());
                dst.extend_from_slice(&transaction_id.to_be_bytes());
            }
            Request::AnnounceV4 {
                connection_id,
                action,
                transaction_id,
                info_hash,
                peer_id,
                downloaded,
                left,
                uploaded,
                event,
                ip_addr,
                key,
                num_want,
                port,
            } => {
                dst.reserve(98);
                dst.extend_from_slice(&connection_id.to_be_bytes());
                dst.extend_from_slice(&(action as u32).to_be_bytes());
                dst.extend_from_slice(&transaction_id.to_be_bytes());
                dst.extend_from_slice(&info_hash);
                dst.extend_from_slice(&peer_id);
                dst.extend_from_slice(&downloaded.to_be_bytes());
                dst.extend_from_slice(&left.to_be_bytes());
                dst.extend_from_slice(&uploaded.to_be_bytes());
                dst.extend_from_slice(&(event as u32).to_be_bytes());
                dst.extend_from_slice(&ip_addr.to_be_bytes());
                dst.extend_from_slice(&key.to_be_bytes());
                dst.extend_from_slice(&num_want.to_be_bytes());
                dst.extend_from_slice(&port.to_be_bytes());
            }
        }

        Ok(())
    }
}

struct ResponseDecoder {}
impl Decoder for ResponseDecoder {
    type Item = Response;
    type Error = anyhow::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            // not enough data to read Action type
            return Ok(None);
        }

        let mut action_bytes = [0u8; 4];
        action_bytes.copy_from_slice(&src[0..4]);
        let action = u32::from_be_bytes(action_bytes);
        let action = Action::try_from(action).context("Unable to parse action")?;
        match action {
            Action::Connect => {
                if src.len() < 4 + 4 + 8 {
                    src.reserve(4 + 4 + 8 - src.len());
                    return Ok(None);
                }
                src.advance(4);

                let mut transaction_id_bytes = [0u8; 4];
                transaction_id_bytes.copy_from_slice(&src[0..4]);
                src.advance(4);
                let transaction_id = u32::from_be_bytes(transaction_id_bytes);
                let mut connection_id_bytes = [0u8; 8];
                connection_id_bytes.copy_from_slice(&src[0..8]);
                src.advance(8);
                let connection_id = u64::from_be_bytes(connection_id_bytes);

                Ok(Some(Response::Connect {
                    action,
                    transaction_id,
                    connection_id,
                }))
            }
            Action::Announce => {
                if src.len() < 20 + 6 {
                    src.reserve(20 + 6 - src.len());
                    return Ok(None);
                }

                let mut transaction_id_bytes = [0u8; 4];
                let mut interval_bytes = [0u8; 4];
                let mut leechers_bytes = [0u8; 4];
                let mut seeders_bytes = [0u8; 4];

                transaction_id_bytes.copy_from_slice(&src[4..8]);
                interval_bytes.copy_from_slice(&src[8..12]);
                leechers_bytes.copy_from_slice(&src[12..16]);
                seeders_bytes.copy_from_slice(&src[16..20]);

                let transaction_id = u32::from_be_bytes(transaction_id_bytes);
                let interval = u32::from_be_bytes(interval_bytes);
                let leechers = u32::from_be_bytes(leechers_bytes);
                let seeders = u32::from_be_bytes(seeders_bytes);

                if src.len() != 20 + ((6 * (leechers + seeders)) as usize) {
                    src.reserve(20 + ((6 * (leechers + seeders)) as usize));
                    return Ok(None);
                }
                src.advance(20);

                let ip_addrs: Vec<SocketAddrV4> = src
                    .chunks_exact(6)
                    .map(|bytes| {
                        let ip_addr =
                            u32::from_be_bytes(bytes[0..4].try_into().expect("Not enough bytes"));
                        let port =
                            u16::from_be_bytes(bytes[4..6].try_into().expect("Not enough bytes"));
                        SocketAddrV4::new(std::net::Ipv4Addr::from_bits(ip_addr), port)
                    })
                    .collect();
                src.advance(ip_addrs.len() * 6);

                Ok(Some(Response::AnnounceV4 {
                    action,
                    transaction_id,
                    interval,
                    leechers,
                    seeders,
                    ip_addrs,
                }))
            }
            _ => unimplemented!(),
        }
    }
}

pub async fn get_response(metadata: &Metadata, listener_addr: SocketAddrV4) -> Result<Vec<Peer>> {
    todo!()
}
