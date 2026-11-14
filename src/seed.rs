use std::collections::BTreeMap;

use crate::chains::conversion as cv;
use anyhow::{bail, Context, Result};
use ipsa::core::smt::Smt;
use soroban_client::xdr::ScVal;

pub const PROBE_BATCH: u64 = 64;

const MAX_KEYS: usize = 200;

const ATTEMPTS: usize = 3;

const PACKET_COMMITMENT: u8 = 0x01;

const PACKET_RECEIPT: u8 = 0x02;

const ACK_COMMITMENT: u8 = 0x03;

#[allow(async_fn_in_trait)]
pub trait RouterReader {
    async fn client_ids(&self) -> Result<Vec<String>>;

    async fn read(&self, keys: &[ScVal]) -> Result<(u32, Vec<Option<ScVal>>)>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Seed {
    pub ledger: u32,
    pub root: [u8; 32],
    pub leaves: BTreeMap<Vec<u8>, Vec<u8>>,
}

pub fn path(client_id: &str, discriminator: u8, sequence: u64) -> Vec<u8> {
    let mut path = client_id.as_bytes().to_vec();
    path.push(discriminator);
    path.extend_from_slice(&sequence.to_be_bytes());

    path
}

pub fn root_of(leaves: &BTreeMap<Vec<u8>, Vec<u8>>) -> [u8; 32] {
    let mut tree = Smt::new();

    for (path, value) in leaves {
        tree.insert(path, value);
    }

    tree.root()
}

pub async fn seed(reader: &impl RouterReader) -> Result<Seed> {
    let clients = reader
        .client_ids()
        .await
        .context("listing the router's clients")?;

    for attempt in 1..=ATTEMPTS {
        let (_, before) = stored_root(reader).await?;
        let leaves = collect(reader, &clients).await?;
        let (ledger, after) = stored_root(reader).await?;
        let root = root_of(&leaves);

        if before == after && root == after {
            return Ok(Seed {
                ledger,
                root,
                leaves,
            });
        }

        tracing::warn!(
            attempt,
            computed = %hex::encode(root),
            stored = %hex::encode(after),
            changed_while_reading = before != after,
            "the leaves read from the router do not rebuild its stored root"
        );
    }

    bail!("the router's storage did not rebuild its stored root after {ATTEMPTS} attempts")
}

async fn stored_root(reader: &impl RouterReader) -> Result<(u32, [u8; 32])> {
    let key = cv::scval_vec(vec![cv::scval_symbol("Root")?])?;
    let (ledger, values) = reader.read(&[key]).await?;

    let root = values
        .into_iter()
        .next()
        .flatten()
        .and_then(|value| cv::scval_as_bytes(&value))
        .context("the router has no stored root")?;

    let root: [u8; 32] = root
        .try_into()
        .map_err(|_| anyhow::anyhow!("the router's stored root is not 32 bytes"))?;

    Ok((ledger, root))
}

async fn collect(
    reader: &impl RouterReader,
    clients: &[String],
) -> Result<BTreeMap<Vec<u8>, Vec<u8>>> {
    let mut leaves = BTreeMap::new();

    for client in clients {
        let last_sent = last_sequence_sent(reader, client).await?;
        let commitments: Vec<Vec<u8>> = (1..=last_sent)
            .map(|sequence| path(client, PACKET_COMMITMENT, sequence))
            .collect();

        read_into(reader, &commitments, &mut leaves).await?;

        let mut start = 1;

        loop {
            let probes: Vec<Vec<u8>> = (start..start + PROBE_BATCH)
                .flat_map(|sequence| {
                    [
                        path(client, PACKET_RECEIPT, sequence),
                        path(client, ACK_COMMITMENT, sequence),
                    ]
                })
                .collect();

            if read_into(reader, &probes, &mut leaves).await? == 0 {
                break;
            }

            start += PROBE_BATCH;
        }
    }

    Ok(leaves)
}

async fn last_sequence_sent(reader: &impl RouterReader, client: &str) -> Result<u64> {
    let key = cv::scval_vec(vec![
        cv::scval_symbol("NextSeqSend")?,
        cv::scval_string(client)?,
    ])?;
    let (_, values) = reader.read(&[key]).await?;

    Ok(values
        .into_iter()
        .next()
        .flatten()
        .and_then(|value| cv::scval_as_u64(&value))
        .unwrap_or(0))
}

async fn read_into(
    reader: &impl RouterReader,
    paths: &[Vec<u8>],
    leaves: &mut BTreeMap<Vec<u8>, Vec<u8>>,
) -> Result<usize> {
    let mut found = 0;

    for chunk in paths.chunks(MAX_KEYS) {
        let keys = chunk
            .iter()
            .map(|path| cv::scval_bytes(path))
            .collect::<Result<Vec<_>>>()?;
        let (_, values) = reader.read(&keys).await?;

        for (path, value) in chunk.iter().zip(values) {
            if let Some(bytes) = value.as_ref().and_then(cv::scval_as_bytes) {
                leaves.insert(path.clone(), bytes);
                found += 1;
            }
        }
    }

    Ok(found)
}
