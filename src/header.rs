use anyhow::{anyhow, bail, Result};
use sha2::{Digest, Sha256};
use soroban_client::xdr::{
    ContractEvent, ContractEventBody, InvokeHostFunctionSuccessPreImage, LedgerCloseMeta, Limits,
    ReadXdr, ScVal, TransactionMeta, TransactionResultPair, WriteXdr,
};
use stellar_ibc_core::api_client::ApiClient;

use crate::proto::{QuorumConfig, StateRootProof, StellarHeader};

pub const ROOT_EVENT_TOPIC: &[u8] = b"ibc_root";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Network {
    Mainnet,
    Testnet,
}

impl Network {
    pub fn from_chain_id(chain_id: &str) -> Self {
        let id = chain_id.to_ascii_lowercase();
        if id.contains("mainnet") || id.contains("public") || id.contains("pubnet") {
            Self::Mainnet
        } else {
            Self::Testnet
        }
    }

    pub fn passphrase(self) -> &'static str {
        match self {
            Self::Mainnet => "Public Global Stellar Network ; September 2015",
            Self::Testnet => "Test SDF Network ; September 2015",
        }
    }

    pub fn network_id(self) -> [u8; 32] {
        Sha256::digest(self.passphrase().as_bytes()).into()
    }
}

pub struct HeaderAssembler {
    api: ApiClient,
    network: Network,
    router_contract_id: Option<[u8; 32]>,
    root_event_topic: Vec<u8>,
}

impl HeaderAssembler {
    pub fn new(
        api: ApiClient,
        network: Network,
        router_contract_id: Option<[u8; 32]>,
        root_event_topic: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            api,
            network,
            router_contract_id,
            root_event_topic: root_event_topic.into(),
        }
    }

    pub fn network(&self) -> Network {
        self.network
    }

    pub async fn quorum_configs(&self, slot: u32) -> Result<Vec<QuorumConfig>> {
        let scp = self.api.get_archive_scp(slot).await?;
        Ok(scp
            .quorum_sets_xdr
            .into_iter()
            .map(|quorum_set_xdr| QuorumConfig {
                quorum_set_xdr,
                valid_from: 0,
            })
            .collect())
    }

    pub async fn assemble(&self, slot: u32, ledger_meta: Option<&[u8]>) -> Result<StellarHeader> {
        let scp = self.api.get_archive_scp(slot).await?;
        let header = self.api.get_archive_ledger_header(slot).await?;

        let next = self.api.get_archive_scp(slot + 1).await?;
        let next_tx_set_xdr = self.api.get_archive_tx_set(slot + 1).await?;

        let state_root_proof = match (ledger_meta, self.router_contract_id) {
            (Some(meta), Some(_router)) => self.state_root_proof(meta)?,
            _ => None,
        };

        Ok(StellarHeader {
            slot_index: slot as u64,
            ledger_header_xdr: header.header_xdr,
            scp_envelopes: scp.envelopes_xdr,
            quorum_sets_xdr: scp.quorum_sets_xdr,
            next_scp_envelopes: next.envelopes_xdr,
            next_tx_set_xdr,
            state_root_proof,
        })
    }

    pub async fn assemble_at(&self, slot: u32) -> Result<StellarHeader> {
        for _ in 0..6 {
            let latest = self.api.get_latest_ledger().await?;
            if latest > slot {
                break;
            }
            tracing::debug!(slot, latest, "waiting for slot N+1 to close");
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }

        let ledger_meta = match self.api.get_ledger(slot).await {
            Ok(ledger) => ledger.metadata_xdr,
            Err(error) => {
                tracing::warn!(%error, slot, "no ledger meta; header will bind no root");
                None
            }
        };

        let header = self.assemble(slot, ledger_meta.as_deref()).await?;
        if header.state_root_proof.is_none() {
            tracing::debug!(slot, "no router root in this ledger");
        }

        Ok(header)
    }

    fn state_root_proof(&self, ledger_meta: &[u8]) -> Result<Option<StateRootProof>> {
        let meta = LedgerCloseMeta::from_xdr(ledger_meta, Limits::none())
            .map_err(|e| anyhow!("LedgerCloseMeta decode: {e}"))?;

        let processing = tx_processing(&meta);
        if processing.is_empty() {
            return Ok(None);
        }

        let mut result_pairs = Vec::with_capacity(processing.len());
        for (pair, _) in &processing {
            result_pairs.push(
                pair.to_xdr(Limits::none())
                    .map_err(|e| anyhow!("TransactionResultPair encode: {e}"))?,
            );
        }

        let Some((index, preimage)) = self.find_router_invocation(&processing)? else {
            return Ok(None);
        };

        let success_preimage_xdr = preimage
            .to_xdr(Limits::none())
            .map_err(|e| anyhow!("InvokeHostFunctionSuccessPreImage encode: {e}"))?;

        let expected = invocation_success_hash(&result_pairs[index])?;
        let computed: [u8; 32] = Sha256::digest(&success_preimage_xdr).into();
        if expected != computed {
            bail!(
                "reconstructed success preimage hashes to {} but the result committed to {} \
                 — the meta and result disagree, so the proof would be rejected on chain",
                hex::encode(computed),
                hex::encode(expected)
            );
        }

        Ok(Some(StateRootProof {
            result_pairs,
            result_index: index as u32,
            success_preimage_xdr,
        }))
    }

    #[allow(clippy::type_complexity)]
    fn find_router_invocation(
        &self,
        processing: &[(TransactionResultPair, Option<(ScVal, Vec<ContractEvent>)>)],
    ) -> Result<Option<(usize, InvokeHostFunctionSuccessPreImage)>> {
        for (index, (_, soroban)) in processing.iter().enumerate() {
            let Some((return_value, events)) = soroban else {
                continue;
            };
            if !events.iter().any(|e| self.is_router_root_event(e)) {
                continue;
            }
            return Ok(Some((
                index,
                InvokeHostFunctionSuccessPreImage {
                    return_value: return_value.clone(),
                    events: events
                        .clone()
                        .try_into()
                        .map_err(|_| anyhow!("too many contract events for one invocation"))?,
                },
            )));
        }
        Ok(None)
    }

    fn is_router_root_event(&self, event: &ContractEvent) -> bool {
        let Some(id) = event.contract_id.as_ref() else {
            return false;
        };
        if id.0 .0 != *self.router_contract_id.as_ref().expect("checked by caller") {
            return false;
        }
        let ContractEventBody::V0(body) = &event.body;
        body.topics.iter().any(|t| match t {
            ScVal::Symbol(sym) => sym.0.as_slice() == self.root_event_topic.as_slice(),
            _ => false,
        })
    }
}

#[allow(clippy::type_complexity)]
fn tx_processing(
    meta: &LedgerCloseMeta,
) -> Vec<(TransactionResultPair, Option<(ScVal, Vec<ContractEvent>)>)> {
    let mut out = Vec::new();
    match meta {
        LedgerCloseMeta::V0(v) => {
            for p in v.tx_processing.iter() {
                out.push((p.result.clone(), soroban_parts(&p.tx_apply_processing)));
            }
        }
        LedgerCloseMeta::V1(v) => {
            for p in v.tx_processing.iter() {
                out.push((p.result.clone(), soroban_parts(&p.tx_apply_processing)));
            }
        }
        LedgerCloseMeta::V2(v) => {
            for p in v.tx_processing.iter() {
                out.push((p.result.clone(), soroban_parts(&p.tx_apply_processing)));
            }
        }
    }
    out
}

fn soroban_parts(meta: &TransactionMeta) -> Option<(ScVal, Vec<ContractEvent>)> {
    match meta {
        TransactionMeta::V3(v) => {
            let s = v.soroban_meta.as_ref()?;
            Some((s.return_value.clone(), s.events.to_vec()))
        }
        TransactionMeta::V4(v) => {
            let soroban = v.soroban_meta.as_ref()?;
            let return_value = soroban.return_value.clone()?;
            let events = v
                .operations
                .first()
                .map(|op| op.events.to_vec())
                .unwrap_or_default();
            Some((return_value, events))
        }
        _ => None,
    }
}

fn invocation_success_hash(pair_xdr: &[u8]) -> Result<[u8; 32]> {
    let pair = TransactionResultPair::from_xdr(pair_xdr, Limits::none())
        .map_err(|e| anyhow!("TransactionResultPair decode: {e}"))?;

    use soroban_client::xdr::{
        InvokeHostFunctionResult, OperationResult, OperationResultTr, TransactionResultResult,
    };

    let TransactionResultResult::TxSuccess(ops) = &pair.result.result else {
        bail!("the referenced transaction did not succeed");
    };
    let [OperationResult::OpInner(OperationResultTr::InvokeHostFunction(
        InvokeHostFunctionResult::Success(hash),
    ))] = ops.as_slice()
    else {
        bail!("the referenced result is not a single successful host-function invocation");
    };

    Ok(hash.0)
}
