pub mod events;

use std::collections::HashMap;
use std::sync::Arc;

use prost::Message as _;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use soroban_client::xdr::{
    ContractId, Hash, HostFunction, InvokeContractArgs, LedgerHeader, Limits, ReadXdr, ScAddress,
    ScSymbol, ScVal, StringM, WriteXdr,
};
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

use stellar_ibc_core::api_client::{ApiClient, CosmosPacketEvent};
use stellar_ibc_core::commitment::{ack_commitment_path, packet_commitment_path};
use stellar_ibc_core::conversion::{scval_bytes, scval_string, scval_vec_of_bytes};
use stellar_ibc_core::ibc::client_state::AnyClientState;
use stellar_ibc_core::ibc::msg::{self, ProtoAny};
use stellar_ibc_core::ibc::packet::{PacketEvent, SEND_PACKET, WRITE_ACK};
use stellar_ibc_core::ibc::soroban::tendermint_header_to_soroban_xdr;
use stellar_ibc_core::ibc::v2_msgs::{Height, MsgAcknowledgement, MsgRecvPacket};
use stellar_ibc_core::state::{PathLookup, State};

use crate::proto::proofapi::{
    proof_api_service_server::{ProofApiService, ProofApiServiceServer},
    Chain, CreateClientRequest, CreateClientResponse, InfoRequest, InfoResponse, RelayByTxRequest,
    RelayByTxResponse, UpdateClientRequest, UpdateClientResponse,
};
use crate::proto::{
    Height as WireHeight, QuorumConfig, StellarClientState, StellarConsensusState, StellarHeader,
};

const IBC_VERSION: &str = "2";
const COSMOS_SEND_PACKET: &str = "send_packet";
const COSMOS_WRITE_ACK: &str = "write_acknowledgement";
const DEFAULT_TRUSTING_PERIOD: u64 = 14 * 24 * 60 * 60;

#[derive(Deserialize)]
struct ParamQuorumConfig {
    quorum_set_xdr: String,
    #[serde(default)]
    valid_from: u64,
}

pub struct RelayHandler {
    api: ApiClient,
    state: Arc<Mutex<State>>,
    header: Arc<crate::header::HeaderAssembler>,
    stellar_chain_id: String,
    cosmos_chain_id: String,
    ibc_contract_id: String,
    event_lookback_ledgers: u32,
    cosmos_signer: String,
}

impl RelayHandler {
    pub fn new(
        api: ApiClient,
        state: Arc<Mutex<State>>,
        header: Arc<crate::header::HeaderAssembler>,
        stellar_chain_id: String,
        cosmos_chain_id: String,
        ibc_contract_id: String,
        event_lookback_ledgers: u32,
        cosmos_signer: String,
    ) -> Self {
        Self {
            api,
            state,
            header,
            stellar_chain_id,
            cosmos_chain_id,
            ibc_contract_id,
            event_lookback_ledgers,
            cosmos_signer,
        }
    }

    pub fn into_server(self) -> ProofApiServiceServer<Self> {
        ProofApiServiceServer::new(self)
    }

    fn known_pair(&self, src: &str, dst: &str) -> bool {
        let stellar = self.stellar_chain_id.as_str();
        let cosmos = self.cosmos_chain_id.as_str();

        (src == stellar && dst == cosmos) || (src == cosmos && dst == stellar)
    }

    fn chain(&self, chain_id: &str) -> Chain {
        let ibc_contract = if chain_id == self.stellar_chain_id {
            self.ibc_contract_id.clone()
        } else {
            String::new()
        };

        Chain {
            chain_id: chain_id.to_string(),
            ibc_version: IBC_VERSION.to_string(),
            ibc_contract,
        }
    }

    async fn proof_at(&self, ledger: u32, key: &[u8]) -> Result<Vec<u8>, Status> {
        let lookup = self
            .state
            .lock()
            .await
            .proof_for_path(ledger, key)
            .await
            .map_err(|error| {
                tracing::error!(%error, ledger, "proof_for_path failed");
                Status::internal(format!("proof_for_path failed: {error}"))
            })?;

        match lookup {
            PathLookup::Found { proof_bytes, .. } => Ok(proof_bytes),
            PathLookup::Absent { .. } => Err(Status::failed_precondition(format!(
                "no value committed at the requested path in ledger {ledger}"
            ))),
        }
    }

    async fn stellar_messages(
        &self,
        packets: &[PacketEvent],
        ledger: u32,
    ) -> Result<(Vec<MsgRecvPacket>, Vec<MsgAcknowledgement>), Status> {
        let height = Height {
            revision_number: 0,
            revision_height: u64::from(ledger),
        };

        let mut recv = Vec::new();
        let mut ack = Vec::new();

        for event in packets {
            match event.kind.as_str() {
                SEND_PACKET => {
                    let key = packet_commitment_path(
                        event.packet.source_client.as_bytes(),
                        event.packet.sequence,
                    );
                    let mut message = msg::recv_msg(event, &self.cosmos_signer);
                    message.proof_commitment = self.proof_at(ledger, &key).await?;
                    message.proof_height = Some(height.clone());
                    recv.push(message);
                }
                WRITE_ACK => {
                    let key = ack_commitment_path(
                        event.packet.dest_client.as_bytes(),
                        event.packet.sequence,
                    );
                    let mut message = msg::ack_msg(event, &self.cosmos_signer);
                    message.proof_acked = self.proof_at(ledger, &key).await?;
                    message.proof_height = Some(height.clone());
                    ack.push(message);
                }
                _ => {}
            }
        }

        Ok((recv, ack))
    }

    fn router_contract_id(&self) -> Result<[u8; 32], Status> {
        stellar_strkey::Contract::from_string(&self.ibc_contract_id)
            .map(|c| c.0)
            .map_err(|error| {
                Status::failed_precondition(format!(
                    "ROUTER_CONTRACT_ADDRESS is not a contract strkey: {error}"
                ))
            })
    }

    async fn consensus_state_at(
        &self,
        slot: u32,
        header: &StellarHeader,
    ) -> Result<StellarConsensusState, Status> {
        let decoded = LedgerHeader::from_xdr(&header.ledger_header_xdr, Limits::none())
            .map_err(|e| Status::internal(format!("ledger header decode: {e}")))?;

        let root = self
            .state
            .lock()
            .await
            .root_at(slot)
            .await
            .map_err(|error| Status::internal(format!("root_at failed: {error}")))?;

        Ok(StellarConsensusState {
            timestamp: decoded.scp_value.close_time.0,
            ledger_hash: Sha256::digest(&header.ledger_header_xdr).to_vec(),
            root: root.to_vec(),
        })
    }

    async fn update_message(
        &self,
        client_id: &str,
        ledger: u32,
    ) -> Result<(ProtoAny, bool), Status> {
        if client_id.is_empty() {
            return Err(Status::invalid_argument(
                "dst_client_id is required to build the client update",
            ));
        }

        let header = self.header.assemble_at(ledger).await.map_err(|error| {
            tracing::error!(%error, ledger, "header assembly failed");
            Status::internal(format!("header assembly failed: {error:#}"))
        })?;

        let carries_root = header.state_root_proof.is_some();

        Ok((
            msg::update_client_msg(
                client_id,
                msg::wasm_client_message(header.encode_to_vec()),
                &self.cosmos_signer,
            ),
            carries_root,
        ))
    }

    async fn update_for(&self, client_id: &str, ledger: u32) -> Result<ProtoAny, Status> {
        let (update, carries_root) = self.update_message(client_id, ledger).await?;

        if !carries_root {
            return Err(Status::failed_precondition(format!(
                "ledger {ledger} carries no router state root; proofs at this height cannot be verified"
            )));
        }

        Ok(update)
    }

    async fn relay_from_cosmos(
        &self,
        req: &RelayByTxRequest,
    ) -> Result<Response<RelayByTxResponse>, Status> {
        let mut found = Vec::new();
        for hash in &req.source_tx_ids {
            let events = self
                .api
                .get_cosmos_packet_events(&hex::encode(hash))
                .await
                .map_err(|error| {
                    tracing::error!(%error, "fetching cosmos packet events failed");
                    Status::internal(format!("fetching cosmos packet events failed: {error}"))
                })?;
            found.extend(events);
        }

        let selected: Vec<CosmosPacketEvent> = found
            .into_iter()
            .filter(|event| match event.kind.as_str() {
                COSMOS_SEND_PACKET => {
                    req.src_packet_sequences.is_empty()
                        || req.src_packet_sequences.contains(&event.packet.sequence)
                }
                COSMOS_WRITE_ACK => {
                    req.dst_packet_sequences.is_empty()
                        || req.dst_packet_sequences.contains(&event.packet.sequence)
                }
                _ => false,
            })
            .collect();

        if selected.is_empty() {
            return Err(Status::not_found(
                "no packet events found in the requested transactions",
            ));
        }

        let target = selected
            .iter()
            .map(|event| event.height)
            .max()
            .expect("selected is non-empty");

        let mut functions = vec![self.tendermint_update(&req.dst_client_id, target).await?];
        functions.extend(self.cosmos_messages(&selected).await?);

        tracing::debug!(
            src_chain = %req.src_chain,
            dst_chain = %req.dst_chain,
            source_txs = req.source_tx_ids.len(),
            packets = selected.len(),
            functions = functions.len(),
            "relay_by_tx built soroban host functions"
        );

        Ok(Response::new(RelayByTxResponse {
            tx: encode_host_functions(functions)?,
            address: self.ibc_contract_id.clone(),
        }))
    }

    async fn tendermint_update(
        &self,
        client_id: &str,
        height: u64,
    ) -> Result<HostFunction, Status> {
        if client_id.is_empty() {
            return Err(Status::invalid_argument(
                "dst_client_id is required to update the tendermint client",
            ));
        }

        let trusted = self
            .api
            .get_client_state_xdr(&self.ibc_contract_id, &self.cosmos_signer, client_id)
            .await
            .map_err(|error| {
                Status::failed_precondition(format!(
                    "client {client_id} is not registered on stellar: {error}"
                ))
            })
            .and_then(|xdr| {
                AnyClientState::from_soroban_xdr(&xdr)
                    .map_err(|e| Status::internal(format!("decode client state: {e}")))
            })?
            .latest_height();

        if trusted >= height {
            return Err(Status::failed_precondition(format!(
                "client {client_id} already trusts height {trusted}, at or beyond the packet \
                 height {height}; nothing to update"
            )));
        }

        let header = self
            .api
            .get_cosmos_header(height, trusted)
            .await
            .map_err(|error| {
                tracing::error!(%error, height, trusted, "cosmos header failed");
                Status::internal(format!("cosmos header failed: {error}"))
            })?;

        let header_xdr = tendermint_header_to_soroban_xdr(&header)
            .map_err(|e| Status::internal(format!("tendermint header to soroban xdr: {e}")))?;

        host_function(
            &self.ibc_contract_id,
            "update_client",
            vec![
                scval_string(client_id).map_err(|e| Status::internal(format!("client id: {e}")))?,
                scval_bytes(&header_xdr).map_err(|e| Status::internal(format!("header: {e}")))?,
            ],
        )
    }

    async fn cosmos_messages(
        &self,
        events: &[CosmosPacketEvent],
    ) -> Result<Vec<HostFunction>, Status> {
        let mut functions = Vec::new();

        for event in events {
            let (key, method) = match event.kind.as_str() {
                COSMOS_SEND_PACKET => (
                    packet_commitment_path(
                        event.packet.source_client.as_bytes(),
                        event.packet.sequence,
                    ),
                    "recv_packet",
                ),
                COSMOS_WRITE_ACK => (
                    ack_commitment_path(event.packet.dest_client.as_bytes(), event.packet.sequence),
                    "acknowledge_packet",
                ),
                _ => continue,
            };

            let (value, proof) = self
                .api
                .get_cosmos_proof(&key, event.height)
                .await
                .map_err(|error| {
                    tracing::error!(%error, height = event.height, "cosmos proof failed");
                    Status::internal(format!("cosmos proof failed: {error}"))
                })?;

            if value.is_empty() {
                return Err(Status::failed_precondition(format!(
                    "cosmos has no value committed at the requested path in block {}",
                    event.height
                )));
            }

            let packet = msg::packet_to_scval(&event.packet)
                .map_err(|e| Status::internal(format!("packet to ScVal: {e}")))?;

            let mut args = vec![packet];
            if method == "acknowledge_packet" {
                args.push(
                    scval_vec_of_bytes(&event.acknowledgements)
                        .map_err(|e| Status::internal(format!("acknowledgements: {e}")))?,
                );
            }
            args.push(scval_bytes(&proof).map_err(|e| Status::internal(format!("proof: {e}")))?);
            args.push(ScVal::U64(event.height));

            functions.push(host_function(&self.ibc_contract_id, method, args)?);
        }

        Ok(functions)
    }

    async fn refreshable_ledger(&self) -> Result<u32, Status> {
        let latest = self
            .api
            .get_latest_ledger()
            .await
            .map_err(|error| Status::internal(format!("get_latest_ledger failed: {error}")))?;

        if latest < 2 {
            return Err(Status::failed_precondition(format!(
                "ledger {latest} has no closed successor to bind it; nothing to update to"
            )));
        }

        Ok(latest - 1)
    }
}

fn host_function(router: &str, method: &str, args: Vec<ScVal>) -> Result<HostFunction, Status> {
    let contract = stellar_strkey::Contract::from_string(router).map_err(|error| {
        Status::failed_precondition(format!(
            "ROUTER_CONTRACT_ADDRESS is not a contract strkey: {error}"
        ))
    })?;

    Ok(HostFunction::InvokeContract(InvokeContractArgs {
        contract_address: ScAddress::Contract(ContractId(Hash(contract.0))),
        function_name: ScSymbol(
            method
                .parse::<StringM<32>>()
                .map_err(|e| Status::internal(format!("method name {method}: {e}")))?,
        ),
        args: args
            .try_into()
            .map_err(|_| Status::internal("too many arguments for one invocation"))?,
    }))
}

fn encode_host_functions(functions: Vec<HostFunction>) -> Result<Vec<u8>, Status> {
    let count = u32::try_from(functions.len())
        .map_err(|_| Status::internal("too many host functions for one response"))?;

    let mut out = count.to_be_bytes().to_vec();
    for function in functions {
        out.extend_from_slice(
            &function
                .to_xdr(Limits::none())
                .map_err(|e| Status::internal(format!("HostFunction encode: {e}")))?,
        );
    }
    Ok(out)
}

fn quorum_configs_from(parameters: &HashMap<String, String>) -> Result<Vec<QuorumConfig>, Status> {
    let raw = parameters.get("quorum_configs").ok_or_else(|| {
        Status::invalid_argument(
            "parameters.quorum_configs is required; the trust root is an operator decision and \
             the gateway is untrusted transport",
        )
    })?;

    let parsed: Vec<ParamQuorumConfig> = serde_json::from_str(raw)
        .map_err(|e| Status::invalid_argument(format!("parameters.quorum_configs: {e}")))?;

    if parsed.is_empty() {
        return Err(Status::invalid_argument(
            "parameters.quorum_configs is empty; refusing to create a client with no trust root",
        ));
    }

    let mut configs = Vec::with_capacity(parsed.len());
    let mut seen: Vec<u64> = Vec::with_capacity(parsed.len());
    for entry in parsed {
        if seen.contains(&entry.valid_from) {
            return Err(Status::invalid_argument(format!(
                "two quorum configurations claim valid_from {}; the trust root would be chosen \
                 by list order",
                entry.valid_from
            )));
        }
        seen.push(entry.valid_from);

        let quorum_set_xdr = hex::decode(entry.quorum_set_xdr.trim()).map_err(|e| {
            Status::invalid_argument(format!("parameters.quorum_configs quorum_set_xdr: {e}"))
        })?;

        configs.push(QuorumConfig {
            quorum_set_xdr,
            valid_from: entry.valid_from,
        });
    }

    Ok(configs)
}

fn parameter_u64(
    parameters: &HashMap<String, String>,
    key: &str,
    default: u64,
) -> Result<u64, Status> {
    match parameters.get(key) {
        None => Ok(default),
        Some(raw) => raw
            .trim()
            .parse()
            .map_err(|e| Status::invalid_argument(format!("parameters.{key}: {e}"))),
    }
}

fn filter_sequences(events: Vec<PacketEvent>, src: &[u64], dst: &[u64]) -> Vec<PacketEvent> {
    events
        .into_iter()
        .filter(|event| {
            let selected = match event.kind.as_str() {
                SEND_PACKET => src,
                WRITE_ACK => dst,
                _ => return false,
            };
            selected.is_empty() || selected.contains(&event.packet.sequence)
        })
        .collect()
}

fn proof_ledger(events: &[PacketEvent]) -> Option<u32> {
    events.iter().map(|event| event.ledger).max()
}

#[tonic::async_trait]
impl ProofApiService for RelayHandler {
    async fn info(&self, request: Request<InfoRequest>) -> Result<Response<InfoResponse>, Status> {
        let req = request.into_inner();

        if !self.known_pair(&req.src_chain, &req.dst_chain) {
            return Err(Status::not_found(format!(
                "no relay route for ({}, {}); known chains are {} and {}",
                req.src_chain, req.dst_chain, self.stellar_chain_id, self.cosmos_chain_id
            )));
        }

        Ok(Response::new(InfoResponse {
            source_chain: Some(self.chain(&req.src_chain)),
            target_chain: Some(self.chain(&req.dst_chain)),
            metadata: HashMap::new(),
        }))
    }

    async fn relay_by_tx(
        &self,
        request: Request<RelayByTxRequest>,
    ) -> Result<Response<RelayByTxResponse>, Status> {
        let req = request.into_inner();

        if !self.known_pair(&req.src_chain, &req.dst_chain) {
            return Err(Status::not_found(format!(
                "no relay route for ({}, {})",
                req.src_chain, req.dst_chain
            )));
        }

        if self.ibc_contract_id.is_empty() {
            return Err(Status::failed_precondition(
                "ROUTER_CONTRACT_ADDRESS is not configured",
            ));
        }

        if req.src_chain == self.cosmos_chain_id {
            return self.relay_from_cosmos(&req).await;
        }

        if self.ibc_contract_id.is_empty() {
            return Err(Status::failed_precondition(
                "ROUTER_CONTRACT_ADDRESS is not configured",
            ));
        }

        let tx_hashes: Vec<String> = req.source_tx_ids.iter().map(hex::encode).collect();

        let found = events::packet_events_in_txs(
            &self.api,
            &self.ibc_contract_id,
            &tx_hashes,
            self.event_lookback_ledgers,
        )
        .await
        .map_err(|error| {
            tracing::error!(%error, "fetching stellar packet events failed");
            Status::internal(format!("fetching stellar packet events failed: {error}"))
        })?;

        let packets = filter_sequences(found, &req.src_packet_sequences, &req.dst_packet_sequences);

        let Some(ledger) = proof_ledger(&packets) else {
            return Err(Status::not_found(
                "no packet events found in the requested transactions",
            ));
        };

        let (recv, ack) = self.stellar_messages(&packets, ledger).await?;
        let update = self.update_for(&req.dst_client_id, ledger).await?;

        tracing::debug!(
            src_chain = %req.src_chain,
            dst_chain = %req.dst_chain,
            source_txs = tx_hashes.len(),
            packets = packets.len(),
            proof_ledger = ledger,
            recv = recv.len(),
            ack = ack.len(),
            "relay_by_tx built cosmos messages"
        );

        let tx = msg::encode_tx_body(Some(update), recv, ack);

        Ok(Response::new(RelayByTxResponse {
            tx,
            address: self.ibc_contract_id.clone(),
        }))
    }

    async fn create_client(
        &self,
        request: Request<CreateClientRequest>,
    ) -> Result<Response<CreateClientResponse>, Status> {
        let req = request.into_inner();

        if req.src_chain != self.stellar_chain_id || req.dst_chain != self.cosmos_chain_id {
            return Err(Status::unimplemented(format!(
                "create_client only builds the stellar client on {}, got ({}, {})",
                self.cosmos_chain_id, req.src_chain, req.dst_chain
            )));
        }

        let quorum_configs = quorum_configs_from(&req.parameters)?;
        let checksum = req
            .parameters
            .get("wasm_checksum")
            .ok_or_else(|| Status::invalid_argument("parameters.wasm_checksum is required"))?;
        let checksum = hex::decode(checksum.trim())
            .map_err(|e| Status::invalid_argument(format!("parameters.wasm_checksum: {e}")))?;

        let latest = self
            .api
            .get_latest_ledger()
            .await
            .map_err(|error| Status::internal(format!("get_latest_ledger failed: {error}")))?;
        let slot = u32::try_from(parameter_u64(
            &req.parameters,
            "height",
            u64::from(latest.saturating_sub(1)),
        )?)
        .map_err(|_| Status::invalid_argument("parameters.height does not fit in u32"))?;
        let max_consensus_age = parameter_u64(
            &req.parameters,
            "max_consensus_age",
            DEFAULT_TRUSTING_PERIOD,
        )?;

        let header = self.header.assemble_at(slot).await.map_err(|error| {
            tracing::error!(%error, slot, "header assembly failed");
            Status::internal(format!("header assembly failed: {error:#}"))
        })?;

        let consensus_state = self.consensus_state_at(slot, &header).await?;

        let client_state = StellarClientState {
            chain_id: self.stellar_chain_id.clone(),
            latest_height: Some(WireHeight {
                revision_number: 0,
                revision_height: u64::from(slot),
            }),
            frozen_height: None,
            quorum_configs,
            proof_specs: Vec::new(),
            network_id: self.header.network().network_id().to_vec(),
            max_consensus_age,
            router_contract_id: self.router_contract_id()?.to_vec(),
            root_event_topic: crate::header::ROOT_EVENT_TOPIC.to_vec(),
        };

        tracing::info!(
            slot,
            quorum_configs = client_state.quorum_configs.len(),
            "create_client built the stellar client state"
        );

        let tx = msg::encode_msgs(vec![msg::create_client_msg(
            msg::wasm_client_state(client_state.encode_to_vec(), checksum, u64::from(slot)),
            msg::wasm_consensus_state(consensus_state.encode_to_vec()),
            &self.cosmos_signer,
        )]);

        Ok(Response::new(CreateClientResponse {
            tx,
            address: String::new(),
        }))
    }

    async fn update_client(
        &self,
        request: Request<UpdateClientRequest>,
    ) -> Result<Response<UpdateClientResponse>, Status> {
        let req = request.into_inner();

        if req.src_chain != self.stellar_chain_id || req.dst_chain != self.cosmos_chain_id {
            return Err(Status::unimplemented(format!(
                "update_client only refreshes the stellar client on {}, got ({}, {})",
                self.cosmos_chain_id, req.src_chain, req.dst_chain
            )));
        }

        let ledger = self.refreshable_ledger().await?;
        let (update, carries_root) = self.update_message(&req.dst_client_id, ledger).await?;

        tracing::info!(
            client_id = %req.dst_client_id,
            ledger,
            carries_root,
            "update_client built a header"
        );

        Ok(Response::new(UpdateClientResponse {
            tx: msg::encode_msgs(vec![update]),
            address: String::new(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stellar_ibc_core::ibc::packet::Packet;

    fn event(kind: &str, sequence: u64, ledger: u32) -> PacketEvent {
        PacketEvent {
            kind: kind.to_string(),
            tx_hash: String::new(),
            ledger,
            acknowledgements: Vec::new(),
            packet: Packet {
                sequence,
                source_client: "08-wasm-0".to_string(),
                dest_client: "07-tendermint-0".to_string(),
                timeout_timestamp: 0,
                payloads: Vec::new(),
            },
        }
    }

    #[test]
    fn empty_filters_keep_every_packet_event() {
        let found = vec![event(SEND_PACKET, 1, 10), event(WRITE_ACK, 2, 11)];

        let kept = filter_sequences(found, &[], &[]);

        assert_eq!(kept.len(), 2);
    }

    #[test]
    fn send_packets_filter_on_source_sequences() {
        let found = vec![event(SEND_PACKET, 1, 10), event(SEND_PACKET, 2, 11)];

        let kept = filter_sequences(found, &[2], &[]);

        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].packet.sequence, 2);
    }

    #[test]
    fn write_acks_filter_on_destination_sequences() {
        let found = vec![event(WRITE_ACK, 1, 10), event(WRITE_ACK, 2, 11)];

        let kept = filter_sequences(found, &[], &[1]);

        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].packet.sequence, 1);
    }

    #[test]
    fn a_send_sequence_filter_does_not_drop_write_acks() {
        let found = vec![event(SEND_PACKET, 1, 10), event(WRITE_ACK, 9, 11)];

        let kept = filter_sequences(found, &[1], &[]);

        assert_eq!(kept.len(), 2);
    }

    #[test]
    fn unrelated_event_kinds_are_dropped() {
        let found = vec![event("recv_packet", 1, 10)];

        assert!(filter_sequences(found, &[], &[]).is_empty());
    }

    #[test]
    fn proof_ledger_is_the_highest_event_ledger() {
        let found = vec![event(SEND_PACKET, 1, 10), event(SEND_PACKET, 2, 44)];

        assert_eq!(proof_ledger(&found), Some(44));
    }

    #[test]
    fn proof_ledger_is_absent_without_events() {
        assert_eq!(proof_ledger(&[]), None);
    }
}
