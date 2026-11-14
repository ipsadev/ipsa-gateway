pub mod events;

use std::collections::HashMap;
use std::sync::Arc;

use prost_gateway::Message as _;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use soroban_client::xdr::{
    ContractId, Hash, HostFunction, InvokeContractArgs, LedgerHeader, Limits, ReadXdr, ScAddress,
    ScSymbol, ScVal, StringM, WriteXdr,
};
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

use crate::api_client::{ApiClient, CosmosPacketEvent};
use crate::availability::{cosmos_header_status, header_assembly_status};
use crate::chains::client_state::AnyClientState;
use crate::chains::conversion::{scval_bytes, scval_string, scval_u64, scval_vec_of_bytes};
use crate::chains::msg::{self, ProtoAny};
use crate::chains::packet::{PacketEvent, SEND_PACKET, WRITE_ACK};
use crate::chains::soroban::tendermint_header_to_soroban_xdr;
use crate::chains::tendermint::{tendermint_genesis, TendermintBootstrap};
use crate::chains::v2_msgs::{Height, MsgAcknowledgement, MsgRecvPacket, MsgTimeout};
use crate::state::{PathLookup, State};
use ipsa::core::commitment::{
    acknowledgement_commitment_path, packet_commitment_path, packet_receipt_path,
};

use ipsa::gateway::proofapi::{
    proof_api_service_server::{ProofApiService, ProofApiServiceServer},
    Chain, CreateClientRequest, CreateClientResponse, InfoRequest, InfoResponse, RelayByTxRequest,
    RelayByTxResponse, UpdateClientRequest, UpdateClientResponse,
};
use ipsa::gateway::stellar::{
    Height as WireHeight, QuorumConfig, StellarClientState, StellarConsensusState, StellarHeader,
};

const TENDERMINT_CLIENT_TYPE: &str = "07-tendermint";
const DEFAULT_TRUSTING_PERIOD_SECS: u64 = 14 * 24 * 60 * 60;
const DEFAULT_UNBONDING_PERIOD_SECS: u64 = 21 * 24 * 60 * 60;
const DEFAULT_CLOCK_DRIFT_SECS: u64 = 40;

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
    source_account: String,
}

impl RelayHandler {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        api: ApiClient,
        state: Arc<Mutex<State>>,
        header: Arc<crate::header::HeaderAssembler>,
        stellar_chain_id: String,
        cosmos_chain_id: String,
        ibc_contract_id: String,
        event_lookback_ledgers: u32,
        cosmos_signer: String,
        source_account: String,
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
            source_account,
        }
    }

    pub fn source_account(&self) -> &str {
        self.source_account.as_str()
    }

    pub fn into_server(self) -> ProofApiServiceServer<Self> {
        ProofApiServiceServer::new(self)
    }

    async fn tendermint_genesis_for_stellar(
        &self,
        parameters: &HashMap<String, String>,
    ) -> Result<Response<CreateClientResponse>, Status> {
        if self.ibc_contract_id.is_empty() {
            return Err(Status::failed_precondition(
                "ROUTER_CONTRACT_ADDRESS is not configured",
            ));
        }

        let height = match parameter_u64(parameters, "height", 0)? {
            0 => self
                .api
                .get_cosmos_latest_height()
                .await
                .map_err(|error| Status::internal(format!("cosmos latest height: {error}")))?,
            height => height,
        };

        let header = self
            .api
            .get_cosmos_header(height, 0)
            .await
            .map_err(|error| {
                tracing::error!(%error, height, "cosmos header failed");
                cosmos_header_status(&error)
            })?;

        let params = TendermintBootstrap {
            chain_id: self.cosmos_chain_id.clone(),
            height,
            trusting_period_secs: parameter_u64(
                parameters,
                "trusting_period_secs",
                DEFAULT_TRUSTING_PERIOD_SECS,
            )?,
            unbonding_period_secs: parameter_u64(
                parameters,
                "unbonding_period_secs",
                DEFAULT_UNBONDING_PERIOD_SECS,
            )?,
            max_clock_drift_secs: parameter_u64(
                parameters,
                "max_clock_drift_secs",
                DEFAULT_CLOCK_DRIFT_SECS,
            )?,
        };

        let genesis = tendermint_genesis(&header, &params).map_err(|error| {
            Status::internal(format!("building the tendermint genesis: {error:#}"))
        })?;

        let client_type = match parameters.get("client_type").map(|t| t.trim()) {
            Some(client_type) if !client_type.is_empty() => client_type.to_string(),
            _ => TENDERMINT_CLIENT_TYPE.to_string(),
        };

        if self.source_account().is_empty() {
            return Err(Status::failed_precondition(
                "STELLAR_SOURCE_ACCOUNT is not configured, so no transaction can be prepared",
            ));
        }

        let tx = self
            .api
            .build_unsigned_tx(
                &self.ibc_contract_id,
                self.source_account(),
                "create_client",
                vec![
                    scval_string(&client_type)
                        .map_err(|error| Status::internal(format!("client_type scval: {error}")))?,
                    scval_bytes(&genesis.client_state_xdr).map_err(|error| {
                        Status::internal(format!("client_state scval: {error}"))
                    })?,
                    scval_bytes(&genesis.consensus_state_xdr).map_err(|error| {
                        Status::internal(format!("consensus_state scval: {error}"))
                    })?,
                    scval_u64(genesis.height),
                ],
            )
            .await
            .map_err(|error| {
                tracing::error!(%error, "build_unsigned_tx(create_client) failed");
                Status::internal(format!("build_unsigned_tx(create_client): {error}"))
            })?;

        tracing::info!(
            %client_type,
            chain_id = %genesis.chain_id,
            height = genesis.height,
            "create_client prepared the tendermint client transaction for stellar"
        );

        Ok(Response::new(CreateClientResponse {
            tx,
            address: self.ibc_contract_id.clone(),
        }))
    }

    async fn tendermint_refresh_for_stellar(
        &self,
        client_id: &str,
    ) -> Result<Response<UpdateClientResponse>, Status> {
        if self.ibc_contract_id.is_empty() {
            return Err(Status::failed_precondition(
                "ROUTER_CONTRACT_ADDRESS is not configured",
            ));
        }

        let target = self
            .api
            .get_cosmos_latest_height()
            .await
            .map_err(|error| Status::internal(format!("cosmos latest height: {error}")))?;

        let trusted = self.trusted_height(client_id).await?;

        if trusted >= target {
            return Err(Status::failed_precondition(format!(
                "client {client_id} already trusts height {trusted}, at or beyond the cosmos head \
                 {target}; nothing to update"
            )));
        }

        let function = self.tendermint_update(client_id, trusted, target).await?;

        tracing::info!(%client_id, trusted, target, "update_client built a tendermint header");

        Ok(Response::new(UpdateClientResponse {
            tx: encode_host_functions(vec![function])?,
            address: self.ibc_contract_id.clone(),
        }))
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

    async fn absence_proof_at(&self, ledger: u32, key: &[u8]) -> Result<Vec<u8>, Status> {
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
            PathLookup::Absent { proof_bytes } => Ok(proof_bytes),
            PathLookup::Found { .. } => Err(Status::failed_precondition(format!(
                "the packet was received in ledger {ledger} and cannot be timed out"
            ))),
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

    async fn expired_cosmos_packets(
        &self,
        req: &RelayByTxRequest,
    ) -> Result<Vec<CosmosPacketEvent>, Status> {
        if req.timeout_tx_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut found = Vec::new();

        for hash in &req.timeout_tx_ids {
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

        Ok(found
            .into_iter()
            .filter(|event| event.kind == COSMOS_SEND_PACKET)
            .filter(|event| {
                req.dst_packet_sequences.is_empty()
                    || req.dst_packet_sequences.contains(&event.packet.sequence)
            })
            .collect())
    }

    async fn cosmos_timeout_messages(
        &self,
        packets: &[CosmosPacketEvent],
        ledger: u32,
    ) -> Result<Vec<MsgTimeout>, Status> {
        let height = Height {
            revision_number: 0,
            revision_height: u64::from(ledger),
        };

        let mut timeouts = Vec::new();

        for event in packets {
            let key =
                packet_receipt_path(event.packet.dest_client.as_bytes(), event.packet.sequence);

            timeouts.push(MsgTimeout {
                packet: Some(event.packet.clone()),
                proof_unreceived: self.absence_proof_at(ledger, &key).await?,
                proof_height: Some(height.clone()),
                signer: self.cosmos_signer.clone(),
            });
        }

        Ok(timeouts)
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
                    let key = acknowledgement_commitment_path(
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
            header_assembly_status(&error)
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

        let expired = self.expired_stellar_packets(req).await?;

        if selected.is_empty() && expired.is_empty() {
            return Err(Status::not_found(
                "no packet events found in the requested transactions",
            ));
        }

        let mut target = selected
            .iter()
            .map(|event| event.height + 1)
            .max()
            .unwrap_or(0);

        if !expired.is_empty() {
            let latest = self.api.get_cosmos_latest_height().await.map_err(|error| {
                tracing::error!(%error, "get_cosmos_latest_height failed");
                Status::internal(format!("get_cosmos_latest_height failed: {error}"))
            })?;
            target = target.max(latest);
        }

        let trusted = self.trusted_height(&req.dst_client_id).await?;

        let mut functions = Vec::new();

        let proof_height = if trusted >= target {
            trusted
        } else {
            functions.push(
                self.tendermint_update(&req.dst_client_id, trusted, target)
                    .await?,
            );
            target
        };

        functions.extend(self.cosmos_messages(&selected, proof_height).await?);
        functions.extend(self.timeout_messages(&expired, proof_height).await?);

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

    async fn trusted_height(&self, client_id: &str) -> Result<u64, Status> {
        if client_id.is_empty() {
            return Err(Status::invalid_argument(
                "dst_client_id is required to update the tendermint client",
            ));
        }

        Ok(self
            .api
            .get_client_state_xdr(&self.ibc_contract_id, self.source_account(), client_id)
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
            .latest_height())
    }

    async fn cosmos_proof_at(
        &self,
        key: &[u8],
        proof_height: u64,
    ) -> Result<(Vec<u8>, Vec<u8>), Status> {
        let read_at = proof_height.saturating_sub(1);

        self.api
            .get_cosmos_proof(key, read_at)
            .await
            .map_err(|error| {
                tracing::error!(%error, height = read_at, "cosmos proof failed");
                Status::internal(format!("cosmos proof failed: {error}"))
            })
    }

    async fn expired_stellar_packets(
        &self,
        req: &RelayByTxRequest,
    ) -> Result<Vec<PacketEvent>, Status> {
        if req.timeout_tx_ids.is_empty() {
            return Ok(Vec::new());
        }

        let tx_hashes: Vec<String> = req.timeout_tx_ids.iter().map(hex::encode).collect();

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

        Ok(found
            .into_iter()
            .filter(|event| event.kind == SEND_PACKET)
            .filter(|event| {
                req.dst_packet_sequences.is_empty()
                    || req.dst_packet_sequences.contains(&event.packet.sequence)
            })
            .collect())
    }

    async fn timeout_messages(
        &self,
        events: &[PacketEvent],
        proof_height: u64,
    ) -> Result<Vec<HostFunction>, Status> {
        let mut functions = Vec::new();

        for event in events {
            let key =
                packet_receipt_path(event.packet.dest_client.as_bytes(), event.packet.sequence);

            let (receipt, proof) = self.cosmos_proof_at(&key, proof_height).await?;

            if !receipt.is_empty() {
                return Err(Status::failed_precondition(format!(
                    "packet {} on {} was received on cosmos and cannot be timed out",
                    event.packet.sequence, event.packet.dest_client
                )));
            }

            let packet = msg::packet_to_scval(&msg::to_proto_packet(&event.packet))
                .map_err(|e| Status::internal(format!("packet to ScVal: {e}")))?;

            functions.push(host_function(
                &self.ibc_contract_id,
                "timeout_packet",
                vec![
                    packet,
                    scval_bytes(&proof).map_err(|e| Status::internal(format!("proof: {e}")))?,
                    ScVal::U64(proof_height),
                ],
            )?);
        }

        Ok(functions)
    }

    async fn tendermint_update(
        &self,
        client_id: &str,
        trusted: u64,
        height: u64,
    ) -> Result<HostFunction, Status> {
        let header = self
            .api
            .get_cosmos_header(height, trusted)
            .await
            .map_err(|error| {
                tracing::error!(%error, height, trusted, "cosmos header failed");
                cosmos_header_status(&error)
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
        proof_height: u64,
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
                    acknowledgement_commitment_path(
                        event.packet.dest_client.as_bytes(),
                        event.packet.sequence,
                    ),
                    "acknowledge_packet",
                ),
                _ => continue,
            };

            let (value, proof) = self.cosmos_proof_at(&key, proof_height).await?;

            if value.is_empty() {
                return Err(Status::failed_precondition(format!(
                    "cosmos has no value committed at the requested path in block {}",
                    proof_height.saturating_sub(1)
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
            args.push(ScVal::U64(proof_height));

            functions.push(host_function(&self.ibc_contract_id, method, args)?);
        }

        Ok(functions)
    }

    async fn servable_ledger(&self) -> Result<u32, Status> {
        let published = self
            .api
            .get_archive_latest_ledger()
            .await
            .map_err(|error| {
                Status::internal(format!("get_archive_latest_ledger failed: {error}"))
            })?;

        if published < 2 {
            return Err(Status::failed_precondition(format!(
                "the history archive has published only through ledger {published}; there is no \
                 ledger whose SCP evidence can be fetched yet"
            )));
        }

        Ok(published - 1)
    }

    async fn refreshable_ledger(&self) -> Result<u32, Status> {
        self.servable_ledger().await
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

pub fn filter_sequences(events: Vec<PacketEvent>, src: &[u64], dst: &[u64]) -> Vec<PacketEvent> {
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

pub fn proof_ledger(events: &[PacketEvent]) -> Option<u32> {
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

    #[tracing::instrument(skip(self, request), name = "grpc.relay_by_tx", fields(src_chain = %request.get_ref().src_chain, dst_chain = %request.get_ref().dst_chain))]
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
        let expired = self.expired_cosmos_packets(&req).await?;

        let ledger = match proof_ledger(&packets) {
            Some(ledger) => ledger,
            None if !expired.is_empty() => self.servable_ledger().await?,
            None => {
                return Err(Status::not_found(
                    "no packet events found in the requested transactions",
                ))
            }
        };

        let ((recv, ack), timeout, update) = tokio::try_join!(
            self.stellar_messages(&packets, ledger),
            self.cosmos_timeout_messages(&expired, ledger),
            self.update_for(&req.dst_client_id, ledger),
        )?;

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

        let tx = msg::encode_tx_body_with_timeouts(Some(update), recv, ack, timeout);

        Ok(Response::new(RelayByTxResponse {
            tx,
            address: self.ibc_contract_id.clone(),
        }))
    }

    #[tracing::instrument(skip(self, request), name = "grpc.create_client", fields(src_chain = %request.get_ref().src_chain, dst_chain = %request.get_ref().dst_chain))]
    async fn create_client(
        &self,
        request: Request<CreateClientRequest>,
    ) -> Result<Response<CreateClientResponse>, Status> {
        let req = request.into_inner();

        if req.src_chain == self.cosmos_chain_id && req.dst_chain == self.stellar_chain_id {
            return self.tendermint_genesis_for_stellar(&req.parameters).await;
        }

        if req.src_chain != self.stellar_chain_id || req.dst_chain != self.cosmos_chain_id {
            return Err(Status::unimplemented(format!(
                "create_client builds the stellar client on {} or the tendermint client on {}, \
                 got ({}, {})",
                self.cosmos_chain_id, self.stellar_chain_id, req.src_chain, req.dst_chain
            )));
        }

        let quorum_configs = quorum_configs_from(&req.parameters)?;
        let checksum = req
            .parameters
            .get("wasm_checksum")
            .ok_or_else(|| Status::invalid_argument("parameters.wasm_checksum is required"))?;
        let checksum = hex::decode(checksum.trim())
            .map_err(|e| Status::invalid_argument(format!("parameters.wasm_checksum: {e}")))?;

        let servable = self.servable_ledger().await?;
        let slot = u32::try_from(parameter_u64(
            &req.parameters,
            "height",
            u64::from(servable),
        )?)
        .map_err(|_| Status::invalid_argument("parameters.height does not fit in u32"))?;
        let max_consensus_age = parameter_u64(
            &req.parameters,
            "max_consensus_age",
            DEFAULT_TRUSTING_PERIOD,
        )?;

        let header = self.header.assemble_at(slot).await.map_err(|error| {
            tracing::error!(%error, slot, "header assembly failed");
            header_assembly_status(&error)
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

    #[tracing::instrument(skip(self, request), name = "grpc.update_client", fields(src_chain = %request.get_ref().src_chain, dst_chain = %request.get_ref().dst_chain, dst_client_id = %request.get_ref().dst_client_id))]
    async fn update_client(
        &self,
        request: Request<UpdateClientRequest>,
    ) -> Result<Response<UpdateClientResponse>, Status> {
        let req = request.into_inner();

        if req.src_chain == self.cosmos_chain_id && req.dst_chain == self.stellar_chain_id {
            return self
                .tendermint_refresh_for_stellar(&req.dst_client_id)
                .await;
        }

        if req.src_chain != self.stellar_chain_id || req.dst_chain != self.cosmos_chain_id {
            return Err(Status::unimplemented(format!(
                "update_client refreshes the stellar client on {} or the tendermint client on {}, \
                 got ({}, {})",
                self.cosmos_chain_id, self.stellar_chain_id, req.src_chain, req.dst_chain
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
