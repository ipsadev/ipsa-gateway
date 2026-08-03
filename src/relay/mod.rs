pub mod events;

use std::collections::HashMap;

use tonic::{Request, Response, Status};

use stellar_ibc_core::api_client::ApiClient;
use stellar_ibc_core::ibc::cosmos_tx;
use stellar_ibc_core::ibc::packet::PacketEvent;

use crate::proto::proofapi::{
    proof_api_service_server::{ProofApiService, ProofApiServiceServer},
    Chain, CreateClientRequest, CreateClientResponse, InfoRequest, InfoResponse, RelayByTxRequest,
    RelayByTxResponse, UpdateClientRequest, UpdateClientResponse,
};

const IBC_VERSION: &str = "2";

pub struct RelayHandler {
    api: ApiClient,
    stellar_chain_id: String,
    cosmos_chain_id: String,
    ibc_contract_id: String,
    event_lookback_ledgers: u32,
    cosmos_signer: String,
}

impl RelayHandler {
    pub fn new(
        api: ApiClient,
        stellar_chain_id: String,
        cosmos_chain_id: String,
        ibc_contract_id: String,
        event_lookback_ledgers: u32,
        cosmos_signer: String,
    ) -> Self {
        Self {
            api,
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
}

fn filter_sequences(events: Vec<PacketEvent>, sequences: &[u64]) -> Vec<PacketEvent> {
    if sequences.is_empty() {
        return events;
    }

    events
        .into_iter()
        .filter(|event| sequences.contains(&event.packet.sequence))
        .collect()
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

        if req.src_chain != self.stellar_chain_id {
            return Err(Status::unimplemented(format!(
                "relay_by_tx from {} is not implemented yet",
                req.src_chain
            )));
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

        let packets = filter_sequences(found, &req.src_packet_sequences);

        let mut recv = cosmos_tx::recv_msgs(&packets, &self.cosmos_signer);
        let mut ack = cosmos_tx::ack_msgs(&packets, &self.cosmos_signer);

        cosmos_tx::inject_mock_proofs(&mut recv, &mut ack);

        tracing::debug!(
            src_chain = %req.src_chain,
            dst_chain = %req.dst_chain,
            source_txs = tx_hashes.len(),
            packets = packets.len(),
            recv = recv.len(),
            ack = ack.len(),
            "relay_by_tx built cosmos messages"
        );

        let tx = cosmos_tx::encode_tx_body(None, recv, ack);

        Ok(Response::new(RelayByTxResponse {
            tx,
            address: self.ibc_contract_id.clone(),
        }))
    }

    async fn create_client(
        &self,
        _request: Request<CreateClientRequest>,
    ) -> Result<Response<CreateClientResponse>, Status> {
        Err(Status::unimplemented("create_client"))
    }

    async fn update_client(
        &self,
        _request: Request<UpdateClientRequest>,
    ) -> Result<Response<UpdateClientResponse>, Status> {
        Err(Status::unimplemented("update_client"))
    }
}
