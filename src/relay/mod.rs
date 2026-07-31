use std::collections::HashMap;

use tonic::{Request, Response, Status};

use crate::proto::proofapi::{
    proof_api_service_server::{ProofApiService, ProofApiServiceServer},
    Chain, CreateClientRequest, CreateClientResponse, InfoRequest, InfoResponse, RelayByTxRequest,
    RelayByTxResponse, UpdateClientRequest, UpdateClientResponse,
};

const IBC_VERSION: &str = "2";

pub struct RelayHandler {
    stellar_chain_id: String,
    cosmos_chain_id: String,
    ibc_contract_id: String,
}

impl RelayHandler {
    pub fn new(stellar_chain_id: String, cosmos_chain_id: String, ibc_contract_id: String) -> Self {
        Self {
            stellar_chain_id,
            cosmos_chain_id,
            ibc_contract_id,
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

        tracing::debug!(
            src_chain = %req.src_chain,
            dst_chain = %req.dst_chain,
            source_txs = req.source_tx_ids.len(),
            timeout_txs = req.timeout_tx_ids.len(),
            "relay_by_tx"
        );

        Err(Status::unimplemented("relay_by_tx"))
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
