use std::sync::Arc;

use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

use crate::proto::{
    stellar_gateway_query_server::{StellarGatewayQuery, StellarGatewayQueryServer},
    EventsRequest, EventsResponse, GatewayContractEvent, IdentifiedClientState,
    LatestHeightRequest, LatestHeightResponse, QueryAcknowledgementRequest,
    QueryAcknowledgementResponse, QueryClientStateRequest, QueryClientStateResponse,
    QueryClientStatesRequest, QueryClientStatesResponse, QueryConsensusStateRequest,
    QueryConsensusStateResponse, QueryIbcHeaderRequest, QueryIbcHeaderResponse,
    QueryNextSeqRecvRequest, QueryNextSeqRecvResponse, QueryPacketCommitmentRequest,
    QueryPacketCommitmentResponse, QueryPacketReceiptRequest, QueryPacketReceiptResponse,
    QueryStellarClientParamsRequest, QueryStellarClientParamsResponse,
};
use stellar_ibc_core::{
    api_client::ApiClient,
    commitment::{ack_commitment_path, packet_commitment_path, packet_receipt_path},
    ibc::event::event_attributes,
    state::{PathLookup, State},
    types::EventCursor,
};

#[derive(Clone)]
pub struct QueryHandler {
    pub api: ApiClient,
    pub state: Arc<Mutex<State>>,
    pub ibc_contract_id: Option<String>,
    pub source_account: String,
    pub header: Arc<crate::header::HeaderAssembler>,
}

impl QueryHandler {
    pub fn new(
        api: ApiClient,
        state: Arc<Mutex<State>>,
        ibc_contract_id: Option<String>,
        source_account: String,
        header: Arc<crate::header::HeaderAssembler>,
    ) -> Self {
        Self {
            header,
            api,
            state,
            ibc_contract_id,
            source_account,
        }
    }

    pub fn into_server(self) -> StellarGatewayQueryServer<Self> {
        StellarGatewayQueryServer::new(self)
    }

    fn router(&self) -> &str {
        self.ibc_contract_id.as_deref().unwrap_or("")
    }

    fn source(&self) -> &str {
        self.source_account.as_str()
    }
}

#[tonic::async_trait]
impl StellarGatewayQuery for QueryHandler {
    #[tracing::instrument(skip(self, _request), name = "grpc.latest_height")]
    async fn latest_height(
        &self,
        _request: Request<LatestHeightRequest>,
    ) -> Result<Response<LatestHeightResponse>, Status> {
        let latest_sequence: u32 = self.api.get_latest_ledger().await.map_err(|error| {
            tracing::error!(%error, "get_latest_ledger failed");
            Status::internal(format!("get_latest_ledger failed: {error}"))
        })?;

        tracing::debug!(revision_height = latest_sequence, "latest height");
        Ok(Response::new(LatestHeightResponse {
            revision_height: latest_sequence.into(),
            revision_number: 0,
        }))
    }

    #[tracing::instrument(skip(self, request), name = "grpc.query_client_state")]
    async fn query_client_state(
        &self,
        request: Request<QueryClientStateRequest>,
    ) -> Result<Response<QueryClientStateResponse>, Status> {
        let req = request.into_inner();
        tracing::debug!(client_id = %req.client_id, "gRPC QueryClientState");

        let xdr = self
            .api
            .get_client_state_xdr(self.router(), self.source(), &req.client_id)
            .await
            .map_err(|error| Status::internal(format!("get client state: {error}")))?;

        let cs = stellar_ibc_core::ibc::client_state::AnyClientState::from_soroban_xdr(&xdr)
            .map_err(|error| Status::internal(format!("decode soroban client state: {error}")))?;

        let client_state =
            ibc::primitives::proto::Protobuf::<ibc::primitives::proto::Any>::encode_vec(cs);

        Ok(Response::new(QueryClientStateResponse {
            client_state,
            proof: Vec::new(),
            proof_height: req.height,
        }))
    }

    #[tracing::instrument(skip(self, _request), name = "grpc.query_client_states")]
    async fn query_client_states(
        &self,
        _request: Request<QueryClientStatesRequest>,
    ) -> Result<Response<QueryClientStatesResponse>, Status> {
        tracing::debug!("gRPC QueryClientStates");

        let ids = self
            .api
            .list_client_ids(self.router())
            .await
            .map_err(|error| Status::internal(format!("list clients: {error}")))?;

        let mut client_states = Vec::new();
        for client_id in ids {
            let xdr = match self
                .api
                .get_client_state_xdr(self.router(), self.source(), &client_id)
                .await
            {
                Ok(xdr) => xdr,
                Err(error) => {
                    tracing::warn!(%client_id, %error, "skipping client: state fetch failed");
                    continue;
                }
            };
            let cs =
                match stellar_ibc_core::ibc::client_state::AnyClientState::from_soroban_xdr(&xdr) {
                    Ok(cs) => cs,
                    Err(error) => {
                        tracing::warn!(%client_id, %error, "skipping client: decode failed");
                        continue;
                    }
                };
            let client_state =
                ibc::primitives::proto::Protobuf::<ibc::primitives::proto::Any>::encode_vec(cs);
            client_states.push(IdentifiedClientState {
                client_id,
                client_state,
            });
        }

        Ok(Response::new(QueryClientStatesResponse { client_states }))
    }

    #[tracing::instrument(skip(self, request), name = "grpc.query_consensus_state")]
    async fn query_consensus_state(
        &self,
        request: Request<QueryConsensusStateRequest>,
    ) -> Result<Response<QueryConsensusStateResponse>, Status> {
        let req = request.into_inner();
        tracing::debug!(
            client_id = %req.client_id,
            revision_height = req.revision_height,
            "gRPC QueryConsensusState"
        );

        let xdr = self
            .api
            .get_consensus_state_xdr(
                self.router(),
                self.source(),
                &req.client_id,
                req.revision_height,
            )
            .await
            .map_err(|error| Status::not_found(format!("consensus state fetch failed: {error}")))?;

        let cs = stellar_ibc_core::ibc::consensus_state::AnyConsensusState::from_soroban_xdr(&xdr)
            .map_err(|error| Status::internal(format!("consensus state decode failed: {error}")))?;
        let consensus_state =
            ibc::primitives::proto::Protobuf::<ibc::primitives::proto::Any>::encode_vec(cs);

        Ok(Response::new(QueryConsensusStateResponse {
            consensus_state,
            proof: Vec::new(),
            proof_height: req.revision_height,
        }))
    }

    #[tracing::instrument(skip(self, request), name = "grpc.query_packet_commitment")]
    async fn query_packet_commitment(
        &self,
        request: Request<QueryPacketCommitmentRequest>,
    ) -> Result<Response<QueryPacketCommitmentResponse>, Status> {
        let req = request.into_inner();
        tracing::debug!(
            client_id = %req.client_id,
            sequence = req.sequence,
            height = req.height,
            "gRPC QueryPacketCommitment"
        );
        let seq = u32::try_from(req.height).map_err(|_| {
            Status::invalid_argument(format!("height {} does not fit in u32", req.height))
        })?;
        let key = packet_commitment_path(req.client_id.as_bytes(), req.sequence);

        let lookup = self
            .state
            .lock()
            .await
            .proof_for_path(seq, &key)
            .await
            .map_err(|e| {
                tracing::error!(error = %e, sequence = req.sequence, "proof_for_path failed");
                Status::internal(format!("proof_for_path failed: {e}"))
            })?;

        let (commitment, proof) = match lookup {
            PathLookup::Found {
                value_hash,
                proof_bytes,
            } => (value_hash.to_vec(), proof_bytes),
            PathLookup::Absent { proof_bytes } => (Vec::new(), proof_bytes),
        };
        tracing::info!(
            sequence = req.sequence,
            present = !commitment.is_empty(),
            proof_bytes = proof.len(),
            "[gateway] served commitment proof"
        );

        Ok(Response::new(QueryPacketCommitmentResponse {
            commitment,
            proof,
            proof_height: req.height,
        }))
    }

    #[tracing::instrument(skip(self, request), name = "grpc.query_packet_receipt")]
    async fn query_packet_receipt(
        &self,
        request: Request<QueryPacketReceiptRequest>,
    ) -> Result<Response<QueryPacketReceiptResponse>, Status> {
        let req = request.into_inner();
        tracing::debug!(
            client_id = %req.client_id,
            sequence = req.sequence,
            height = req.height,
            "gRPC QueryPacketReceipt"
        );
        let seq = u32::try_from(req.height).map_err(|_| {
            Status::invalid_argument(format!("height {} does not fit in u32", req.height))
        })?;
        let key = packet_receipt_path(req.client_id.as_bytes(), req.sequence);

        let lookup = self
            .state
            .lock()
            .await
            .proof_for_path(seq, &key)
            .await
            .map_err(|e| Status::internal(format!("proof_for_path failed: {e}")))?;

        let (received, proof) = match lookup {
            PathLookup::Found { proof_bytes, .. } => (true, proof_bytes),
            PathLookup::Absent { proof_bytes } => (false, proof_bytes),
        };
        tracing::info!(
            sequence = req.sequence,
            received,
            proof_bytes = proof.len(),
            "[gateway] served receipt proof"
        );

        Ok(Response::new(QueryPacketReceiptResponse {
            received,
            proof,
            proof_height: req.height,
        }))
    }

    #[tracing::instrument(skip(self, request), name = "grpc.query_acknowledgement")]
    async fn query_acknowledgement(
        &self,
        request: Request<QueryAcknowledgementRequest>,
    ) -> Result<Response<QueryAcknowledgementResponse>, Status> {
        let req = request.into_inner();
        tracing::debug!(
            client_id = %req.client_id,
            sequence = req.sequence,
            height = req.height,
            "gRPC QueryAcknowledgement"
        );
        let seq = u32::try_from(req.height).map_err(|_| {
            Status::invalid_argument(format!("height {} does not fit in u32", req.height))
        })?;
        let key = ack_commitment_path(req.client_id.as_bytes(), req.sequence);

        let lookup = self
            .state
            .lock()
            .await
            .proof_for_path(seq, &key)
            .await
            .map_err(|e| Status::internal(format!("proof_for_path failed: {e}")))?;

        let (acknowledgement, proof) = match lookup {
            PathLookup::Found {
                value_hash,
                proof_bytes,
            } => (value_hash.to_vec(), proof_bytes),
            PathLookup::Absent { proof_bytes } => (Vec::new(), proof_bytes),
        };
        tracing::info!(
            sequence = req.sequence,
            ack_bytes = acknowledgement.len(),
            proof_bytes = proof.len(),
            "[gateway] served ack proof"
        );

        Ok(Response::new(QueryAcknowledgementResponse {
            acknowledgement,
            proof,
            proof_height: req.height,
        }))
    }

    #[tracing::instrument(skip(self, _request), name = "grpc.query_next_seq_recv")]
    async fn query_next_seq_recv(
        &self,
        _request: Request<QueryNextSeqRecvRequest>,
    ) -> Result<Response<QueryNextSeqRecvResponse>, Status> {
        tracing::debug!("gRPC QueryNextSeqRecv (unimplemented in v2)");
        Err(Status::unimplemented(
            "QueryNextSeqRecv: nextSequenceSend path was removed in IBC v2",
        ))
    }

    #[tracing::instrument(skip(self, request), name = "grpc.query_ibc_header")]
    async fn query_ibc_header(
        &self,
        request: Request<QueryIbcHeaderRequest>,
    ) -> Result<Response<QueryIbcHeaderResponse>, Status> {
        use prost::Message as _;

        let seq = request.into_inner().height as u32;
        tracing::debug!(sequence = seq, "gRPC QueryIbcHeader");

        for _ in 0..6 {
            let latest = self.api.get_latest_ledger().await.map_err(|error| {
                tracing::error!(%error, "get_latest_ledger failed (waiting for header ledger)");
                Status::internal(format!("get_latest_ledger failed: {error}"))
            })?;
            if latest > seq {
                break;
            }
            tracing::debug!(sequence = seq, latest, "waiting for slot N+1 to close");
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }

        let ledger_meta = match self.api.get_ledger(seq).await {
            Ok(ledger) => ledger.metadata_xdr,
            Err(error) => {
                tracing::warn!(%error, sequence = seq, "no ledger meta; header will bind no root");
                None
            }
        };

        let stellar_header = self
            .header
            .assemble(seq, ledger_meta.as_deref())
            .await
            .map_err(|error| {
                tracing::error!(%error, sequence = seq, "header assembly failed");
                Status::internal(format!("header assembly failed: {error:#}"))
            })?;

        if stellar_header.state_root_proof.is_none() {
            tracing::debug!(sequence = seq, "no router root in this ledger");
        }

        Ok(Response::new(QueryIbcHeaderResponse {
            header: stellar_header.encode_to_vec(),
        }))
    }

    #[tracing::instrument(skip(self, request), name = "grpc.query_stellar_client_params")]
    async fn query_stellar_client_params(
        &self,
        request: Request<QueryStellarClientParamsRequest>,
    ) -> Result<Response<QueryStellarClientParamsResponse>, Status> {
        let slot = request.into_inner().height as u32;

        let quorum_configs = self.header.quorum_configs(slot).await.map_err(|error| {
            tracing::error!(%error, slot, "quorum config fetch failed");
            Status::internal(format!("quorum config fetch failed: {error:#}"))
        })?;

        let router_contract_id = match &self.ibc_contract_id {
            Some(id) => stellar_strkey::Contract::from_string(id)
                .map(|c| c.0.to_vec())
                .unwrap_or_default(),
            None => Vec::new(),
        };

        Ok(Response::new(QueryStellarClientParamsResponse {
            quorum_configs,
            network_id: self.header.network().network_id().to_vec(),
            router_contract_id,
            root_event_topic: crate::header::ROOT_EVENT_TOPIC.to_vec(),
        }))
    }

    #[tracing::instrument(skip(self, request), name = "grpc.events")]
    async fn events(
        &self,
        request: Request<EventsRequest>,
    ) -> Result<Response<EventsResponse>, Status> {
        let contract_id = match self
            .ibc_contract_id
            .as_ref()
            .filter(|s| !s.is_empty())
            .cloned()
        {
            Some(id) => id,
            _ => {
                static WARNED_UNCONFIGURED: std::sync::atomic::AtomicBool =
                    std::sync::atomic::AtomicBool::new(false);
                if !WARNED_UNCONFIGURED.swap(true, std::sync::atomic::Ordering::Relaxed) {
                    tracing::warn!(
                        "Events: ROUTER_CONTRACT_ADDRESS is not configured; returning empty event pages. \
                         Deploy the router (`make -C ci deploy-contracts`) then restart the gateway."
                    );
                }
                let latest = self.api.get_latest_ledger().await.map_err(|error| {
                    tracing::error!(%error, "get_latest_ledger failed (events empty-page fallback)");
                    Status::internal(format!("get_latest_ledger failed: {error}"))
                })?;
                return Ok(Response::new(EventsResponse {
                    latest_ledger: latest.into(),
                    cursor: String::new(),
                    events: Vec::new(),
                }));
            }
        };

        let req = request.into_inner();

        let cursor = if !req.cursor.is_empty() {
            EventCursor::Cursor(req.cursor.clone())
        } else if req.start_ledger > 0 {
            EventCursor::StartLedger(req.start_ledger)
        } else {
            return Err(Status::invalid_argument(
                "events: must set either start_ledger or cursor",
            ));
        };

        let limit = if req.limit == 0 {
            None
        } else {
            Some(req.limit)
        };

        let page = self
            .api
            .get_events(&contract_id, cursor, limit)
            .await
            .map_err(|error| {
                tracing::error!(%error, %contract_id, "get_events failed");
                Status::internal(format!("getEvents failed: {error}"))
            })?;

        if page.events.is_empty() {
            tracing::debug!(
                latest_ledger = page.latest_ledger,
                %contract_id,
                "events poll: none"
            );
        } else {
            tracing::info!(
                events = page.events.len(),
                ledger = page.latest_ledger,
                "[gateway] observed router event(s)"
            );
        }

        let events = page
            .events
            .into_iter()
            .map(|ev| {
                let attributes =
                    event_attributes(&ev.topics_xdr, &ev.value_xdr).unwrap_or_default();

                GatewayContractEvent {
                    id: ev.id,
                    ledger: ev.ledger.into(),
                    ledger_closed_at: ev.ledger_closed_at,
                    contract_id: ev.contract_id,
                    tx_hash: ev.tx_hash,
                    topics_xdr: ev.topics_xdr,
                    value_xdr: ev.value_xdr,
                    attributes,
                }
            })
            .collect();

        Ok(Response::new(EventsResponse {
            latest_ledger: page.latest_ledger.into(),
            cursor: page.cursor,
            events,
        }))
    }
}
