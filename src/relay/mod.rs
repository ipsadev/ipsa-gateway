pub mod events;

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

use stellar_ibc_core::api_client::ApiClient;
use stellar_ibc_core::commitment::{ack_commitment_path, packet_commitment_path};
use stellar_ibc_core::ibc::msg;
use stellar_ibc_core::ibc::packet::{PacketEvent, SEND_PACKET, WRITE_ACK};
use stellar_ibc_core::ibc::v2_msgs::{Height, MsgAcknowledgement, MsgRecvPacket};
use stellar_ibc_core::state::{PathLookup, State};

use crate::proto::proofapi::{
    proof_api_service_server::{ProofApiService, ProofApiServiceServer},
    Chain, CreateClientRequest, CreateClientResponse, InfoRequest, InfoResponse, RelayByTxRequest,
    RelayByTxResponse, UpdateClientRequest, UpdateClientResponse,
};

const IBC_VERSION: &str = "2";

pub struct RelayHandler {
    api: ApiClient,
    state: Arc<Mutex<State>>,
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
        stellar_chain_id: String,
        cosmos_chain_id: String,
        ibc_contract_id: String,
        event_lookback_ledgers: u32,
        cosmos_signer: String,
    ) -> Self {
        Self {
            api,
            state,
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

        let packets = filter_sequences(found, &req.src_packet_sequences, &req.dst_packet_sequences);

        let Some(ledger) = proof_ledger(&packets) else {
            return Err(Status::not_found(
                "no packet events found in the requested transactions",
            ));
        };

        let (recv, ack) = self.stellar_messages(&packets, ledger).await?;

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

        let tx = msg::encode_tx_body(None, recv, ack);

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
