use ibc::primitives::proto::{Any, Protobuf};
use prost::Message as _;
use serde_json::json;
use soroban_client::xdr::{
    InvokeHostFunctionSuccessPreImage, LedgerHeader, Limits, ReadXdr, ScVal, WriteXdr,
};
use tonic::Code;

use interstellar_gateway::proto::{
    EventsRequest, LatestHeightRequest, QueryAcknowledgementRequest, QueryClientStateRequest,
    QueryClientStatesRequest, QueryConsensusStateRequest, QueryIbcHeaderRequest,
    QueryNextSeqRecvRequest, QueryPacketCommitmentRequest, QueryPacketReceiptRequest,
};
use stellar_ibc_core::commitment::{
    ack_commitment_path, packet_commitment_path, packet_receipt_path,
};
use stellar_ibc_core::conversion::{
    scval_bytes, scval_height, scval_string, scval_struct, scval_symbol, scval_to_xdr, scval_vec,
};
use stellar_ibc_core::ibc::client_state::AnyClientState;

use super::mock::{
    ledger_meta_with_invocations, ledger_meta_with_write, root_event, router_event_json,
    GatewayTest,
};

pub fn client_state_xdr() -> Vec<u8> {
    let trust_level = scval_struct(vec![
        ("numerator", ScVal::U32(1)),
        ("denominator", ScVal::U32(3)),
    ])
    .unwrap();
    let client_state = scval_struct(vec![
        ("chain_id", scval_string("testchain-1").unwrap()),
        ("trust_level", trust_level),
        ("trusting_period_secs", ScVal::U64(1_209_600)),
        ("unbonding_period_secs", ScVal::U64(1_814_400)),
        ("max_clock_drift_secs", ScVal::U64(40)),
        ("latest_height", scval_height(0, 10).unwrap()),
        ("frozen_height", scval_height(0, 0).unwrap()),
    ])
    .unwrap();
    scval_to_xdr(&client_state).unwrap()
}

#[tokio::test]
async fn latest_height_is_bounded_by_the_archive_not_the_live_tip() {
    let t = GatewayTest::start(None).await;
    t.with_data(|d| {
        d.latest_ledger = 9_000;
        d.archive_latest_ledger = 778;
    });

    let resp = t
        .query()
        .latest_height(LatestHeightRequest {})
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.revision_height, 777);
    assert_eq!(resp.revision_number, 0);
}

#[tokio::test]
async fn latest_height_stays_one_short_of_the_published_tip() {
    let t = GatewayTest::start(None).await;
    t.with_data(|d| d.archive_latest_ledger = 4_267_008);

    let resp = t
        .query()
        .latest_height(LatestHeightRequest {})
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.revision_height, 4_267_007);
}

#[tokio::test]
async fn query_next_seq_recv_is_unimplemented_in_v2() {
    let t = GatewayTest::start(None).await;
    let err = t
        .query()
        .query_next_seq_recv(QueryNextSeqRecvRequest {
            client_id: "07-tendermint-0".into(),
        })
        .await
        .unwrap_err();

    assert_eq!(err.code(), Code::Unimplemented);
}

#[tokio::test]
async fn events_are_decoded_into_attributes() {
    let t = GatewayTest::start(Some([1u8; 32])).await;

    let topic = hex::encode(scval_to_xdr(&scval_symbol("create_client").unwrap()).unwrap());
    let value = hex::encode(
        scval_to_xdr(
            &scval_struct(vec![(
                "client_id",
                scval_string("07-tendermint-0").unwrap(),
            )])
            .unwrap(),
        )
        .unwrap(),
    );

    t.with_data(|d| {
        d.latest_ledger = 5;
        d.events.push(json!({
            "id": "e1",
            "ledger": 5,
            "ledger_closed_at": "2026-01-01T00:00:00Z",
            "contract_id": "Crouter",
            "tx_hash": "deadbeef",
            "topics_xdr": [topic],
            "value_xdr": value,
        }));
    });

    let resp = t
        .query()
        .events(EventsRequest {
            start_ledger: 1,
            cursor: String::new(),
            limit: 0,
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.events.len(), 1);
    let attributes = &resp.events[0].attributes;
    assert!(
        attributes.contains("type=create_client"),
        "got: {attributes}"
    );
    assert!(
        attributes.contains("client_id=07-tendermint-0"),
        "got: {attributes}"
    );
}

#[tokio::test]
async fn query_client_state_converts_soroban_to_protobuf() {
    let t = GatewayTest::start(None).await;
    t.with_data(|d| {
        d.client_states
            .insert("07-tendermint-0".into(), client_state_xdr());
    });

    let resp = t
        .query()
        .query_client_state(QueryClientStateRequest {
            client_id: "07-tendermint-0".into(),
            height: 10,
        })
        .await
        .unwrap()
        .into_inner();

    assert!(!resp.client_state.is_empty());
    assert_eq!(resp.proof_height, 10);

    let decoded = <AnyClientState as Protobuf<Any>>::decode_vec(&resp.client_state).unwrap();
    assert_eq!(decoded.chain_id(), "testchain-1");
    assert_eq!(decoded.latest_height(), 10);
}

#[tokio::test]
async fn query_client_state_errors_when_state_missing() {
    let t = GatewayTest::start(None).await;

    let err = t
        .query()
        .query_client_state(QueryClientStateRequest {
            client_id: "absent".into(),
            height: 1,
        })
        .await
        .unwrap_err();

    assert_eq!(err.code(), Code::Internal);
}

#[tokio::test]
async fn events_decode_packet_attributes() {
    let t = GatewayTest::start(Some([1u8; 32])).await;

    let payload = scval_struct(vec![
        ("source_port", scval_string("transfer").unwrap()),
        ("dest_port", scval_string("transfer").unwrap()),
    ])
    .unwrap();
    let packet = scval_struct(vec![
        ("sequence", ScVal::U64(7)),
        ("source_client", scval_string("07-tendermint-0").unwrap()),
        ("dest_client", scval_string("08-wasm-0").unwrap()),
        ("payloads", scval_vec(vec![payload]).unwrap()),
    ])
    .unwrap();
    let value =
        hex::encode(scval_to_xdr(&scval_struct(vec![("packet", packet)]).unwrap()).unwrap());
    let topic = hex::encode(scval_to_xdr(&scval_symbol("send_packet").unwrap()).unwrap());

    t.with_data(|d| {
        d.latest_ledger = 5;
        d.events.push(json!({
            "id": "e1",
            "ledger": 5,
            "ledger_closed_at": "2026-01-01T00:00:00Z",
            "contract_id": "Crouter",
            "tx_hash": "deadbeef",
            "topics_xdr": [topic],
            "value_xdr": value,
        }));
    });

    let resp = t
        .query()
        .events(EventsRequest {
            start_ledger: 1,
            cursor: String::new(),
            limit: 0,
        })
        .await
        .unwrap()
        .into_inner();

    let attributes = &resp.events[0].attributes;
    assert!(attributes.contains("type=send_packet"), "got: {attributes}");
    assert!(
        attributes.contains("packet_sequence=7"),
        "got: {attributes}"
    );
    assert!(
        attributes.contains("packet_src_channel=07-tendermint-0"),
        "got: {attributes}"
    );
    assert!(
        attributes.contains("packet_dst_channel=08-wasm-0"),
        "got: {attributes}"
    );
    assert!(
        attributes.contains("packet_src_port=transfer"),
        "got: {attributes}"
    );
    assert!(
        attributes.contains("packet_dst_port=transfer"),
        "got: {attributes}"
    );
}

#[tokio::test]
async fn query_consensus_state_converts_soroban_to_protobuf() {
    let t = GatewayTest::start(None).await;

    let consensus_state = scval_struct(vec![
        ("timestamp_secs", ScVal::U64(1_700_000_000)),
        ("next_validators_hash", scval_bytes(&[7u8; 32]).unwrap()),
        ("root", scval_bytes(&[9u8; 32]).unwrap()),
    ])
    .unwrap();
    let xdr = scval_to_xdr(&consensus_state).unwrap();

    t.with_data(|d| {
        d.consensus_states.insert("07-tendermint-0".into(), xdr);
    });

    let resp = t
        .query()
        .query_consensus_state(QueryConsensusStateRequest {
            client_id: "07-tendermint-0".into(),
            revision_number: 0,
            revision_height: 10,
        })
        .await
        .unwrap()
        .into_inner();

    assert!(!resp.consensus_state.is_empty());
    assert_eq!(resp.proof_height, 10);
}

#[tokio::test]
async fn packet_commitment_absent_yields_nonmembership_proof() {
    let t = GatewayTest::start(None).await;
    t.with_data(|d| {
        d.ledgers.insert(5, (vec![], None));
    });

    let resp = t
        .query()
        .query_packet_commitment(QueryPacketCommitmentRequest {
            client_id: "07-tendermint-0".into(),
            sequence: 1,
            height: 5,
        })
        .await
        .unwrap()
        .into_inner();

    assert!(resp.commitment.is_empty());
    assert!(!resp.proof.is_empty());
    assert_eq!(resp.proof_height, 5);
}

#[tokio::test]
async fn state_reflects_contract_write_as_membership_proof() {
    let contract = [7u8; 32];
    let client_id = "07-tendermint-0";
    let key = packet_commitment_path(client_id.as_bytes(), 1);
    let value = vec![0xCDu8; 32];
    let meta = ledger_meta_with_write(contract, key, value);

    let t = GatewayTest::start(Some(contract)).await;
    t.with_data(|d| {
        d.ledgers.insert(5, (vec![], Some(meta)));
    });

    let found = t
        .query()
        .query_packet_commitment(QueryPacketCommitmentRequest {
            client_id: client_id.into(),
            sequence: 1,
            height: 5,
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(found.commitment.len(), 32);
    assert!(!found.proof.is_empty());

    let other = t
        .query()
        .query_packet_commitment(QueryPacketCommitmentRequest {
            client_id: client_id.into(),
            sequence: 2,
            height: 5,
        })
        .await
        .unwrap()
        .into_inner();

    assert!(other.commitment.is_empty());
}

#[tokio::test]
async fn query_ibc_header_assembles_scp_evidence() {
    const LEDGER: u32 = 4_160_600;
    let archive = crate::archive_fixture::archive_responses(LEDGER);
    let envelope_count = archive.envelope_count;
    let ledger_hash = archive.ledger_hash;

    let t = GatewayTest::start(None).await;
    t.with_data(|d| {
        d.latest_ledger = LEDGER + 2; // slot N+1 must have closed
        d.archive_scp = archive.scp;
        d.archive_headers = archive.headers;
        d.archive_tx_sets = archive.tx_sets;
    });

    let resp = t
        .query()
        .query_ibc_header(QueryIbcHeaderRequest {
            height: LEDGER as u64,
        })
        .await
        .unwrap()
        .into_inner();

    let header = interstellar_gateway::proto::StellarHeader::decode(resp.header.as_slice())
        .expect("a StellarHeader the light client can decode");

    assert_eq!(header.slot_index, LEDGER as u64);
    assert_eq!(header.scp_envelopes.len(), envelope_count);
    assert_eq!(
        header.next_scp_envelopes.len(),
        envelope_count,
        "slot N+1's envelopes are what bind the parts of the header SCP does \
         not sign"
    );
    assert_eq!(header.quorum_sets_xdr.len(), 1);

    assert_eq!(
        &header.next_tx_set_xdr[4..36],
        &ledger_hash,
        "the next slot's tx set must follow this ledger"
    );

    assert!(header.state_root_proof.is_none());
}

const ROUTER: [u8; 32] = [7u8; 32];
const ROOT: [u8; 32] = [42u8; 32];

async fn header_with_meta(meta: Vec<u8>) -> interstellar_gateway::proto::StellarHeader {
    const LEDGER: u32 = 4_160_600;
    let archive = crate::archive_fixture::archive_responses(LEDGER);

    let t = GatewayTest::start(Some(ROUTER)).await;
    t.with_data(|d| {
        d.latest_ledger = LEDGER + 2;
        d.archive_scp = archive.scp;
        d.archive_headers = archive.headers;
        d.archive_tx_sets = archive.tx_sets;
        d.ledgers.insert(LEDGER, (Vec::new(), Some(meta)));
    });

    let resp = t
        .query()
        .query_ibc_header(QueryIbcHeaderRequest {
            height: LEDGER as u64,
        })
        .await
        .unwrap()
        .into_inner();

    interstellar_gateway::proto::StellarHeader::decode(resp.header.as_slice()).unwrap()
}

fn root_in(proof: &interstellar_gateway::proto::StateRootProof) -> Vec<u8> {
    let preimage =
        InvokeHostFunctionSuccessPreImage::from_xdr(&proof.success_preimage_xdr, Limits::none())
            .expect("the preimage the result committed to");

    let event = preimage
        .events
        .iter()
        .find(|e| e.contract_id.as_ref().is_some_and(|id| id.0 .0 == ROUTER))
        .expect("an event from the router");

    let soroban_client::xdr::ContractEventBody::V0(body) = &event.body;
    match &body.data {
        ScVal::Bytes(bytes) => bytes.0.to_vec(),
        other => panic!("the root must be bytes, got {other:?}"),
    }
}

#[tokio::test]
async fn state_root_proof_binds_a_single_router_invocation() {
    let meta = ledger_meta_with_invocations(&[Some(vec![root_event(ROUTER, "ibc_root", ROOT)])]);

    let header = header_with_meta(meta).await;

    let proof = header
        .state_root_proof
        .expect("the router emitted a root in this ledger");
    assert_eq!(proof.result_index, 0);
    assert_eq!(proof.result_pairs.len(), 1);
    assert_eq!(root_in(&proof), ROOT.to_vec());
}

#[tokio::test]
async fn state_root_proof_points_at_the_router_among_other_invocations() {
    let other = [3u8; 32];
    let meta = ledger_meta_with_invocations(&[
        None,
        Some(vec![root_event(other, "ibc_root", [1u8; 32])]),
        Some(vec![root_event(ROUTER, "ibc_root", ROOT)]),
        Some(vec![root_event(other, "transfer", [2u8; 32])]),
    ]);

    let header = header_with_meta(meta).await;

    let proof = header
        .state_root_proof
        .expect("one of the invocations is the router's");
    assert_eq!(
        proof.result_index, 2,
        "the index must name the router's own result, not another contract's"
    );
    assert_eq!(
        proof.result_pairs.len(),
        4,
        "the whole result set travels with the header so txSetResultHash can be reproduced"
    );
    assert_eq!(root_in(&proof), ROOT.to_vec());
}

#[tokio::test]
async fn state_root_proof_is_absent_without_a_router_event() {
    let other = [3u8; 32];
    let meta =
        ledger_meta_with_invocations(&[None, Some(vec![root_event(other, "ibc_root", [1u8; 32])])]);

    let header = header_with_meta(meta).await;

    assert!(
        header.state_root_proof.is_none(),
        "a ledger with no router root must bind no root rather than fail"
    );
}

#[tokio::test]
async fn state_root_proof_ignores_a_router_event_with_another_topic() {
    let meta = ledger_meta_with_invocations(&[Some(vec![root_event(ROUTER, "transfer", ROOT)])]);

    let header = header_with_meta(meta).await;

    assert!(
        header.state_root_proof.is_none(),
        "only the configured root event topic may carry the state root"
    );
}

#[tokio::test]
async fn state_root_proof_is_absent_when_no_transaction_ran() {
    let header = header_with_meta(ledger_meta_with_invocations(&[])).await;

    assert!(header.state_root_proof.is_none());
}

#[tokio::test]
async fn query_ibc_header_fails_diagnosably_without_archive_evidence() {
    let t = GatewayTest::start(None).await;
    let header_xdr = LedgerHeader::default().to_xdr(Limits::none()).unwrap();
    t.with_data(|d| {
        d.latest_ledger = 7;
        d.ledgers.insert(5, (header_xdr, None));
    });

    let err = t
        .query()
        .query_ibc_header(QueryIbcHeaderRequest { height: 5 })
        .await
        .expect_err("no archived SCP evidence for this ledger");

    assert_eq!(err.code(), tonic::Code::Internal);
    assert!(
        err.message().contains("header assembly failed"),
        "the error must name the stage that failed, got: {}",
        err.message()
    );
}

#[tokio::test]
async fn packet_receipt_reflects_contract_write() {
    let contract = [8u8; 32];
    let client_id = "07-tendermint-0";
    let key = packet_receipt_path(client_id.as_bytes(), 1);
    let meta = ledger_meta_with_write(contract, key, vec![0x01]);

    let t = GatewayTest::start(Some(contract)).await;
    t.with_data(|d| {
        d.ledgers.insert(5, (vec![], Some(meta)));
    });

    let resp = t
        .query()
        .query_packet_receipt(QueryPacketReceiptRequest {
            client_id: client_id.into(),
            sequence: 1,
            height: 5,
        })
        .await
        .unwrap()
        .into_inner();

    assert!(resp.received);
    assert!(!resp.proof.is_empty());
}

#[tokio::test]
async fn acknowledgement_reflects_contract_write() {
    let contract = [9u8; 32];
    let client_id = "07-tendermint-0";
    let key = ack_commitment_path(client_id.as_bytes(), 1);
    let meta = ledger_meta_with_write(contract, key, vec![0xEEu8; 32]);

    let t = GatewayTest::start(Some(contract)).await;
    t.with_data(|d| {
        d.ledgers.insert(5, (vec![], Some(meta)));
    });

    let resp = t
        .query()
        .query_acknowledgement(QueryAcknowledgementRequest {
            client_id: client_id.into(),
            sequence: 1,
            height: 5,
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.acknowledgement.len(), 32);
    assert!(!resp.proof.is_empty());
}

#[tokio::test]
async fn query_client_states_lists_decoded_states() {
    let t = GatewayTest::start(None).await;
    t.with_data(|d| {
        d.client_states
            .insert("07-tendermint-0".into(), client_state_xdr());
    });

    let resp = t
        .query()
        .query_client_states(QueryClientStatesRequest {})
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.client_states.len(), 1);
    assert_eq!(resp.client_states[0].client_id, "07-tendermint-0");
    assert!(!resp.client_states[0].client_state.is_empty());
}

#[tokio::test]
async fn replay_across_a_gap_applies_every_event_bearing_ledger() {
    let contract = [7u8; 32];
    let client_id = "07-tendermint-0";

    let first = ledger_meta_with_write(
        contract,
        packet_commitment_path(client_id.as_bytes(), 1),
        vec![0xC1u8; 32],
    );
    let second = ledger_meta_with_write(
        contract,
        packet_commitment_path(client_id.as_bytes(), 2),
        vec![0xC2u8; 32],
    );

    let t = GatewayTest::start(Some(contract)).await;
    t.with_data(|d| {
        d.ledgers.insert(5, (vec![], Some(first)));
        d.ledgers.insert(40, (vec![], Some(second)));
        d.events.push(router_event_json(contract, 5));
        d.events.push(router_event_json(contract, 40));
    });

    let at_five = t
        .query()
        .query_packet_commitment(QueryPacketCommitmentRequest {
            client_id: client_id.into(),
            sequence: 1,
            height: 5,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(at_five.commitment.len(), 32);

    let across_gap = t
        .query()
        .query_packet_commitment(QueryPacketCommitmentRequest {
            client_id: client_id.into(),
            sequence: 2,
            height: 40,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        across_gap.commitment.len(),
        32,
        "a write 35 ledgers past the last processed one must survive the filtered replay"
    );

    let earlier = t
        .query()
        .query_packet_commitment(QueryPacketCommitmentRequest {
            client_id: client_id.into(),
            sequence: 1,
            height: 40,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        earlier.commitment.len(),
        32,
        "the filtered replay must not drop state written before the gap"
    );
}

#[tokio::test]
async fn a_single_ledger_query_does_not_consult_the_event_index() {
    let contract = [7u8; 32];
    let client_id = "07-tendermint-0";

    let meta = ledger_meta_with_write(
        contract,
        packet_commitment_path(client_id.as_bytes(), 1),
        vec![0xC1u8; 32],
    );

    let t = GatewayTest::start(Some(contract)).await;
    t.with_data(|d| {
        d.ledgers.insert(5, (vec![], Some(meta)));
    });

    let at_five = t
        .query()
        .query_packet_commitment(QueryPacketCommitmentRequest {
            client_id: client_id.into(),
            sequence: 1,
            height: 5,
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        at_five.commitment.len(),
        32,
        "a single-ledger query must never consult the event index"
    );
}

#[tokio::test]
async fn replay_keeps_paging_past_a_window_that_holds_no_events() {
    let contract = [7u8; 32];
    let client_id = "07-tendermint-0";

    let first = ledger_meta_with_write(
        contract,
        packet_commitment_path(client_id.as_bytes(), 1),
        vec![0xC1u8; 32],
    );
    let late = ledger_meta_with_write(
        contract,
        packet_commitment_path(client_id.as_bytes(), 2),
        vec![0xC2u8; 32],
    );

    let t = GatewayTest::start(Some(contract)).await;
    t.with_data(|d| {
        d.ledgers.insert(5, (vec![], Some(first)));
        d.ledgers.insert(900, (vec![], Some(late)));
        d.event_pages = vec![
            ("0002147483648000-0".to_string(), vec![]),
            (String::new(), vec![router_event_json(contract, 900)]),
        ];
    });

    t.query()
        .query_packet_commitment(QueryPacketCommitmentRequest {
            client_id: client_id.into(),
            sequence: 1,
            height: 5,
        })
        .await
        .unwrap();

    let across = t
        .query()
        .query_packet_commitment(QueryPacketCommitmentRequest {
            client_id: client_id.into(),
            sequence: 2,
            height: 900,
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        across.commitment.len(),
        32,
        "an empty event page means 'nothing in this window', not 'no more events' — \
         stopping there serves an absence proof for a commitment that exists"
    );
}

#[tokio::test]
async fn replay_stops_once_the_cursor_has_swept_past_the_target() {
    let contract = [7u8; 32];
    let client_id = "07-tendermint-0";

    let meta = ledger_meta_with_write(
        contract,
        packet_commitment_path(client_id.as_bytes(), 1),
        vec![0xC1u8; 32],
    );

    let t = GatewayTest::start(Some(contract)).await;
    t.with_data(|d| {
        d.ledgers.insert(5, (vec![], Some(meta)));
        d.ledgers.insert(40, (vec![], None));
        d.event_pages = vec![(format!("{}-0", 9_000u64 << 32), vec![])];
    });

    t.query()
        .query_packet_commitment(QueryPacketCommitmentRequest {
            client_id: client_id.into(),
            sequence: 1,
            height: 5,
        })
        .await
        .unwrap();

    let later = t
        .query()
        .query_packet_commitment(QueryPacketCommitmentRequest {
            client_id: client_id.into(),
            sequence: 1,
            height: 40,
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        later.commitment.len(),
        32,
        "a cursor already past the target ends the sweep without dropping earlier state"
    );
}
