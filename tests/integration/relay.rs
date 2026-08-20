use std::collections::HashMap;

use prost::Message as _;
use serde_json::json;
use tonic::Code;

use stellar_ibc_core::ibc::msg::{
    MsgCreateClient, MsgUpdateClient, ProtoMessage as _, ProtoTxBody, WasmClientMessage,
    WasmClientState, WasmConsensusState,
};

use interstellar_gateway::proto::proofapi::{CreateClientRequest, UpdateClientRequest};
use interstellar_gateway::proto::{StellarClientState, StellarConsensusState};

use super::mock::{ledger_meta_with_invocations, root_event, GatewayTest};

const ROUTER: [u8; 32] = [7u8; 32];
const ROOT: [u8; 32] = [42u8; 32];
const LEDGER: u32 = 4_160_600;
const STELLAR: &str = "stellar-testnet";
const COSMOS: &str = "simd-1";

fn quorum_set_xdr() -> String {
    let mut out = 1u32.to_be_bytes().to_vec();
    out.extend_from_slice(&1u32.to_be_bytes());
    out.extend_from_slice(&0u32.to_be_bytes());
    out.extend_from_slice(&[3u8; 32]);
    out.extend_from_slice(&0u32.to_be_bytes());
    hex::encode(out)
}

fn parameters(quorum_configs: serde_json::Value) -> HashMap<String, String> {
    HashMap::from([
        ("quorum_configs".to_string(), quorum_configs.to_string()),
        ("wasm_checksum".to_string(), hex::encode([5u8; 32])),
        ("height".to_string(), LEDGER.to_string()),
    ])
}

fn one_config() -> serde_json::Value {
    json!([{ "quorum_set_xdr": quorum_set_xdr(), "valid_from": 0 }])
}

async fn start_with(latest: u32, meta: Vec<u8>) -> GatewayTest {
    let archive = crate::archive_fixture::archive_responses(LEDGER);

    let t = GatewayTest::start(Some(ROUTER)).await;
    t.with_data(|d| {
        d.latest_ledger = latest;
        d.archive_scp = archive.scp;
        d.archive_headers = archive.headers;
        d.archive_tx_sets = archive.tx_sets;
        d.ledgers.insert(LEDGER, (Vec::new(), Some(meta)));
    });
    t
}

fn meta_with_root() -> Vec<u8> {
    ledger_meta_with_invocations(&[Some(vec![root_event(ROUTER, "ibc_root", ROOT)])])
}

async fn start() -> GatewayTest {
    start_with(LEDGER + 2, meta_with_root()).await
}

async fn start_for_refresh() -> GatewayTest {
    start_with(LEDGER + 1, meta_with_root()).await
}

fn stellar_client_state(tx: &[u8]) -> (StellarClientState, WasmClientState) {
    let body = ProtoTxBody::decode(tx).expect("a cosmos tx body");
    assert_eq!(body.messages.len(), 1);
    assert_eq!(
        body.messages[0].type_url,
        "/ibc.core.client.v1.MsgCreateClient"
    );

    let msg = MsgCreateClient::decode(body.messages[0].value.as_slice()).unwrap();
    let wasm = WasmClientState::decode(msg.client_state.unwrap().value.as_slice()).unwrap();
    let inner = StellarClientState::decode(wasm.data.as_slice()).unwrap();
    (inner, wasm)
}

#[tokio::test]
async fn create_client_builds_a_wasm_wrapped_stellar_client() {
    let t = start().await;

    let resp = t
        .relay()
        .create_client(CreateClientRequest {
            src_chain: STELLAR.to_string(),
            dst_chain: COSMOS.to_string(),
            parameters: parameters(one_config()),
        })
        .await
        .unwrap()
        .into_inner();

    let (inner, wasm) = stellar_client_state(&resp.tx);

    assert_eq!(wasm.checksum, [5u8; 32].to_vec());
    assert_eq!(wasm.latest_height.unwrap().revision_height, LEDGER as u64);
    assert_eq!(inner.chain_id, STELLAR);
    assert_eq!(inner.latest_height.unwrap().revision_height, LEDGER as u64);
    assert_eq!(inner.router_contract_id, ROUTER.to_vec());
    assert_eq!(inner.root_event_topic, b"ibc_root".to_vec());
    assert_eq!(inner.quorum_configs.len(), 1);
    assert_eq!(
        inner.quorum_configs[0].quorum_set_xdr,
        hex::decode(quorum_set_xdr()).unwrap()
    );
    assert_eq!(
        inner.max_consensus_age,
        14 * 24 * 60 * 60,
        "a client with no configured trusting period must not be immortal"
    );
}

#[tokio::test]
async fn create_client_carries_the_root_in_the_consensus_state() {
    let t = start().await;

    let resp = t
        .relay()
        .create_client(CreateClientRequest {
            src_chain: STELLAR.to_string(),
            dst_chain: COSMOS.to_string(),
            parameters: parameters(one_config()),
        })
        .await
        .unwrap()
        .into_inner();

    let body = ProtoTxBody::decode(resp.tx.as_slice()).unwrap();
    let msg = MsgCreateClient::decode(body.messages[0].value.as_slice()).unwrap();
    let wasm = WasmConsensusState::decode(msg.consensus_state.unwrap().value.as_slice()).unwrap();
    let consensus = StellarConsensusState::decode(wasm.data.as_slice()).unwrap();

    assert_eq!(consensus.ledger_hash.len(), 32);
    assert_eq!(consensus.root.len(), 32);
}

#[tokio::test]
async fn create_client_requires_a_trust_root() {
    let t = start().await;
    let mut params = parameters(one_config());
    params.remove("quorum_configs");

    let err = t
        .relay()
        .create_client(CreateClientRequest {
            src_chain: STELLAR.to_string(),
            dst_chain: COSMOS.to_string(),
            parameters: params,
        })
        .await
        .unwrap_err();

    assert_eq!(err.code(), Code::InvalidArgument);
    assert!(
        err.message().contains("quorum_configs is required"),
        "got: {}",
        err.message()
    );
}

#[tokio::test]
async fn create_client_rejects_an_empty_trust_root() {
    let t = start().await;

    let err = t
        .relay()
        .create_client(CreateClientRequest {
            src_chain: STELLAR.to_string(),
            dst_chain: COSMOS.to_string(),
            parameters: parameters(json!([])),
        })
        .await
        .unwrap_err();

    assert_eq!(err.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn create_client_rejects_configurations_sharing_a_valid_from() {
    let t = start().await;
    let two = json!([
        { "quorum_set_xdr": quorum_set_xdr(), "valid_from": 0 },
        { "quorum_set_xdr": quorum_set_xdr(), "valid_from": 0 },
    ]);

    let err = t
        .relay()
        .create_client(CreateClientRequest {
            src_chain: STELLAR.to_string(),
            dst_chain: COSMOS.to_string(),
            parameters: parameters(two),
        })
        .await
        .unwrap_err();

    assert_eq!(err.code(), Code::InvalidArgument);
    assert!(
        err.message().contains("chosen by list order"),
        "got: {}",
        err.message()
    );
}

#[tokio::test]
async fn create_client_keeps_a_rotation_with_distinct_ranges() {
    let t = start().await;
    let rotation = json!([
        { "quorum_set_xdr": quorum_set_xdr(), "valid_from": 0 },
        { "quorum_set_xdr": quorum_set_xdr(), "valid_from": 4_000_000 },
    ]);

    let resp = t
        .relay()
        .create_client(CreateClientRequest {
            src_chain: STELLAR.to_string(),
            dst_chain: COSMOS.to_string(),
            parameters: parameters(rotation),
        })
        .await
        .unwrap()
        .into_inner();

    let (inner, _) = stellar_client_state(&resp.tx);
    assert_eq!(inner.quorum_configs.len(), 2);
    assert_eq!(inner.quorum_configs[1].valid_from, 4_000_000);
}

fn update_header(tx: &[u8]) -> (String, interstellar_gateway::proto::StellarHeader) {
    let body = ProtoTxBody::decode(tx).expect("a cosmos tx body");
    assert_eq!(body.messages.len(), 1);
    assert_eq!(
        body.messages[0].type_url,
        "/ibc.core.client.v1.MsgUpdateClient"
    );

    let msg = MsgUpdateClient::decode(body.messages[0].value.as_slice()).unwrap();
    let carried = msg.client_message.unwrap();
    assert_eq!(carried.type_url, "/ibc.lightclients.wasm.v1.ClientMessage");

    let wasm = WasmClientMessage::decode(carried.value.as_slice()).unwrap();
    let header = interstellar_gateway::proto::StellarHeader::decode(wasm.data.as_slice()).unwrap();
    (msg.client_id, header)
}

#[tokio::test]
async fn update_client_builds_a_header_for_the_named_client() {
    let t = start_for_refresh().await;

    let resp = t
        .relay()
        .update_client(UpdateClientRequest {
            src_chain: STELLAR.to_string(),
            dst_chain: COSMOS.to_string(),
            dst_client_id: "08-wasm-0".to_string(),
        })
        .await
        .unwrap()
        .into_inner();

    let (client_id, header) = update_header(&resp.tx);

    assert_eq!(client_id, "08-wasm-0");
    assert_eq!(
        header.slot_index, LEDGER as u64,
        "the refresh target must be a ledger whose successor has closed"
    );
    assert!(!header.scp_envelopes.is_empty());
    assert!(!header.next_scp_envelopes.is_empty());
    assert!(header.state_root_proof.is_some());
}

#[tokio::test]
async fn update_client_refreshes_a_ledger_that_carries_no_root() {
    let t = start_with(LEDGER + 1, ledger_meta_with_invocations(&[])).await;

    let resp = t
        .relay()
        .update_client(UpdateClientRequest {
            src_chain: STELLAR.to_string(),
            dst_chain: COSMOS.to_string(),
            dst_client_id: "08-wasm-0".to_string(),
        })
        .await
        .expect("an idle link must still be able to keep its client alive")
        .into_inner();

    let (_, header) = update_header(&resp.tx);

    assert!(
        header.state_root_proof.is_none(),
        "a ledger with no provable write binds no root"
    );
}

#[tokio::test]
async fn update_client_requires_a_client_id() {
    let t = start_for_refresh().await;

    let err = t
        .relay()
        .update_client(UpdateClientRequest {
            src_chain: STELLAR.to_string(),
            dst_chain: COSMOS.to_string(),
            dst_client_id: String::new(),
        })
        .await
        .unwrap_err();

    assert_eq!(err.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn update_client_rejects_an_unknown_chain_pair() {
    let t = start_for_refresh().await;

    let err = t
        .relay()
        .update_client(UpdateClientRequest {
            src_chain: COSMOS.to_string(),
            dst_chain: STELLAR.to_string(),
            dst_client_id: "07-tendermint-0".to_string(),
        })
        .await
        .unwrap_err();

    assert_eq!(err.code(), Code::Unimplemented);
}

#[tokio::test]
async fn create_client_rejects_an_unknown_chain_pair() {
    let t = start().await;

    let err = t
        .relay()
        .create_client(CreateClientRequest {
            src_chain: COSMOS.to_string(),
            dst_chain: STELLAR.to_string(),
            parameters: parameters(one_config()),
        })
        .await
        .unwrap_err();

    assert_eq!(err.code(), Code::Unimplemented);
}
