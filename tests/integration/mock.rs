use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::{Path, Query, State as AxState};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use soroban_client::xdr::{
    ContractDataDurability, ContractDataEntry, ContractEvent, ContractEventBody, ContractEventType,
    ContractEventV0, ContractId, ExtensionPoint, Hash, InvokeHostFunctionResult,
    InvokeHostFunctionSuccessPreImage, LedgerCloseMeta, LedgerCloseMetaExt, LedgerCloseMetaV0,
    LedgerCloseMetaV2, LedgerEntry, LedgerEntryChange, LedgerEntryChanges, LedgerEntryData,
    LedgerEntryExt, Limits, OperationMeta, OperationMetaV2, OperationResult, OperationResultTr,
    ScSymbol, ScVal, SorobanTransactionMetaExt, SorobanTransactionMetaV2, StringM, TransactionMeta,
    TransactionMetaV1, TransactionMetaV4, TransactionResult, TransactionResultExt,
    TransactionResultMeta, TransactionResultMetaV1, TransactionResultPair, TransactionResultResult,
    VecM, WriteXdr,
};
use tokio::net::TcpListener;
use tonic::transport::Channel;

use ibc::clients::tendermint::client_state::ClientState as TmClientState;
use ibc::clients::tendermint::consensus_state::ConsensusState as TmConsensusState;
use ibc::core::commitment_types::specs::ProofSpecs;
use ibc::primitives::proto::Protobuf;
use ibc_proto::google::protobuf::{Duration as PbDuration, Timestamp};
use ibc_proto::ibc::core::client::v1::Height as RawHeight;
use ibc_proto::ibc::core::commitment::v1::MerkleRoot;
use ibc_proto::ibc::lightclients::tendermint::v1::{
    ClientState as RawTmClientState, ConsensusState as RawTmConsensusState, Fraction,
};

use ipsa_chains::conversion::{scval_bytes, scval_struct};
use ipsa_gateway::config::GatewayConfig;
use ipsa_gateway::proto::proofapi::proof_api_service_client::ProofApiServiceClient;
use ipsa_gateway::proto::stellar_gateway_msg_client::StellarGatewayMsgClient;
use ipsa_gateway::proto::stellar_gateway_query_client::StellarGatewayQueryClient;
use ipsa_gateway::runner;

pub struct PrepareCall {
    pub signer: String,
    pub method: String,
    pub args: Vec<Vec<u8>>,
}

#[derive(Default)]
pub struct MockData {
    pub latest_ledger: u32,
    pub prepare_tx_xdr: Vec<u8>,
    pub submit_hash: String,
    pub submit_return_value_xdr: Vec<u8>,
    pub ledgers: HashMap<u32, (Vec<u8>, Option<Vec<u8>>)>,
    pub client_states: HashMap<String, Vec<u8>>,
    pub consensus_states: HashMap<String, Vec<u8>>,
    pub events: Vec<Value>,
    pub event_pages: Vec<(String, Vec<Value>)>,
    pub prepare_calls: Vec<PrepareCall>,
    pub submit_calls: Vec<String>,
    pub archive_latest_ledger: u32,
    pub archive_scp: HashMap<u32, Value>,
    pub archive_headers: HashMap<u32, Value>,
    pub archive_tx_sets: HashMap<u32, Value>,
    pub cosmos_packets: HashMap<String, Value>,
    pub cosmos_proofs: HashMap<Vec<u8>, (Vec<u8>, Vec<u8>)>,
    pub cosmos_headers: HashMap<u64, Vec<u8>>,
    pub cosmos_latest_height: u64,
    pub cosmos_proof_heights: Vec<u64>,
}

pub struct GatewayTest {
    data: Arc<Mutex<MockData>>,
    msg: StellarGatewayMsgClient<Channel>,
    query: StellarGatewayQueryClient<Channel>,
    relay: ProofApiServiceClient<Channel>,
}

impl GatewayTest {
    pub async fn start(ibc_contract_id: Option<[u8; 32]>) -> Self {
        let (api_url, data) = start_mock_api().await;

        let id_string = ibc_contract_id
            .map(|c| format!("{}", stellar_strkey::Contract(c)))
            .unwrap_or_default();
        let cfg = GatewayConfig {
            host: "127.0.0.1".to_string(),
            grpc_port: 0,
            api_url,
            ibc_contract_id: id_string,
            transfer_contract_id: String::new(),
            source_account: "GBXDMJBUPSBOG23CYLZDFR5SHS33USHQRSI4XGZUL75SHARCTDPGA22H".to_string(),
            stellar_chain_id: "stellar-testnet".to_string(),
            cosmos_chain_id: "simd-1".to_string(),
            event_lookback_ledgers: 17280,
            cosmos_signer: String::new(),
            router_genesis_ledger: None,
            snapshot_database_url: None,
            follow_seconds: 0,
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            runner::serve_with_listener(cfg, listener).await.unwrap();
        });

        let channel = connect(format!("http://{addr}")).await;

        Self {
            data,
            msg: StellarGatewayMsgClient::new(channel.clone()),
            query: StellarGatewayQueryClient::new(channel.clone()),
            relay: ProofApiServiceClient::new(channel),
        }
    }

    pub fn msg(&self) -> StellarGatewayMsgClient<Channel> {
        self.msg.clone()
    }

    pub fn query(&self) -> StellarGatewayQueryClient<Channel> {
        self.query.clone()
    }

    pub fn relay(&self) -> ProofApiServiceClient<Channel> {
        self.relay.clone()
    }

    pub fn with_data<R>(&self, f: impl FnOnce(&mut MockData) -> R) -> R {
        let mut d = self.data.lock().unwrap();
        f(&mut d)
    }
}

async fn connect(endpoint: String) -> Channel {
    for _ in 0..100 {
        if let Ok(channel) = Channel::from_shared(endpoint.clone())
            .unwrap()
            .connect()
            .await
        {
            return channel;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("could not connect to gateway at {endpoint}");
}

pub async fn start_mock_api() -> (String, Arc<Mutex<MockData>>) {
    let data = Arc::new(Mutex::new(MockData::default()));
    let app = Router::new()
        .route("/ledger/latest", get(ledger_latest))
        .route("/ledger/{seq}", get(ledger_by_seq))
        .route("/tx/prepare", post(tx_prepare))
        .route("/tx/submit", post(tx_submit))
        .route("/events", get(events))
        .route("/archive/latest", get(archive_latest))
        .route("/archive/scp/{ledger}", get(archive_scp))
        .route(
            "/archive/ledger-header/{ledger}",
            get(archive_ledger_header),
        )
        .route("/archive/tx-set/{ledger}", get(archive_tx_set))
        .route("/cosmos/latest", get(cosmos_latest))
        .route("/cosmos/tx/{hash}/packets", get(cosmos_packets))
        .route("/cosmos/proof", get(cosmos_proof))
        .route("/cosmos/header/{height}", get(cosmos_header))
        .route("/stellar/clients", get(list_clients))
        .route("/stellar/clients/{id}/state", get(client_state))
        .route(
            "/stellar/clients/{id}/consensus/{height}",
            get(consensus_state),
        )
        .route(
            "/stellar/transfer/balance/{denom}/{address}",
            get(transfer_balance),
        )
        .with_state(data.clone());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (format!("http://{addr}"), data)
}

async fn ledger_latest(AxState(data): AxState<Arc<Mutex<MockData>>>) -> Json<Value> {
    let d = data.lock().unwrap();
    Json(json!({ "sequence": d.latest_ledger }))
}

async fn ledger_by_seq(
    AxState(data): AxState<Arc<Mutex<MockData>>>,
    Path(seq): Path<u32>,
) -> Json<Value> {
    let d = data.lock().unwrap();
    let (header, meta) = d
        .ledgers
        .get(&seq)
        .cloned()
        .unwrap_or_else(|| (vec![], None));
    let mut out = json!({ "header_xdr": hex::encode(header) });
    if let Some(meta) = meta {
        out["metadata_xdr"] = json!(hex::encode(meta));
    }
    Json(out)
}

async fn tx_prepare(
    AxState(data): AxState<Arc<Mutex<MockData>>>,
    Json(body): Json<Value>,
) -> Json<Value> {
    let signer = body
        .get("signer")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let method = body
        .get("method")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let args = body
        .get("args_xdr")
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str())
                .filter_map(|s| hex::decode(s).ok())
                .collect()
        })
        .unwrap_or_default();

    let mut d = data.lock().unwrap();
    d.prepare_calls.push(PrepareCall {
        signer,
        method,
        args,
    });
    let tx_xdr = hex::encode(&d.prepare_tx_xdr);
    Json(json!({ "tx_xdr": tx_xdr }))
}

async fn tx_submit(
    AxState(data): AxState<Arc<Mutex<MockData>>>,
    Json(body): Json<Value>,
) -> Json<Value> {
    let tx = body
        .get("tx_xdr")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let mut d = data.lock().unwrap();
    d.submit_calls.push(tx);
    let mut out = json!({ "hash": d.submit_hash });
    if !d.submit_return_value_xdr.is_empty() {
        out["return_value_xdr"] = json!(hex::encode(&d.submit_return_value_xdr));
    }
    Json(out)
}

async fn events(
    AxState(data): AxState<Arc<Mutex<MockData>>>,
    Query(params): Query<HashMap<String, String>>,
) -> Json<Value> {
    let d = data.lock().unwrap();

    if !d.event_pages.is_empty() {
        let asked = params.get("cursor").cloned().unwrap_or_default();
        let index = if asked.is_empty() {
            0
        } else {
            d.event_pages
                .iter()
                .position(|(c, _)| *c == asked)
                .map(|i| i + 1)
                .unwrap_or(d.event_pages.len())
        };
        let (cursor, events) = d
            .event_pages
            .get(index)
            .cloned()
            .unwrap_or_else(|| (String::new(), Vec::new()));
        return Json(json!({
            "latest_ledger": d.latest_ledger,
            "cursor": cursor,
            "events": events,
        }));
    }

    Json(json!({
        "latest_ledger": d.latest_ledger,
        "cursor": "",
        "events": d.events.clone(),
    }))
}

async fn list_clients(AxState(data): AxState<Arc<Mutex<MockData>>>) -> Json<Value> {
    let d = data.lock().unwrap();
    let ids: Vec<String> = d.client_states.keys().cloned().collect();
    Json(json!({ "clients": [ { "client_ids": ids } ] }))
}

async fn client_state(
    AxState(data): AxState<Arc<Mutex<MockData>>>,
    Path(id): Path<String>,
) -> Json<Value> {
    let d = data.lock().unwrap();
    match d.client_states.get(&id) {
        Some(xdr) => Json(json!({ "client_state_xdr": hex::encode(xdr) })),
        _ => Json(json!({})),
    }
}

async fn consensus_state(
    AxState(data): AxState<Arc<Mutex<MockData>>>,
    Path((id, _height)): Path<(String, u64)>,
) -> Json<Value> {
    let d = data.lock().unwrap();
    match d.consensus_states.get(&id) {
        Some(xdr) => Json(json!({ "consensus_state_xdr": hex::encode(xdr) })),
        _ => Json(json!({})),
    }
}

async fn transfer_balance(
    AxState(_data): AxState<Arc<Mutex<MockData>>>,
    Path((_denom, _address)): Path<(String, String)>,
) -> Json<Value> {
    Json(json!({ "balance": "0" }))
}

pub fn sample_packet() -> ScVal {
    scval_struct(vec![
        ("sequence", ScVal::U64(1)),
        ("data", scval_bytes(b"payload").unwrap()),
    ])
    .unwrap()
}

pub fn ledger_meta_with_write(contract: [u8; 32], key: Vec<u8>, val: Vec<u8>) -> Vec<u8> {
    let entry = ContractDataEntry {
        ext: ExtensionPoint::V0,
        contract: soroban_client::xdr::ScAddress::Contract(ContractId(Hash(contract))),
        key: scval_bytes(&key).unwrap(),
        durability: ContractDataDurability::Persistent,
        val: scval_bytes(&val).unwrap(),
    };
    let ledger_entry = LedgerEntry {
        last_modified_ledger_seq: 0,
        data: LedgerEntryData::ContractData(entry),
        ext: LedgerEntryExt::V0,
    };
    let changes = LedgerEntryChanges(
        vec![LedgerEntryChange::Created(ledger_entry)]
            .try_into()
            .unwrap(),
    );
    let tx_meta = TransactionMeta::V1(TransactionMetaV1 {
        tx_changes: LedgerEntryChanges(VecM::default()),
        operations: vec![OperationMeta { changes }].try_into().unwrap(),
    });
    let tx_result = TransactionResultMeta {
        result: Default::default(),
        fee_processing: LedgerEntryChanges(VecM::default()),
        tx_apply_processing: tx_meta,
    };
    let v0 = LedgerCloseMetaV0 {
        ledger_header: Default::default(),
        tx_set: Default::default(),
        tx_processing: vec![tx_result].try_into().unwrap(),
        upgrades_processing: VecM::default(),
        scp_info: VecM::default(),
    };
    LedgerCloseMeta::V0(v0).to_xdr(Limits::none()).unwrap()
}

pub fn router_event_json(contract: [u8; 32], ledger: u32) -> Value {
    json!({
        "id": format!("{ledger}-0"),
        "ledger": ledger,
        "ledger_closed_at": "1970-01-01T00:00:00Z",
        "contract_id": stellar_strkey::Contract(contract).to_string().as_str(),
        "tx_hash": "00",
        "topics_xdr": [],
        "value_xdr": "",
    })
}

pub fn root_event(contract: [u8; 32], topic: &str, root: [u8; 32]) -> ContractEvent {
    ContractEvent {
        ext: ExtensionPoint::V0,
        contract_id: Some(ContractId(Hash(contract))),
        type_: ContractEventType::Contract,
        body: ContractEventBody::V0(ContractEventV0 {
            topics: vec![ScVal::Symbol(ScSymbol(
                topic.parse::<StringM<32>>().unwrap(),
            ))]
            .try_into()
            .unwrap(),
            data: scval_bytes(&root).unwrap(),
        }),
    }
}

fn success_result(events: &[ContractEvent]) -> TransactionResultPair {
    let preimage = InvokeHostFunctionSuccessPreImage {
        return_value: ScVal::Void,
        events: events.to_vec().try_into().unwrap(),
    };
    let hash: [u8; 32] = Sha256::digest(preimage.to_xdr(Limits::none()).unwrap()).into();

    TransactionResultPair {
        transaction_hash: Hash([0u8; 32]),
        result: TransactionResult {
            fee_charged: 100,
            result: TransactionResultResult::TxSuccess(
                vec![OperationResult::OpInner(
                    OperationResultTr::InvokeHostFunction(InvokeHostFunctionResult::Success(Hash(
                        hash,
                    ))),
                )]
                .try_into()
                .unwrap(),
            ),
            ext: TransactionResultExt::V0,
        },
    }
}

fn classic_result() -> TransactionResultPair {
    TransactionResultPair {
        transaction_hash: Hash([9u8; 32]),
        result: TransactionResult {
            fee_charged: 100,
            result: TransactionResultResult::TxSuccess(VecM::default()),
            ext: TransactionResultExt::V0,
        },
    }
}

pub fn ledger_meta_with_invocations(invocations: &[Option<Vec<ContractEvent>>]) -> Vec<u8> {
    let mut tx_processing = Vec::new();
    for invocation in invocations {
        let (result, meta) = match invocation {
            Some(events) => {
                let meta = TransactionMeta::V4(TransactionMetaV4 {
                    ext: ExtensionPoint::V0,
                    tx_changes_before: LedgerEntryChanges(VecM::default()),
                    operations: vec![OperationMetaV2 {
                        ext: ExtensionPoint::V0,
                        changes: LedgerEntryChanges(VecM::default()),
                        events: events.clone().try_into().unwrap(),
                    }]
                    .try_into()
                    .unwrap(),
                    tx_changes_after: LedgerEntryChanges(VecM::default()),
                    soroban_meta: Some(SorobanTransactionMetaV2 {
                        ext: SorobanTransactionMetaExt::V0,
                        return_value: Some(ScVal::Void),
                    }),
                    events: VecM::default(),
                    diagnostic_events: VecM::default(),
                });
                (success_result(events), meta)
            }
            None => (
                classic_result(),
                TransactionMeta::V1(TransactionMetaV1 {
                    tx_changes: LedgerEntryChanges(VecM::default()),
                    operations: VecM::default(),
                }),
            ),
        };

        tx_processing.push(TransactionResultMetaV1 {
            ext: ExtensionPoint::V0,
            result,
            fee_processing: LedgerEntryChanges(VecM::default()),
            tx_apply_processing: meta,
            post_tx_apply_fee_processing: LedgerEntryChanges(VecM::default()),
        });
    }

    let v2 = LedgerCloseMetaV2 {
        ext: LedgerCloseMetaExt::V0,
        ledger_header: Default::default(),
        tx_set: Default::default(),
        tx_processing: tx_processing.try_into().unwrap(),
        upgrades_processing: VecM::default(),
        scp_info: VecM::default(),
        total_byte_size_of_live_soroban_state: 0,
        evicted_keys: VecM::default(),
    };
    LedgerCloseMeta::V2(v2).to_xdr(Limits::none()).unwrap()
}

pub fn ledger_meta_with_published_write(
    contract: [u8; 32],
    key: Vec<u8>,
    val: Vec<u8>,
    root: [u8; 32],
) -> Vec<u8> {
    let entry = LedgerEntry {
        last_modified_ledger_seq: 0,
        data: LedgerEntryData::ContractData(ContractDataEntry {
            ext: ExtensionPoint::V0,
            contract: soroban_client::xdr::ScAddress::Contract(ContractId(Hash(contract))),
            key: scval_bytes(&key).unwrap(),
            durability: ContractDataDurability::Persistent,
            val: scval_bytes(&val).unwrap(),
        }),
        ext: LedgerEntryExt::V0,
    };
    let events = vec![root_event(contract, "ibc_root", root)];
    let meta = TransactionMeta::V4(TransactionMetaV4 {
        ext: ExtensionPoint::V0,
        tx_changes_before: LedgerEntryChanges(VecM::default()),
        operations: vec![OperationMetaV2 {
            ext: ExtensionPoint::V0,
            changes: LedgerEntryChanges(
                vec![LedgerEntryChange::Created(entry)].try_into().unwrap(),
            ),
            events: events.clone().try_into().unwrap(),
        }]
        .try_into()
        .unwrap(),
        tx_changes_after: LedgerEntryChanges(VecM::default()),
        soroban_meta: Some(SorobanTransactionMetaV2 {
            ext: SorobanTransactionMetaExt::V0,
            return_value: Some(ScVal::Void),
        }),
        events: VecM::default(),
        diagnostic_events: VecM::default(),
    });

    let v2 = LedgerCloseMetaV2 {
        ext: LedgerCloseMetaExt::V0,
        ledger_header: Default::default(),
        tx_set: Default::default(),
        tx_processing: vec![TransactionResultMetaV1 {
            ext: ExtensionPoint::V0,
            result: success_result(&events),
            fee_processing: LedgerEntryChanges(VecM::default()),
            tx_apply_processing: meta,
            post_tx_apply_fee_processing: LedgerEntryChanges(VecM::default()),
        }]
        .try_into()
        .unwrap(),
        upgrades_processing: VecM::default(),
        scp_info: VecM::default(),
        total_byte_size_of_live_soroban_state: 0,
        evicted_keys: VecM::default(),
    };

    LedgerCloseMeta::V2(v2).to_xdr(Limits::none()).unwrap()
}

pub const FIXTURE_CHAIN_ID: &str = "testchain-1";
pub const FIXTURE_LATEST_HEIGHT: u64 = 10;

#[allow(deprecated)]
pub fn tm_client_state_protobuf() -> Vec<u8> {
    let raw = RawTmClientState {
        chain_id: FIXTURE_CHAIN_ID.to_string(),
        trust_level: Some(Fraction {
            numerator: 1,
            denominator: 3,
        }),
        trusting_period: Some(PbDuration {
            seconds: 1_209_600,
            nanos: 0,
        }),
        unbonding_period: Some(PbDuration {
            seconds: 1_814_400,
            nanos: 0,
        }),
        max_clock_drift: Some(PbDuration {
            seconds: 40,
            nanos: 0,
        }),
        frozen_height: Some(RawHeight {
            revision_number: 0,
            revision_height: 0,
        }),
        latest_height: Some(RawHeight {
            revision_number: 0,
            revision_height: FIXTURE_LATEST_HEIGHT,
        }),
        proof_specs: ProofSpecs::cosmos().into(),
        upgrade_path: vec!["upgrade".to_string(), "upgradedIBCState".to_string()],
        allow_update_after_expiry: false,
        allow_update_after_misbehaviour: false,
    };
    let cs = TmClientState::try_from(raw).unwrap();
    Protobuf::<RawTmClientState>::encode_vec(cs)
}

pub fn tm_consensus_state_protobuf() -> Vec<u8> {
    let raw = RawTmConsensusState {
        timestamp: Some(Timestamp {
            seconds: 1_700_000_000,
            nanos: 0,
        }),
        root: Some(MerkleRoot {
            hash: vec![9u8; 32],
        }),
        next_validators_hash: vec![7u8; 32],
    };
    let cons = TmConsensusState::try_from(raw).unwrap();
    Protobuf::<RawTmConsensusState>::encode_vec(cons)
}

fn archive_lookup(
    found: Option<Value>,
    what: &str,
    ledger: u32,
) -> (axum::http::StatusCode, Json<Value>) {
    match found {
        Some(body) => (axum::http::StatusCode::OK, Json(body)),
        None => (
            axum::http::StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("no {what} for ledger {ledger}") })),
        ),
    }
}

async fn cosmos_latest(AxState(data): AxState<Arc<Mutex<MockData>>>) -> Json<Value> {
    let d = data.lock().unwrap();
    Json(json!({ "height": d.cosmos_latest_height }))
}

async fn cosmos_packets(
    AxState(data): AxState<Arc<Mutex<MockData>>>,
    Path(hash): Path<String>,
) -> Json<Value> {
    let d = data.lock().unwrap();
    Json(
        d.cosmos_packets
            .get(&hash)
            .cloned()
            .unwrap_or_else(|| json!({ "events": [] })),
    )
}

async fn cosmos_proof(
    AxState(data): AxState<Arc<Mutex<MockData>>>,
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
) -> Json<Value> {
    let mut d = data.lock().unwrap();

    if let Some(height) = params.get("height").and_then(|h| h.parse::<u64>().ok()) {
        d.cosmos_proof_heights.push(height);
    }

    let key = hex::decode(params.get("key").cloned().unwrap_or_default()).unwrap_or_default();
    let (value, proof) = d.cosmos_proofs.get(&key).cloned().unwrap_or_default();
    Json(json!({ "value": hex::encode(value), "proof": hex::encode(proof) }))
}

async fn cosmos_header(
    AxState(data): AxState<Arc<Mutex<MockData>>>,
    Path(height): Path<u64>,
) -> Json<Value> {
    let d = data.lock().unwrap();
    let header = d.cosmos_headers.get(&height).cloned().unwrap_or_default();
    Json(json!({ "header": hex::encode(header) }))
}

async fn archive_latest(AxState(data): AxState<Arc<Mutex<MockData>>>) -> Json<Value> {
    let d = data.lock().unwrap();
    Json(json!({
        "ledger": d.archive_latest_ledger,
        "archive": d.archive_latest_ledger,
        "watcher": Value::Null,
        "source": "archive",
    }))
}

async fn archive_scp(
    AxState(data): AxState<Arc<Mutex<MockData>>>,
    Path(ledger): Path<u32>,
) -> (axum::http::StatusCode, Json<Value>) {
    let found = data.lock().unwrap().archive_scp.get(&ledger).cloned();
    archive_lookup(found, "SCP messages", ledger)
}

async fn archive_ledger_header(
    AxState(data): AxState<Arc<Mutex<MockData>>>,
    Path(ledger): Path<u32>,
) -> (axum::http::StatusCode, Json<Value>) {
    let found = data.lock().unwrap().archive_headers.get(&ledger).cloned();
    archive_lookup(found, "ledger header", ledger)
}

async fn archive_tx_set(
    AxState(data): AxState<Arc<Mutex<MockData>>>,
    Path(ledger): Path<u32>,
) -> (axum::http::StatusCode, Json<Value>) {
    let found = data.lock().unwrap().archive_tx_sets.get(&ledger).cloned();
    archive_lookup(found, "generalized tx set", ledger)
}
