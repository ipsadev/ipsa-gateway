use ipsa_chains::{
    conversion::{scval_bytes, scval_string, scval_struct, scval_symbol, scval_to_xdr, scval_vec},
    packet::WRITE_ACK,
};
use ipsa_gateway::{api_client::ApiClient, relay::events::packet_events_in_txs};
use serde_json::json;
use soroban_client::xdr::ScVal;

use super::mock::start_mock_api;

const ROUTER: [u8; 32] = [7u8; 32];
const LEDGER: u32 = 4_160_600;
const CLIENT: &str = "07-tendermint-0";
const RECEIVE_TX: &str = "feedface";
const LONE_ACK_TX: &str = "cafebabe";

fn hex_xdr(value: &ScVal) -> String {
    hex::encode(scval_to_xdr(value).unwrap())
}

fn topics(kind: &str, sequence: u64) -> Vec<String> {
    vec![
        hex_xdr(&scval_symbol(kind).unwrap()),
        hex_xdr(&scval_string(CLIENT).unwrap()),
        hex_xdr(&ScVal::U64(sequence)),
    ]
}

fn packet(sequence: u64) -> ScVal {
    let payload = scval_struct(vec![
        ("source_port", scval_string("transfer").unwrap()),
        ("dest_port", scval_string("transfer").unwrap()),
        ("version", scval_string("ics20-1").unwrap()),
        ("encoding", scval_string("application/json").unwrap()),
        (
            "value",
            scval_bytes(br#"{"denom":"stake","amount":"1000"}"#).unwrap(),
        ),
    ])
    .unwrap();

    scval_struct(vec![
        ("sequence", ScVal::U64(sequence)),
        ("source_client", scval_string("08-wasm-0").unwrap()),
        ("dest_client", scval_string(CLIENT).unwrap()),
        ("timeout_timestamp", ScVal::U64(1_700_000_000)),
        ("payloads", scval_vec(vec![payload]).unwrap()),
    ])
    .unwrap()
}

fn event(tx_hash: &str, kind: &str, sequence: u64, value: ScVal) -> serde_json::Value {
    json!({
        "id": format!("{tx_hash}-{kind}"),
        "ledger": LEDGER,
        "ledger_closed_at": "2026-01-01T00:00:00Z",
        "contract_id": stellar_strkey::Contract(ROUTER).to_string().as_str(),
        "tx_hash": tx_hash,
        "topics_xdr": topics(kind, sequence),
        "value_xdr": hex_xdr(&value),
    })
}

fn acknowledgement_value() -> ScVal {
    scval_struct(vec![(
        "acknowledgements",
        scval_vec(vec![scval_bytes(&[1]).unwrap()]).unwrap(),
    )])
    .unwrap()
}

#[tokio::test]
async fn a_write_ack_without_a_packet_takes_it_from_the_recv_packet_in_its_transaction() {
    let (api_url, data) = start_mock_api().await;

    {
        let mut mock = data.lock().unwrap();

        mock.events.push(event(
            RECEIVE_TX,
            "recv_packet",
            1,
            scval_struct(vec![("packet", packet(1))]).unwrap(),
        ));
        mock.events
            .push(event(RECEIVE_TX, WRITE_ACK, 1, acknowledgement_value()));
    }

    let found = packet_events_in_txs(
        &ApiClient::new(&api_url),
        &stellar_strkey::Contract(ROUTER).to_string(),
        &[RECEIVE_TX.to_string()],
        17_280,
    )
    .await
    .unwrap();

    assert_eq!(found.len(), 1);
    assert_eq!(found[0].kind, WRITE_ACK);
    assert_eq!(found[0].packet.sequence, 1);
    assert_eq!(found[0].packet.dest_client, CLIENT);
    assert_eq!(found[0].acknowledgements, vec![vec![1u8]]);
}

#[tokio::test]
async fn a_write_ack_with_no_recv_packet_beside_it_is_dropped() {
    let (api_url, data) = start_mock_api().await;

    data.lock()
        .unwrap()
        .events
        .push(event(LONE_ACK_TX, WRITE_ACK, 2, acknowledgement_value()));

    let found = packet_events_in_txs(
        &ApiClient::new(&api_url),
        &stellar_strkey::Contract(ROUTER).to_string(),
        &[LONE_ACK_TX.to_string()],
        17_280,
    )
    .await
    .unwrap();

    assert!(found.is_empty());
}

async fn receive_in_mock(
    latest_ledger: u32,
) -> (
    String,
    std::sync::Arc<std::sync::Mutex<super::mock::MockData>>,
) {
    let (api_url, data) = start_mock_api().await;

    {
        let mut mock = data.lock().unwrap();

        mock.latest_ledger = latest_ledger;
        mock.events.push(event(
            RECEIVE_TX,
            "recv_packet",
            1,
            scval_struct(vec![("packet", packet(1))]).unwrap(),
        ));
        mock.events
            .push(event(RECEIVE_TX, WRITE_ACK, 1, acknowledgement_value()));
    }

    (api_url, data)
}

#[tokio::test]
async fn a_recent_transaction_is_found_by_scanning_only_the_recent_ledgers() {
    let latest = LEDGER + 10;
    let (api_url, data) = receive_in_mock(latest).await;

    let found = packet_events_in_txs(
        &ApiClient::new(&api_url),
        &stellar_strkey::Contract(ROUTER).to_string(),
        &[RECEIVE_TX.to_string()],
        17_280,
    )
    .await
    .unwrap();

    assert_eq!(found.len(), 1);
    assert_eq!(data.lock().unwrap().event_start_ledgers, vec![latest - 720]);
}

#[tokio::test]
async fn a_transaction_older_than_the_recent_ledgers_is_found_in_the_full_lookback() {
    let latest = LEDGER + 5_000;
    let (api_url, data) = receive_in_mock(latest).await;

    let found = packet_events_in_txs(
        &ApiClient::new(&api_url),
        &stellar_strkey::Contract(ROUTER).to_string(),
        &[RECEIVE_TX.to_string()],
        17_280,
    )
    .await
    .unwrap();

    assert_eq!(found.len(), 1);
    assert_eq!(
        data.lock().unwrap().event_start_ledgers,
        vec![latest - 720, latest - 17_280]
    );
}

#[tokio::test]
async fn finding_a_transaction_reads_the_latest_sequence_without_the_ledger() {
    let (api_url, data) = receive_in_mock(LEDGER + 10).await;

    packet_events_in_txs(
        &ApiClient::new(&api_url),
        &stellar_strkey::Contract(ROUTER).to_string(),
        &[RECEIVE_TX.to_string()],
        17_280,
    )
    .await
    .unwrap();

    assert_eq!(data.lock().unwrap().full_latest_reads, 0);
}

#[tokio::test]
async fn an_api_without_the_sequence_route_falls_back_to_the_latest_ledger() {
    let (api_url, data) = start_mock_api().await;

    {
        let mut mock = data.lock().unwrap();

        mock.latest_ledger = LEDGER;
        mock.without_sequence_route = true;
    }

    let latest = ApiClient::new(&api_url).get_latest_ledger().await.unwrap();

    assert_eq!(latest, LEDGER);
    assert_eq!(data.lock().unwrap().full_latest_reads, 1);
}
