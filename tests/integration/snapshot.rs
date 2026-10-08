use std::{collections::BTreeMap, sync::Arc};

use ipsa_gateway::{
    api_client::ApiClient,
    snapshot::{Snapshot, SnapshotStore},
    state::State,
};
use ipsa_ibc_core::{commitment::packet_commitment_path, smt::Smt};

use super::mock::{ledger_meta_with_write, router_event_json, start_mock_api, MockData};

const CONTRACT: [u8; 32] = [9u8; 32];

const CLIENT: &[u8] = b"07-tendermint-0";

fn router() -> String {
    format!("{}", stellar_strkey::Contract(CONTRACT))
}

fn write(sequence: u64, byte: u8) -> (Vec<u8>, Vec<u8>) {
    (packet_commitment_path(CLIENT, sequence), vec![byte; 32])
}

fn add_ledger(
    data: &Arc<std::sync::Mutex<MockData>>,
    ledger: u32,
    (key, value): (Vec<u8>, Vec<u8>),
) {
    let mut d = data.lock().unwrap();

    d.ledgers.insert(
        ledger,
        (vec![], Some(ledger_meta_with_write(CONTRACT, key, value))),
    );
    d.events.push(router_event_json(CONTRACT, ledger));
    d.latest_ledger = d.latest_ledger.max(ledger);
}

fn forget_ledgers(data: &Arc<std::sync::Mutex<MockData>>, ledgers: &[u32]) {
    let mut d = data.lock().unwrap();

    for ledger in ledgers {
        d.ledgers.remove(ledger);
    }
}

fn root_of(writes: &[(Vec<u8>, Vec<u8>)]) -> [u8; 32] {
    let mut smt = Smt::new();

    for (key, value) in writes {
        smt.insert(key, value);
    }

    smt.root()
}

fn gateway(api_url: &str, store: &SnapshotStore, genesis: u32) -> State {
    State::new(ApiClient::new(api_url), Some(CONTRACT), Some(router()))
        .with_genesis_ledger(Some(genesis))
        .with_snapshot_store(Some(store.clone()))
}

#[tokio::test]
async fn replaying_writes_a_snapshot_of_the_tree() {
    let (api_url, data) = start_mock_api().await;
    let store = SnapshotStore::memory();
    let (a, b) = (write(1, 0xA1), write(2, 0xB2));

    add_ledger(&data, 10, a.clone());
    add_ledger(&data, 11, b.clone());

    let root = gateway(&api_url, &store, 10).root_at(11).await.unwrap();
    let snapshot = store.load(&router()).await.unwrap().expect("a snapshot");

    assert_eq!(root, root_of(&[a.clone(), b.clone()]));
    assert_eq!(snapshot.last_processed, 11);
    assert_eq!(snapshot.root, root);
    assert_eq!(snapshot.leaves, BTreeMap::from([a, b]));
}

#[tokio::test]
async fn a_restarted_gateway_resumes_from_the_snapshot_when_the_rpc_has_forgotten_genesis() {
    let (api_url, data) = start_mock_api().await;
    let store = SnapshotStore::memory();
    let (a, b, c) = (write(1, 0xA1), write(2, 0xB2), write(3, 0xC3));

    add_ledger(&data, 10, a.clone());
    add_ledger(&data, 11, b.clone());

    gateway(&api_url, &store, 10).root_at(11).await.unwrap();

    forget_ledgers(&data, &[10, 11]);
    add_ledger(&data, 12, c.clone());

    let mut restarted = gateway(&api_url, &store, 10);
    let root = restarted.root_at(12).await.unwrap();

    assert_eq!(root, root_of(&[a, b, c]));
    assert_eq!(restarted.last_processed(), Some(12));
    assert_eq!(
        store.load(&router()).await.unwrap().unwrap().last_processed,
        12
    );
}

#[tokio::test]
async fn a_snapshot_that_does_not_rebuild_to_its_root_is_ignored() {
    let (api_url, data) = start_mock_api().await;
    let store = SnapshotStore::memory();
    let (a, b) = (write(1, 0xA1), write(2, 0xB2));

    add_ledger(&data, 10, a.clone());
    add_ledger(&data, 11, b.clone());

    store
        .save(
            &router(),
            &Snapshot {
                last_processed: 11,
                root: [0xEE; 32],
                leaves: BTreeMap::from([write(7, 0x77)]),
            },
        )
        .await
        .unwrap();

    let root = gateway(&api_url, &store, 10).root_at(11).await.unwrap();

    assert_eq!(root, root_of(&[a, b]));
}

#[tokio::test]
async fn a_gateway_without_a_store_still_replays_from_genesis() {
    let (api_url, data) = start_mock_api().await;
    let (a, b) = (write(1, 0xA1), write(2, 0xB2));

    add_ledger(&data, 10, a.clone());
    add_ledger(&data, 11, b.clone());

    let root = State::new(ApiClient::new(&api_url), Some(CONTRACT), Some(router()))
        .with_genesis_ledger(Some(10))
        .root_at(11)
        .await
        .unwrap();

    assert_eq!(root, root_of(&[a, b]));
}

#[tokio::test]
async fn postgres_round_trips_a_snapshot() {
    let Ok(url) = std::env::var("GATEWAY_TEST_DB_URL") else {
        return;
    };

    let store = SnapshotStore::postgres(url);
    let router = format!("test-{}", std::process::id());
    let first = Snapshot {
        last_processed: 11,
        root: root_of(&[write(1, 0xA1), write(2, 0xB2)]),
        leaves: BTreeMap::from([write(1, 0xA1), write(2, 0xB2)]),
    };
    let second = Snapshot {
        last_processed: 12,
        root: root_of(&[write(2, 0xB2)]),
        leaves: BTreeMap::from([write(2, 0xB2)]),
    };

    assert_eq!(store.load(&router).await.unwrap(), None);

    store.save(&router, &first).await.unwrap();
    assert_eq!(store.load(&router).await.unwrap(), Some(first));

    store.save(&router, &second).await.unwrap();
    assert_eq!(store.load(&router).await.unwrap(), Some(second));
}
