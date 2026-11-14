use std::sync::Arc;

use ipsa::core::{
    commitment::packet_commitment_path,
    smt::{verify_membership, Smt},
};
use ipsa_gateway::{
    api_client::ApiClient,
    snapshot::SnapshotStore,
    state::{PathLookup, State},
};

use super::mock::{
    ledger_meta_with_published_write, ledger_meta_with_published_write_and_foreign_root,
    ledger_meta_with_write, router_event_json, start_mock_api, MockData,
};

const CONTRACT: [u8; 32] = [7u8; 32];
const CLIENT: &[u8] = b"sepolia-0";

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

fn root_of(writes: &[(Vec<u8>, Vec<u8>)]) -> [u8; 32] {
    let mut smt = Smt::new();

    for (key, value) in writes {
        smt.insert(key, value);
    }

    smt.root()
}

fn gateway(api_url: &str, genesis: u32) -> State {
    State::new(ApiClient::new(api_url), Some(CONTRACT), Some(router()))
        .with_genesis_ledger(Some(genesis))
}

#[tokio::test]
async fn siblings_at_the_latest_ledger_fold_to_its_root() {
    let (api_url, data) = start_mock_api().await;
    let (a, b) = (write(1, 0xA1), write(2, 0xB2));

    add_ledger(&data, 10, a.clone());
    add_ledger(&data, 11, b.clone());

    let found = gateway(&api_url, 10).siblings_at(11, &a.0).await.unwrap();

    assert_eq!(found.ledger, 11);
    assert_eq!(found.root, root_of(&[a.clone(), b]));
    assert_eq!(found.value, a.1);
    assert_eq!(found.siblings.len(), 64);
    assert!(verify_membership(
        &found.root,
        &a.0,
        &found.value,
        &found.siblings
    ));
}

#[tokio::test]
async fn an_earlier_ledger_is_proved_against_the_tree_as_it_was() {
    let (api_url, data) = start_mock_api().await;
    let (a, b, a_rewritten, c) = (
        write(1, 0xA1),
        write(2, 0xB2),
        write(1, 0xAA),
        write(3, 0xC3),
    );

    add_ledger(&data, 10, a.clone());
    add_ledger(&data, 11, b.clone());
    add_ledger(&data, 12, a_rewritten.clone());
    add_ledger(&data, 13, c.clone());

    let mut state = gateway(&api_url, 10);

    state.root_at(13).await.unwrap();

    let then = state.siblings_at(11, &a.0).await.unwrap();

    assert_eq!(then.root, root_of(&[a.clone(), b.clone()]));
    assert_eq!(
        then.value, a.1,
        "the value as written at ledger 10, not ledger 12's"
    );
    assert!(verify_membership(
        &then.root,
        &a.0,
        &then.value,
        &then.siblings
    ));

    let now = state.siblings_at(13, &a.0).await.unwrap();

    assert_eq!(now.root, root_of(&[a_rewritten.clone(), b, c]));
    assert_eq!(now.value, a_rewritten.1);
    assert!(verify_membership(
        &now.root,
        &a.0,
        &now.value,
        &now.siblings
    ));
}

#[tokio::test]
async fn a_path_written_after_the_ledger_is_not_in_its_tree() {
    let (api_url, data) = start_mock_api().await;
    let (a, b) = (write(1, 0xA1), write(2, 0xB2));

    add_ledger(&data, 10, a.clone());
    add_ledger(&data, 11, b.clone());

    let mut state = gateway(&api_url, 10);

    state.root_at(11).await.unwrap();

    let error = state.siblings_at(10, &b.0).await.unwrap_err();

    assert!(error
        .to_string()
        .contains("is not in the tree at ledger 10"));
}

#[tokio::test]
async fn a_ledger_before_a_restored_snapshot_is_rebuilt_from_the_router_ledgers() {
    let (api_url, data) = start_mock_api().await;
    let store = SnapshotStore::memory();
    let (a, b, c) = (write(1, 0xA1), write(2, 0xB2), write(3, 0xC3));

    add_ledger(&data, 10, a.clone());
    add_ledger(&data, 11, b.clone());

    gateway(&api_url, 10)
        .with_snapshot_store(Some(store.clone()))
        .root_at(11)
        .await
        .unwrap();

    add_ledger(&data, 12, c);

    let mut restarted = gateway(&api_url, 10).with_snapshot_store(Some(store));

    restarted.root_at(12).await.unwrap();

    let mut never_restarted = gateway(&api_url, 10);
    never_restarted.root_at(12).await.unwrap();

    let rewound = restarted.siblings_at(10, &a.0).await.unwrap();
    let expected = never_restarted.siblings_at(10, &a.0).await.unwrap();

    assert_eq!(rewound.root, expected.root);
    assert_eq!(rewound.value, a.1);
    assert!(restarted.siblings_at(11, &a.0).await.is_ok());
}

fn add_published_ledger(
    data: &Arc<std::sync::Mutex<MockData>>,
    ledger: u32,
    (key, value): (Vec<u8>, Vec<u8>),
    root: [u8; 32],
) {
    let mut d = data.lock().unwrap();

    d.ledgers.insert(
        ledger,
        (
            vec![],
            Some(ledger_meta_with_published_write(CONTRACT, key, value, root)),
        ),
    );
    d.events.push(router_event_json(CONTRACT, ledger));
    d.latest_ledger = d.latest_ledger.max(ledger);
}

#[tokio::test]
async fn a_packet_is_proved_at_its_ledger_after_the_follower_has_moved_past_it() {
    let (api_url, data) = start_mock_api().await;
    let (a, b) = (write(1, 0xA1), write(2, 0xB2));
    let root_then = root_of(std::slice::from_ref(&a));
    let root_now = root_of(&[a.clone(), b.clone()]);

    add_published_ledger(&data, 10, a.clone(), root_then);
    add_published_ledger(&data, 12, b.clone(), root_now);

    let mut state = gateway(&api_url, 10);

    state.root_at(12).await.unwrap();

    let PathLookup::Found { proof_bytes, .. } = state.proof_for_path(10, &a.0).await.unwrap()
    else {
        panic!("the packet written at ledger 10 is in its tree");
    };
    let PathLookup::Absent { .. } = state.proof_for_path(10, &b.0).await.unwrap() else {
        panic!("a path written at ledger 12 is not in ledger 10's tree");
    };

    assert!(!proof_bytes.is_empty());
    assert!(matches!(
        state.proof_for_path(12, &b.0).await.unwrap(),
        PathLookup::Found { .. }
    ));
}

#[tokio::test]
async fn a_tree_that_disagrees_with_the_published_root_is_refused() {
    let (api_url, data) = start_mock_api().await;
    let a = write(1, 0xA1);

    add_published_ledger(&data, 10, a.clone(), [0xEE; 32]);

    let mut state = gateway(&api_url, 10);
    let Err(error) = state.proof_for_path(10, &a.0).await else {
        panic!("a tree the router never published is refused");
    };

    assert!(error.to_string().contains("but the router published"));
}

#[tokio::test]
async fn a_foreign_ibc_root_in_the_same_ledger_is_ignored() {
    let (api_url, data) = start_mock_api().await;
    let a = write(1, 0xA1);
    let router_root = root_of(std::slice::from_ref(&a));
    let foreign_contract = [9u8; 32];

    {
        let mut d = data.lock().unwrap();

        d.ledgers.insert(
            10,
            (
                vec![],
                Some(ledger_meta_with_published_write_and_foreign_root(
                    CONTRACT,
                    a.0.clone(),
                    a.1.clone(),
                    router_root,
                    foreign_contract,
                    [0xEE; 32],
                )),
            ),
        );
        d.events.push(router_event_json(CONTRACT, 10));
        d.latest_ledger = d.latest_ledger.max(10);
    }

    let mut state = gateway(&api_url, 10);
    let PathLookup::Found { .. } = state.proof_for_path(10, &a.0).await.unwrap() else {
        panic!(
            "the router published its own root; a foreign contract's later ibc_root must not win"
        );
    };
}
