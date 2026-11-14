use std::{cell::RefCell, collections::BTreeMap};

use ipsa_gateway::chains::conversion as cv;
use ipsa_gateway::seed::{path, root_of, seed, RouterReader, PROBE_BATCH};
use soroban_client::xdr::ScVal;

const COMMITMENT: u8 = 0x01;

const RECEIPT: u8 = 0x02;

const ACK: u8 = 0x03;

struct FakeRouter {
    clients: Vec<String>,
    last_sent: BTreeMap<String, u64>,
    leaves: BTreeMap<Vec<u8>, Vec<u8>>,
    stored_root: RefCell<Vec<[u8; 32]>>,
    ledger: u32,
}

impl FakeRouter {
    fn new(clients: &[&str]) -> Self {
        Self {
            clients: clients.iter().map(|client| client.to_string()).collect(),
            last_sent: BTreeMap::new(),
            leaves: BTreeMap::new(),
            stored_root: RefCell::new(Vec::new()),
            ledger: 5_323_500,
        }
    }

    fn leaf(mut self, client: &str, discriminator: u8, sequence: u64, value: &[u8]) -> Self {
        self.leaves
            .insert(path(client, discriminator, sequence), value.to_vec());
        self
    }

    fn sent(mut self, client: &str, last: u64) -> Self {
        self.last_sent.insert(client.to_string(), last);
        self
    }

    fn honest(self) -> Self {
        let root = root_of(&self.leaves);
        self.stored_root.replace(vec![root]);
        self
    }

    fn roots(self, roots: Vec<[u8; 32]>) -> Self {
        self.stored_root.replace(roots);
        self
    }

    fn root_now(&self) -> [u8; 32] {
        let mut roots = self.stored_root.borrow_mut();

        if roots.len() > 1 {
            roots.remove(0)
        } else {
            roots[0]
        }
    }
}

impl RouterReader for FakeRouter {
    async fn client_ids(&self) -> anyhow::Result<Vec<String>> {
        Ok(self.clients.clone())
    }

    async fn read(&self, keys: &[ScVal]) -> anyhow::Result<(u32, Vec<Option<ScVal>>)> {
        let values = keys
            .iter()
            .map(|key| {
                if let Some(path) = cv::scval_as_bytes(key) {
                    return self
                        .leaves
                        .get(&path)
                        .map(|value| cv::scval_bytes(value).unwrap());
                }

                let ScVal::Vec(Some(items)) = key else {
                    return None;
                };

                match cv::scval_as_symbol(&items[0]).as_deref() {
                    Some("Root") => Some(cv::scval_bytes(&self.root_now()).unwrap()),
                    Some("NextSeqSend") => {
                        let client = cv::scval_as_string(&items[1]).unwrap();

                        self.last_sent.get(&client).map(|last| cv::scval_u64(*last))
                    }
                    _ => None,
                }
            })
            .collect();

        Ok((self.ledger, values))
    }
}

#[tokio::test]
async fn a_seed_reads_every_commitment_receipt_and_acknowledgement() {
    let router = FakeRouter::new(&["07-tendermint-1", "sepolia-0"])
        .sent("sepolia-0", 5)
        .leaf("sepolia-0", COMMITMENT, 5, &[7; 32])
        .leaf("sepolia-0", RECEIPT, 1, &[1])
        .leaf("sepolia-0", ACK, 1, &[9; 32])
        .leaf("07-tendermint-1", RECEIPT, 1, &[1])
        .leaf("07-tendermint-1", ACK, 1, &[3; 32])
        .honest();

    let seeded = seed(&router).await.expect("seed");

    assert_eq!(seeded.leaves, router.leaves);
    assert_eq!(seeded.root, root_of(&router.leaves));
    assert_eq!(seeded.ledger, router.ledger);
}

#[tokio::test]
async fn receipts_past_the_first_probe_batch_are_found() {
    let mut router = FakeRouter::new(&["sepolia-0"]);

    for sequence in 1..=PROBE_BATCH + 6 {
        router = router.leaf("sepolia-0", RECEIPT, sequence, &[1]);
    }

    let router = router.honest();
    let seeded = seed(&router).await.expect("seed");

    assert_eq!(seeded.leaves.len() as u64, PROBE_BATCH + 6);
}

#[tokio::test]
async fn a_root_the_leaves_do_not_rebuild_is_refused() {
    let router = FakeRouter::new(&["sepolia-0"])
        .leaf("sepolia-0", RECEIPT, 1, &[1])
        .roots(vec![[0xab; 32]]);

    assert!(seed(&router).await.is_err());
}

#[tokio::test]
async fn a_root_that_changes_while_reading_is_read_again() {
    let router = FakeRouter::new(&["sepolia-0"]).leaf("sepolia-0", RECEIPT, 1, &[1]);
    let real = root_of(&router.leaves);
    let router = router.roots(vec![[0xcd; 32], real, real]);

    let seeded = seed(&router).await.expect("seed after a retry");

    assert_eq!(seeded.root, real);
}
