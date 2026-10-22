use ipsa_api::rpc::RpcClient;
use ipsa_chains::conversion as cv;
use ipsa_gateway::seed::{seed, RouterReader};
use soroban_client::xdr::ScVal;

const ROUTER: &str = "CBA7QRXNLV2VGXNB7JALC4ZRPD4Q3SFAJRXFS3NTUJBFLLIDT6AEECSS";

const CLIENTS: [&str; 4] = [
    "07-tendermint-0",
    "07-tendermint-1",
    "sepolia-0",
    "avalanche-0",
];

struct LiveRouter {
    rpc: RpcClient,
    contract: [u8; 32],
}

impl RouterReader for LiveRouter {
    async fn client_ids(&self) -> anyhow::Result<Vec<String>> {
        Ok(CLIENTS.iter().map(|client| client.to_string()).collect())
    }

    async fn read(&self, keys: &[ScVal]) -> anyhow::Result<(u32, Vec<Option<ScVal>>)> {
        let ledger_keys = keys
            .iter()
            .map(|key| cv::persistent_contract_data_key(self.contract, key.clone()))
            .collect::<anyhow::Result<Vec<_>>>()?;

        let (ledger, entries) = self.rpc.get_ledger_entries(&ledger_keys).await?;

        let values = entries
            .into_iter()
            .map(|entry| entry.and_then(|data| cv::ledger_entry_contract_val(&data)))
            .collect();

        Ok((ledger, values))
    }
}

#[tokio::test]
async fn the_live_router_is_seeded_from_its_storage_to_its_own_root() {
    let Ok(url) = std::env::var("STELLAR_LIVE_RPC_URL") else {
        return;
    };

    let router = LiveRouter {
        rpc: RpcClient::new(&url).expect("rpc client"),
        contract: stellar_strkey::Contract::from_string(ROUTER)
            .expect("router")
            .0,
    };

    let seeded = seed(&router)
        .await
        .expect("the router's storage rebuilds its stored root");

    assert!(!seeded.leaves.is_empty());
    println!(
        "seeded {} leaves at ledger {} with root {}",
        seeded.leaves.len(),
        seeded.ledger,
        hex::encode(seeded.root)
    );
}

#[tokio::test]
async fn a_seeded_gateway_proves_a_packet_sent_before_the_seed() {
    let Ok(api_url) = std::env::var("IPSA_LIVE_API_URL") else {
        return;
    };

    let contract = stellar_strkey::Contract::from_string(ROUTER)
        .expect("router")
        .0;
    let mut state = ipsa_gateway::state::State::new(
        ipsa_gateway::api_client::ApiClient::new(&api_url),
        Some(contract),
        Some(ROUTER.to_string()),
    )
    .with_snapshot_store(Some(ipsa_gateway::snapshot::SnapshotStore::memory()));

    let commitment = ipsa_gateway::seed::path("sepolia-0", 0x01, 5);
    let siblings = state
        .siblings_at(5_323_419, &commitment)
        .await
        .expect("the tree at the ledger packet 5 was sent, checked against the published root");

    assert_eq!(siblings.ledger, 5_323_419);
    assert_eq!(siblings.value.len(), 32);
    println!("root at 5323419: {}", hex::encode(siblings.root));
}
