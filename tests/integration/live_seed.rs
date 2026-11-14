use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use ipsa_gateway::chains::conversion as cv;
use ipsa_gateway::seed::{seed, RouterReader};
use soroban_client::xdr::ScVal;

const ROUTER: &str = "CBA7QRXNLV2VGXNB7JALC4ZRPD4Q3SFAJRXFS3NTUJBFLLIDT6AEECSS";

const CLIENTS: [&str; 3] = ["07-tendermint-0", "07-tendermint-1", "sepolia-0"];

struct LiveRouter {
    rpc_url: String,
    contract: [u8; 32],
}

impl LiveRouter {
    async fn get_ledger_entries(
        &self,
        keys: &[Vec<u8>],
    ) -> anyhow::Result<(u32, Vec<Option<Vec<u8>>>)> {
        let keys: Vec<String> = keys.iter().map(|key| BASE64.encode(key)).collect();

        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getLedgerEntries",
            "params": {"keys": keys},
        });

        let response: serde_json::Value = reqwest::Client::new()
            .post(&self.rpc_url)
            .json(&body)
            .send()
            .await?
            .json()
            .await?;

        let result = &response["result"];

        let latest_ledger = result["latestLedger"].as_u64().ok_or_else(|| {
            anyhow::anyhow!("getLedgerEntries returned no latestLedger: {response}")
        })?;

        let mut found = std::collections::HashMap::new();

        for entry in result["entries"].as_array().into_iter().flatten() {
            let key = entry["key"].as_str().unwrap_or_default().to_string();
            let data = BASE64.decode(entry["xdr"].as_str().unwrap_or_default())?;

            found.insert(key, data);
        }

        let values = keys.iter().map(|key| found.remove(key)).collect();

        Ok((latest_ledger as u32, values))
    }
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

        let (ledger, entries) = self.get_ledger_entries(&ledger_keys).await?;

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
        rpc_url: url,
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
