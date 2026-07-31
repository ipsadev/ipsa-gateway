use std::net::SocketAddr;

pub struct GatewayConfig {
    pub host: String,
    pub grpc_port: u16,
    pub api_url: String,
    pub ibc_contract_id: String,
    pub transfer_contract_id: String,
    pub source_account: String,
    pub stellar_chain_id: String,
    pub cosmos_chain_id: String,
    pub event_lookback_ledgers: u32,
    pub cosmos_signer: String,
}

fn env_or(key: &str, default: &str) -> String {
    match std::env::var(key) {
        Ok(value) => {
            let cleaned = value
                .trim()
                .trim_matches(|c| c == '"' || c == '\'')
                .trim()
                .to_string();

            if cleaned.is_empty() {
                default.to_string()
            } else {
                cleaned
            }
        }
        Err(_) => default.to_string(),
    }
}

impl GatewayConfig {
    pub fn from_env() -> Self {
        Self {
            host: env_or("STELLAR_GATEWAY_HOST", "0.0.0.0"),
            grpc_port: env_or("STELLAR_GATEWAY_GRPC_PORT", "50052")
                .parse()
                .unwrap_or(50052),
            api_url: env_or("STELLAR_API_URL", "http://127.0.0.1:8101"),
            ibc_contract_id: env_or("ROUTER_CONTRACT_ADDRESS", ""),
            transfer_contract_id: env_or("TRANSFER_CONTRACT_ADDRESS", ""),
            source_account: env_or("STELLAR_SOURCE_ACCOUNT", ""),
            stellar_chain_id: env_or("STELLAR_CHAIN_ID", "stellar-testnet"),
            cosmos_chain_id: env_or("COSMOS_CHAIN_ID", "simd-1"),
            event_lookback_ledgers: env_or("RELAY_EVENT_LOOKBACK_LEDGERS", "17280")
                .parse()
                .unwrap_or(17280),
            cosmos_signer: env_or("COSMOS_RELAYER_ADDRESS", ""),
        }
    }

    pub fn grpc_addr(&self) -> SocketAddr {
        format!("{}:{}", self.host, self.grpc_port)
            .parse()
            .expect("invalid grpc address")
    }
}
