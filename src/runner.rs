use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::server::Router;
use tonic::transport::Server;

use crate::{config::GatewayConfig, msg::MsgHandler, query::QueryHandler, relay::RelayHandler};
use stellar_ibc_core::{api_client::ApiClient, state::State};

async fn build_router(cfg: &GatewayConfig) -> Router {
    let api = ApiClient::new(&cfg.api_url);

    let ibc_contract_id = if cfg.ibc_contract_id.is_empty() {
        tracing::warn!("ROUTER_CONTRACT_ADDRESS is empty — state state will accept any contract");
        None
    } else {
        match stellar_strkey::Contract::from_string(&cfg.ibc_contract_id) {
            Ok(contract) => Some(contract.0),
            Err(error) => {
                tracing::warn!(%error, "ROUTER_CONTRACT_ADDRESS could not be parsed as a Stellar contract strkey");
                None
            }
        }
    };

    let state = Arc::new(Mutex::new(State::new(api.clone(), ibc_contract_id)));

    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_service_status("", tonic_health::ServingStatus::Serving)
        .await;

    const GATEWAY_FILE_DESCRIPTOR_SET: &[u8] =
        include_bytes!(concat!(env!("OUT_DIR"), "/stellar_gateway_descriptor.bin"));

    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(GATEWAY_FILE_DESCRIPTOR_SET)
        .build_v1()
        .expect("gRPC reflection service failed to build");

    Server::builder()
        .add_service(reflection_service)
        .add_service(health_service)
        .add_service(
            QueryHandler::new(
                api.clone(),
                state,
                Some(cfg.ibc_contract_id.clone()).filter(|s| !s.is_empty()),
                cfg.source_account.clone(),
            )
            .into_server(),
        )
        .add_service(
            MsgHandler::new(
                api.clone(),
                cfg.ibc_contract_id.clone(),
                cfg.transfer_contract_id.clone(),
            )
            .into_server(),
        )
        .add_service(
            RelayHandler::new(
                api.clone(),
                cfg.stellar_chain_id.clone(),
                cfg.cosmos_chain_id.clone(),
                cfg.ibc_contract_id.clone(),
                cfg.event_lookback_ledgers,
                cfg.cosmos_signer.clone(),
            )
            .into_server(),
        )
}

pub async fn serve_with_listener(
    cfg: GatewayConfig,
    listener: TcpListener,
) -> Result<(), tonic::transport::Error> {
    build_router(&cfg)
        .await
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await
}

pub async fn run(cfg: GatewayConfig) {
    tracing::info!(
        grpc_port = cfg.grpc_port,
        api_url = %cfg.api_url,
        router = %cfg.ibc_contract_id,
        "[gateway] starting"
    );

    let grpc_addr = cfg.grpc_addr();
    tracing::info!(%grpc_addr, "[gateway] listening");

    build_router(&cfg)
        .await
        .serve(grpc_addr)
        .await
        .expect("gRPC server failed");
}
