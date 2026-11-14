use super::mock::test_config;

async fn listener() -> tokio::net::TcpListener {
    tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap()
}

#[tokio::test]
async fn the_gateway_refuses_to_start_without_a_router() {
    let cfg = test_config("http://127.0.0.1:1".to_string(), String::new());

    let error = ipsa_gateway::runner::serve_with_listener(cfg, listener().await)
        .await
        .expect_err("an empty ROUTER_CONTRACT_ADDRESS must stop the gateway from starting");

    assert!(error.to_string().contains("ROUTER_CONTRACT_ADDRESS"));
}

#[tokio::test]
async fn the_gateway_refuses_to_start_with_an_invalid_router() {
    let cfg = test_config(
        "http://127.0.0.1:1".to_string(),
        "not-a-contract".to_string(),
    );

    let error = ipsa_gateway::runner::serve_with_listener(cfg, listener().await)
        .await
        .expect_err("an invalid ROUTER_CONTRACT_ADDRESS must stop the gateway from starting");

    assert!(error.to_string().contains("ROUTER_CONTRACT_ADDRESS"));
}
