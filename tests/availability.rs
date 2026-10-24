use ipsa_gateway::availability::{
    cosmos_header_status, cosmos_height_pending, header_assembly_status, stellar_evidence_pending,
};
use tonic::Code;

const ARCHIVE_NOT_PUBLISHED: &str = r#"GET /archive/scp/5354244 returned 502 Bad Gateway: {"error":"GET https://history.stellar.org/prd/core-testnet/core_testnet_001/scp/00/51/b3/scp-0051b33f.xdr.gz returned HTTP 404"}"#;

const HUB_BLOCK_AHEAD: &str = r#"GET /cosmos/header/19552131?trusted_height=19551883 returned 502 Bad Gateway: {"error":"GET https://rpc.provider-sentry-01.ics-testnet.polypore.xyz/commit?height=19552131 returned 500 Internal Server Error: {\"error\":{\"code\":-32603,\"data\":\"height 19552131 must be less than or equal to the current blockchain height 19552130\",\"message\":\"Internal error\"},\"id\":-1,\"jsonrpc\":\"2.0\"}"}"#;

#[test]
fn a_ledger_the_archive_has_not_published_is_not_ready_yet() {
    assert!(stellar_evidence_pending(ARCHIVE_NOT_PUBLISHED));

    let status = header_assembly_status(&anyhow::anyhow!(ARCHIVE_NOT_PUBLISHED));

    assert_eq!(status.code(), Code::Unavailable);
    assert!(status.message().contains("not available yet"));
}

#[test]
fn other_header_failures_stay_errors() {
    let broken =
        "GET /archive/ledger-header/5354244 returned 500 Internal Server Error: decode failed";

    assert!(!stellar_evidence_pending(broken));
    assert_eq!(
        header_assembly_status(&anyhow::anyhow!(broken)).code(),
        Code::Internal
    );
}

#[test]
fn a_hub_block_not_produced_yet_is_not_ready_yet() {
    assert!(cosmos_height_pending(HUB_BLOCK_AHEAD));
    assert!(!cosmos_height_pending(
        "GET /cosmos/header/1 returned 404 Not Found"
    ));
    assert_eq!(
        cosmos_header_status(&anyhow::anyhow!(HUB_BLOCK_AHEAD)).code(),
        Code::Unavailable
    );
    assert_eq!(
        cosmos_header_status(&anyhow::anyhow!("connection refused")).code(),
        Code::Internal
    );
}
