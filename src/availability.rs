use std::time::Duration;

use tonic::Status;

pub const HEIGHT_AHEAD: &str = "must be less than or equal to the current blockchain height";

pub const HEIGHT_WAIT: Duration = Duration::from_secs(1);

pub const HEIGHT_ATTEMPTS: u32 = 20;

pub fn stellar_evidence_pending(detail: &str) -> bool {
    detail.contains("/archive/") && detail.contains("404")
}

pub fn cosmos_height_pending(detail: &str) -> bool {
    detail.contains(HEIGHT_AHEAD)
}

pub fn header_assembly_status(error: &anyhow::Error) -> Status {
    let detail = format!("{error:#}");

    if stellar_evidence_pending(&detail) {
        Status::unavailable(format!(
            "the ledger's consensus evidence is not available yet: the watcher does not hold it and \
             the history archive has not published its checkpoint ({detail})"
        ))
    } else {
        Status::internal(format!("header assembly failed: {detail}"))
    }
}

pub fn cosmos_header_status(error: &anyhow::Error) -> Status {
    let detail = format!("{error:#}");

    if cosmos_height_pending(&detail) {
        Status::unavailable(format!(
            "the Cosmos Hub has not produced the block yet: {detail}"
        ))
    } else {
        Status::internal(format!("cosmos header failed: {detail}"))
    }
}
