use anyhow::Result;

use stellar_ibc_core::api_client::ApiClient;
use stellar_ibc_core::ibc::packet::{
    decode_acknowledgements, decode_packet, event_kind, PacketEvent, SEND_PACKET, WRITE_ACK,
};
use stellar_ibc_core::types::{EventCursor, EventRecord};

const PAGE_LIMIT: u32 = 200;
const MAX_PAGES: usize = 50;

fn to_packet_event(record: &EventRecord) -> Option<PacketEvent> {
    let kind = event_kind(&record.topics_xdr)?;

    if kind != SEND_PACKET && kind != WRITE_ACK {
        return None;
    }

    let packet = decode_packet(&record.value_xdr)
        .map_err(|error| tracing::debug!(%error, tx_hash = %record.tx_hash, "packet decode failed"))
        .ok()?;

    let acknowledgements = if kind == WRITE_ACK {
        decode_acknowledgements(&record.value_xdr)
    } else {
        Vec::new()
    };

    Some(PacketEvent {
        kind,
        tx_hash: record.tx_hash.clone(),
        ledger: record.ledger,
        packet,
        acknowledgements,
    })
}

pub async fn packet_events_in_txs(
    api: &ApiClient,
    contract_id: &str,
    tx_hashes: &[String],
    lookback_ledgers: u32,
) -> Result<Vec<PacketEvent>> {
    if tx_hashes.is_empty() {
        return Ok(Vec::new());
    }

    let wanted: Vec<String> = tx_hashes
        .iter()
        .map(|hash| hash.trim_start_matches("0x").to_lowercase())
        .collect();

    let latest = api.get_latest_ledger().await?;
    let start_ledger = latest.saturating_sub(lookback_ledgers).max(1);

    let mut cursor = EventCursor::StartLedger(start_ledger);
    let mut last_cursor = String::new();
    let mut found = Vec::new();

    for _ in 0..MAX_PAGES {
        let page = api
            .get_events(contract_id, cursor, Some(PAGE_LIMIT))
            .await?;

        for record in &page.events {
            if !wanted.contains(&record.tx_hash.to_lowercase()) {
                continue;
            }
            if let Some(event) = to_packet_event(record) {
                found.push(event);
            }
        }

        if page.cursor.is_empty() || page.cursor == last_cursor {
            break;
        }

        last_cursor = page.cursor.clone();
        cursor = EventCursor::Cursor(page.cursor);
    }

    Ok(found)
}
