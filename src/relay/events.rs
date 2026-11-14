use anyhow::Result;

use crate::api_client::ApiClient;
use ipsa_chains::packet::{
    decode_acknowledgements, decode_packet, event_kind, Packet, PacketEvent, SEND_PACKET, WRITE_ACK,
};
use ipsa_chains::types::{EventCursor, EventRecord};

const PAGE_LIMIT: u32 = 200;
const MAX_PAGES: usize = 50;
const RECENT_LEDGERS: u32 = 720;

const RECV_PACKET: &str = "recv_packet";

fn to_packet_event(record: &EventRecord, records: &[EventRecord]) -> Option<PacketEvent> {
    let kind = event_kind(&record.topics_xdr)?;

    if kind != SEND_PACKET && kind != WRITE_ACK {
        return None;
    }

    let packet = match decode_packet(&record.value_xdr) {
        Ok(packet) => packet,
        Err(_) if kind == WRITE_ACK => received_packet(record, records)?,
        Err(error) => {
            tracing::debug!(%error, tx_hash = %record.tx_hash, "packet decode failed");

            return None;
        }
    };

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

fn received_packet(acknowledgement: &EventRecord, records: &[EventRecord]) -> Option<Packet> {
    let received = records.iter().find(|record| {
        record.tx_hash == acknowledgement.tx_hash
            && event_kind(&record.topics_xdr).as_deref() == Some(RECV_PACKET)
            && record.topics_xdr.get(1..3) == acknowledgement.topics_xdr.get(1..3)
    });

    let Some(received) = received else {
        tracing::debug!(tx_hash = %acknowledgement.tx_hash, "write_ack has no recv_packet in its transaction");

        return None;
    };

    decode_packet(&received.value_xdr)
        .map_err(|error| tracing::debug!(%error, tx_hash = %received.tx_hash, "recv_packet decode failed"))
        .ok()
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
    let recent = lookback_ledgers.min(RECENT_LEDGERS);

    let mut records = transaction_events(api, contract_id, &wanted, latest, recent).await?;

    let all_found = wanted.iter().all(|hash| {
        records
            .iter()
            .any(|record| record.tx_hash.to_lowercase() == *hash)
    });

    if !all_found && recent < lookback_ledgers {
        records = transaction_events(api, contract_id, &wanted, latest, lookback_ledgers).await?;
    }

    let found = records
        .iter()
        .filter_map(|record| to_packet_event(record, &records))
        .collect();

    Ok(found)
}

async fn transaction_events(
    api: &ApiClient,
    contract_id: &str,
    wanted: &[String],
    latest: u32,
    lookback_ledgers: u32,
) -> Result<Vec<EventRecord>> {
    let start_ledger = latest.saturating_sub(lookback_ledgers).max(1);

    let mut cursor = EventCursor::StartLedger(start_ledger);
    let mut last_cursor = String::new();
    let mut records = Vec::new();

    for _ in 0..MAX_PAGES {
        let mut page = api
            .get_events(contract_id, cursor, Some(PAGE_LIMIT))
            .await?;

        records.extend(
            std::mem::take(&mut page.events)
                .into_iter()
                .filter(|record| wanted.contains(&record.tx_hash.to_lowercase())),
        );

        if page.cursor.is_empty() || page.cursor == last_cursor {
            break;
        }

        last_cursor = page.cursor.clone();
        cursor = EventCursor::Cursor(page.cursor);
    }

    Ok(records)
}
