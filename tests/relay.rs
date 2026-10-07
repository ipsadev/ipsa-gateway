use ipsa_chains::packet::{Packet, PacketEvent, SEND_PACKET, WRITE_ACK};
use ipsa_gateway::relay::{filter_sequences, proof_ledger};

fn event(kind: &str, sequence: u64, ledger: u32) -> PacketEvent {
    PacketEvent {
        kind: kind.to_string(),
        tx_hash: String::new(),
        ledger,
        acknowledgements: Vec::new(),
        packet: Packet {
            sequence,
            source_client: "08-wasm-0".to_string(),
            dest_client: "07-tendermint-0".to_string(),
            timeout_timestamp: 0,
            payloads: Vec::new(),
        },
    }
}

#[test]
fn empty_filters_keep_every_packet_event() {
    let found = vec![event(SEND_PACKET, 1, 10), event(WRITE_ACK, 2, 11)];
    let kept = filter_sequences(found, &[], &[]);

    assert_eq!(kept.len(), 2);
}

#[test]
fn send_packets_filter_on_source_sequences() {
    let found = vec![event(SEND_PACKET, 1, 10), event(SEND_PACKET, 2, 11)];
    let kept = filter_sequences(found, &[2], &[]);

    assert_eq!(kept.len(), 1);
    assert_eq!(kept[0].packet.sequence, 2);
}

#[test]
fn write_acks_filter_on_destination_sequences() {
    let found = vec![event(WRITE_ACK, 1, 10), event(WRITE_ACK, 2, 11)];
    let kept = filter_sequences(found, &[], &[1]);

    assert_eq!(kept.len(), 1);
    assert_eq!(kept[0].packet.sequence, 1);
}

#[test]
fn a_send_sequence_filter_does_not_drop_write_acks() {
    let found = vec![event(SEND_PACKET, 1, 10), event(WRITE_ACK, 9, 11)];
    let kept = filter_sequences(found, &[1], &[]);

    assert_eq!(kept.len(), 2);
}

#[test]
fn unrelated_event_kinds_are_dropped() {
    let found = vec![event("recv_packet", 1, 10)];

    assert!(filter_sequences(found, &[], &[]).is_empty());
}

#[test]
fn proof_ledger_is_the_highest_event_ledger() {
    let found = vec![event(SEND_PACKET, 1, 10), event(SEND_PACKET, 2, 44)];

    assert_eq!(proof_ledger(&found), Some(44));
}

#[test]
fn proof_ledger_is_absent_without_events() {
    assert_eq!(proof_ledger(&[]), None);
}
