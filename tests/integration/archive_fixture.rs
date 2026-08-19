use std::collections::HashMap;

fn frame(payload: &[u8]) -> Vec<u8> {
    let mut out = ((payload.len() as u32) | 0x8000_0000)
        .to_be_bytes()
        .to_vec();
    out.extend_from_slice(payload);
    out
}

fn u32be(v: u32) -> [u8; 4] {
    v.to_be_bytes()
}

fn var_bytes(data: &[u8]) -> Vec<u8> {
    let mut out = u32be(data.len() as u32).to_vec();
    out.extend_from_slice(data);
    out.extend(std::iter::repeat_n(0u8, (4 - (data.len() % 4)) % 4));
    out
}

fn node_id(public: &[u8; 32]) -> Vec<u8> {
    let mut out = u32be(0).to_vec();
    out.extend_from_slice(public);
    out
}

pub fn quorum_set(validators: &[[u8; 32]], threshold: u32) -> Vec<u8> {
    let mut out = u32be(threshold).to_vec();
    out.extend_from_slice(&u32be(validators.len() as u32));
    for v in validators {
        out.extend_from_slice(&node_id(v));
    }
    out.extend_from_slice(&u32be(0));
    out
}

pub fn externalize_envelope(
    node: &[u8; 32],
    slot: u64,
    value: &[u8],
    quorum_set_hash: [u8; 32],
) -> Vec<u8> {
    let mut statement = node_id(node);
    statement.extend_from_slice(&slot.to_be_bytes());
    statement.extend_from_slice(&u32be(2));
    statement.extend_from_slice(&u32be(1));
    statement.extend_from_slice(&var_bytes(value));
    statement.extend_from_slice(&u32be(1));
    statement.extend_from_slice(&quorum_set_hash);

    let mut out = statement;
    out.extend_from_slice(&var_bytes(&[0u8; 64]));
    out
}

pub fn scp_file(ledger: u32, quorum_sets: &[Vec<u8>], envelopes: &[Vec<u8>]) -> Vec<u8> {
    let mut entry = u32be(0).to_vec();
    entry.extend_from_slice(&u32be(quorum_sets.len() as u32));
    for q in quorum_sets {
        entry.extend_from_slice(q);
    }
    entry.extend_from_slice(&u32be(ledger));
    entry.extend_from_slice(&u32be(envelopes.len() as u32));
    for e in envelopes {
        entry.extend_from_slice(e);
    }
    frame(&entry)
}

pub fn ledger_header(ledger: u32, close_time: u64) -> Vec<u8> {
    let mut out = u32be(23).to_vec();
    out.extend_from_slice(&[0u8; 32]);
    out.extend_from_slice(&[7u8; 32]);
    out.extend_from_slice(&close_time.to_be_bytes());
    out.extend_from_slice(&u32be(0));
    out.extend_from_slice(&u32be(0));
    out.extend_from_slice(&[9u8; 32]);
    out.extend_from_slice(&[2u8; 32]);
    out.extend_from_slice(&u32be(ledger));
    out.extend_from_slice(&0u64.to_be_bytes());
    out.extend_from_slice(&0u64.to_be_bytes());
    out.extend_from_slice(&u32be(0));
    out.extend_from_slice(&0u64.to_be_bytes());
    out.extend_from_slice(&u32be(100));
    out.extend_from_slice(&u32be(5_000_000));
    out.extend_from_slice(&u32be(1000));
    for _ in 0..4 {
        out.extend_from_slice(&[0u8; 32]);
    }
    out.extend_from_slice(&u32be(0));
    out
}

pub fn ledger_file(ledger: u32, close_time: u64) -> (Vec<u8>, [u8; 32]) {
    use sha2::{Digest, Sha256};

    let header = ledger_header(ledger, close_time);
    let hash: [u8; 32] = Sha256::digest(&header).into();

    let mut entry = hash.to_vec();
    entry.extend_from_slice(&header);
    entry.extend_from_slice(&u32be(0));

    (frame(&entry), hash)
}

pub fn generalized_tx_set(previous_ledger_hash: [u8; 32]) -> Vec<u8> {
    let mut out = u32be(1).to_vec();
    out.extend_from_slice(&previous_ledger_hash);
    out.extend_from_slice(&u32be(0));
    out
}

pub fn transactions_file(ledger: u32, previous_ledger_hash: [u8; 32]) -> Vec<u8> {
    let mut entry = u32be(ledger).to_vec();
    entry.extend_from_slice(&[0u8; 32]);
    entry.extend_from_slice(&u32be(0));
    entry.extend_from_slice(&u32be(1));
    entry.extend_from_slice(&generalized_tx_set(previous_ledger_hash));
    frame(&entry)
}

pub struct HeaderArchive {
    pub files: HashMap<(String, u32), Vec<u8>>,
    pub ledger_hash: [u8; 32],
    pub quorum_set_xdr: Vec<u8>,
    pub envelope_count: usize,
}

pub fn header_archive(ledger: u32) -> HeaderArchive {
    use sha2::{Digest, Sha256};
    use stellar_ibc_core::stellar::archive::checkpoint_for;

    let validators: Vec<[u8; 32]> = (1..=4u8).map(|i| [i; 32]).collect();
    let qset = quorum_set(&validators, 3);
    let qset_hash: [u8; 32] = Sha256::digest(&qset).into();

    let envelopes = |slot: u64| -> Vec<Vec<u8>> {
        validators
            .iter()
            .map(|v| externalize_envelope(v, slot, b"agreed-value", qset_hash))
            .collect()
    };

    let mut scp = scp_file(
        ledger,
        std::slice::from_ref(&qset),
        &envelopes(ledger as u64),
    );
    let next_scp = scp_file(
        ledger + 1,
        std::slice::from_ref(&qset),
        &envelopes(ledger as u64 + 1),
    );
    let same_checkpoint = checkpoint_for(ledger) == checkpoint_for(ledger + 1);
    if same_checkpoint {
        scp.extend_from_slice(&next_scp);
    }

    let (ledger_bytes, ledger_hash) = ledger_file(ledger, 1_700_000_000);
    let tx_bytes = transactions_file(ledger + 1, ledger_hash);

    let mut files = HashMap::new();
    files.insert(("scp".to_string(), checkpoint_for(ledger)), scp);
    if !same_checkpoint {
        files.insert(("scp".to_string(), checkpoint_for(ledger + 1)), next_scp);
    }
    files.insert(("ledger".to_string(), checkpoint_for(ledger)), ledger_bytes);
    files.insert(
        ("transactions".to_string(), checkpoint_for(ledger + 1)),
        tx_bytes,
    );

    HeaderArchive {
        files,
        ledger_hash,
        quorum_set_xdr: qset,
        envelope_count: validators.len(),
    }
}

pub struct ArchiveResponses {
    pub scp: HashMap<u32, serde_json::Value>,
    pub headers: HashMap<u32, serde_json::Value>,
    pub tx_sets: HashMap<u32, serde_json::Value>,
    pub ledger_hash: [u8; 32],
    pub envelope_count: usize,
}

pub fn archive_responses(ledger: u32) -> ArchiveResponses {
    use sha2::{Digest, Sha256};

    let validators: Vec<[u8; 32]> = (1..=4u8).map(|i| [i; 32]).collect();
    let qset = quorum_set(&validators, 3);
    let qset_hash: [u8; 32] = Sha256::digest(&qset).into();

    let envelopes = |slot: u64| -> Vec<String> {
        validators
            .iter()
            .map(|v| hex::encode(externalize_envelope(v, slot, b"agreed-value", qset_hash)))
            .collect()
    };
    let scp_body = |slot: u32| {
        serde_json::json!({
            "ledger": slot,
            "quorum_sets_xdr": [hex::encode(&qset)],
            "envelopes_xdr": envelopes(slot as u64),
        })
    };

    let header = ledger_header(ledger, 1_700_000_000);
    let ledger_hash: [u8; 32] = Sha256::digest(&header).into();

    let mut scp = HashMap::new();
    scp.insert(ledger, scp_body(ledger));
    scp.insert(ledger + 1, scp_body(ledger + 1));

    let mut headers = HashMap::new();
    headers.insert(
        ledger,
        serde_json::json!({
            "ledger": ledger,
            "header_xdr": hex::encode(&header),
            "hash": hex::encode(ledger_hash),
        }),
    );

    let mut tx_sets = HashMap::new();
    tx_sets.insert(
        ledger + 1,
        serde_json::json!({
            "ledger": ledger + 1,
            "generalized_tx_set_xdr": hex::encode(generalized_tx_set(ledger_hash)),
        }),
    );

    ArchiveResponses {
        scp,
        headers,
        tx_sets,
        ledger_hash,
        envelope_count: validators.len(),
    }
}
