use std::collections::{BTreeMap, HashMap, VecDeque};

use ipsa::core::{
    proof::{serialize_membership_proof, serialize_non_membership_proof},
    smt::{ProofError, Smt},
};
use ipsa_chains::{
    conversion::{scval_as_bytes, scval_to_v2_path},
    types::{EventCursor, LedgerData},
};
use soroban_client::xdr::{
    ContractEvent, ContractEventBody, LedgerCloseMeta, LedgerEntryChange, LedgerEntryData,
    LedgerKey, Limits, ReadXdr, ScAddress, ScVal, TransactionMeta,
};

use crate::{
    api_client::ApiClient,
    seed::{self, RouterReader},
    snapshot::{Snapshot, SnapshotStore},
};

pub enum PathLookup {
    Found {
        value_hash: [u8; 32],
        proof_bytes: Vec<u8>,
    },
    Absent {
        proof_bytes: Vec<u8>,
    },
}

pub const JOURNAL_LEDGERS: u32 = 34_560;

const CONCURRENT_LEDGER_FETCHES: usize = 8;

pub const SEED_CLIENT_TYPES: [&str; 3] = ["07-tendermint", "08-wasm", "sepolia"];

struct ApiReader<'a> {
    api: &'a ApiClient,
    router: &'a str,
}

impl RouterReader for ApiReader<'_> {
    async fn client_ids(&self) -> anyhow::Result<Vec<String>> {
        let mut ids = self.api.list_client_ids(self.router).await?;

        for client_type in SEED_CLIENT_TYPES {
            ids.extend(
                self.api
                    .list_client_ids_of_type(self.router, client_type)
                    .await?,
            );
        }

        ids.sort();
        ids.dedup();

        Ok(ids)
    }

    async fn read(&self, keys: &[ScVal]) -> anyhow::Result<(u32, Vec<Option<ScVal>>)> {
        self.api.contract_data(self.router, keys).await
    }
}

#[derive(Clone, Debug)]
pub struct TreeSiblings {
    pub ledger: u32,
    pub root: [u8; 32],
    pub value: Vec<u8>,
    pub siblings: Vec<[u8; 32]>,
}

struct JournalEntry {
    ledger: u32,
    path: Vec<u8>,
    previous: Option<Vec<u8>>,
}

fn cursor_ledger(cursor: &str) -> Option<u32> {
    let toid: u64 = cursor.split('-').next()?.parse().ok()?;
    u32::try_from(toid >> 32).ok()
}

pub struct State {
    api: ApiClient,
    roots: HashMap<u32, [u8; 32]>,
    ibc_contract_id: Option<[u8; 32]>,
    router_address: Option<String>,
    smt: Smt,
    last_processed: Option<u32>,
    genesis_ledger: Option<u32>,
    published: HashMap<u32, [u8; 32]>,
    leaves: BTreeMap<Vec<u8>, Vec<u8>>,
    store: Option<SnapshotStore>,
    restore_attempted: bool,
    journal: VecDeque<JournalEntry>,
    journal_floor: Option<u32>,
    applying: u32,
}

impl State {
    pub fn new(
        api: ApiClient,
        ibc_contract_id: Option<[u8; 32]>,
        router_address: Option<String>,
    ) -> Self {
        Self {
            api,
            roots: HashMap::new(),
            ibc_contract_id,
            router_address,
            smt: Smt::new(),
            last_processed: None,
            genesis_ledger: None,
            published: HashMap::new(),
            leaves: BTreeMap::new(),
            store: None,
            restore_attempted: false,
            journal: VecDeque::new(),
            journal_floor: None,
            applying: 0,
        }
    }

    pub fn with_snapshot_store(mut self, store: Option<SnapshotStore>) -> Self {
        self.store = store;
        self
    }

    pub fn last_processed(&self) -> Option<u32> {
        self.last_processed
    }

    pub fn with_genesis_ledger(mut self, genesis: Option<u32>) -> Self {
        self.genesis_ledger = genesis;
        self
    }

    pub fn genesis_ledger(&self) -> Option<u32> {
        self.genesis_ledger
    }

    pub async fn root_at(&mut self, seq: u32) -> anyhow::Result<[u8; 32]> {
        if let Some(&root) = self.roots.get(&seq) {
            return Ok(root);
        }
        self.process_through(seq).await
    }

    pub async fn proof_for_path(&mut self, seq: u32, key: &[u8]) -> anyhow::Result<PathLookup> {
        let (tree, _) = self.tree_at(seq).await?;

        match tree.generate_membership_proof(key) {
            Ok(proof) => Ok(PathLookup::Found {
                value_hash: proof.value_hash,
                proof_bytes: serialize_membership_proof(&proof),
            }),
            Err(ProofError::KeyAbsent) => {
                let proof = tree.generate_non_membership_proof(key)?;

                Ok(PathLookup::Absent {
                    proof_bytes: serialize_non_membership_proof(&proof),
                })
            }
            Err(error) => Err(error.into()),
        }
    }

    pub async fn siblings_at(&mut self, seq: u32, key: &[u8]) -> anyhow::Result<TreeSiblings> {
        let (tree, leaves) = self.tree_at(seq).await?;

        let value = leaves.get(key).cloned().ok_or_else(|| {
            anyhow::anyhow!(
                "path {} is not in the tree at ledger {seq}",
                hex::encode(key)
            )
        })?;
        let proof = tree.generate_membership_proof(key)?;

        Ok(TreeSiblings {
            ledger: seq,
            root: tree.root(),
            value,
            siblings: proof.siblings,
        })
    }

    async fn tree_at(&mut self, seq: u32) -> anyhow::Result<(Smt, BTreeMap<Vec<u8>, Vec<u8>>)> {
        self.root_at(seq).await?;

        let floor = self.journal_floor.or(self.last_processed).unwrap_or(seq);

        if seq < floor {
            anyhow::bail!(
                "ledger {seq} is older than the gateway's write journal, which starts after \
                 ledger {floor}; the tree as it was then cannot be rebuilt"
            );
        }

        let mut leaves = self.leaves.clone();

        for entry in self.journal.iter().rev().filter(|entry| entry.ledger > seq) {
            match &entry.previous {
                Some(value) => leaves.insert(entry.path.clone(), value.clone()),
                None => leaves.remove(&entry.path),
            };
        }

        let mut tree = Smt::new();

        for (path, value) in &leaves {
            tree.insert(path, value);
        }

        let root = tree.root();

        if let Some(published) = self.published.get(&seq).copied() {
            if published != root {
                self.reseed().await;

                anyhow::bail!(
                    "the tree rebuilt at ledger {seq} has root {} but the router published {}; \
                     the gateway is seeding its tree again from the router's storage",
                    hex::encode(root),
                    hex::encode(published)
                );
            }
        }

        Ok((tree, leaves))
    }

    fn record(&mut self, path: &[u8]) {
        self.journal.push_back(JournalEntry {
            ledger: self.applying,
            path: path.to_vec(),
            previous: self.leaves.get(path).cloned(),
        });
    }

    fn prune_journal(&mut self, seq: u32) {
        let Some(horizon) = seq.checked_sub(JOURNAL_LEDGERS) else {
            return;
        };

        while self
            .journal
            .front()
            .is_some_and(|entry| entry.ledger <= horizon)
        {
            self.journal.pop_front();
        }

        self.journal_floor = Some(
            self.journal_floor
                .map_or(horizon, |floor| floor.max(horizon)),
        );
    }

    async fn process_through(&mut self, seq: u32) -> anyhow::Result<[u8; 32]> {
        if self.last_processed.is_none() {
            self.restore().await;
        }

        let before = self.last_processed;
        let root = self.replay_through(seq).await?;

        if self.last_processed != before {
            self.persist().await;
        }

        Ok(root)
    }

    async fn restore(&mut self) {
        if self.restore_attempted {
            return;
        }

        self.restore_attempted = true;

        let Some(router) = self.router_address.clone() else {
            return;
        };

        if let Some(store) = self.store.clone() {
            match store.load(&router).await {
                Ok(Some(snapshot)) => self.restore_snapshot(&router, snapshot),
                Ok(None) => tracing::info!(router, "no gateway snapshot yet"),
                Err(error) => tracing::warn!(%error, router, "could not read the gateway snapshot"),
            }
        }

        self.seed_from_router(&router).await;

        if let Some(last) = self.last_processed {
            self.rebuild_journal(last).await;
        }
    }

    async fn rebuild_journal(&mut self, through: u32) {
        let floor = through.saturating_sub(JOURNAL_LEDGERS);

        let Some(ledgers) = self
            .ledgers_with_router_activity(floor.saturating_add(1), through)
            .await
        else {
            tracing::warn!(
                floor,
                through,
                "could not list the router's recent activity — proofs for ledgers before {through} will be refused"
            );

            return;
        };

        let mut journal = VecDeque::new();

        for seq in &ledgers {
            let meta = match self.ledger_meta(*seq).await {
                Ok(Some(meta)) => meta,
                Ok(None) => continue,
                Err(error) => {
                    tracing::warn!(%error, ledger = seq, "could not read a ledger while rebuilding the journal");

                    return;
                }
            };

            let mut prior: HashMap<Vec<u8>, Option<Vec<u8>>> = HashMap::new();

            for change in ledger_changes(&meta) {
                match change {
                    LedgerEntryChange::State(entry) | LedgerEntryChange::Restored(entry) => {
                        if let Some((path, value)) = self.provable(&entry.data) {
                            prior.insert(path, Some(value));
                        }
                    }
                    LedgerEntryChange::Created(entry) | LedgerEntryChange::Updated(entry) => {
                        if let Some((path, value)) = self.provable(&entry.data) {
                            let previous = prior.remove(&path).flatten();

                            journal.push_back(JournalEntry {
                                ledger: *seq,
                                path: path.clone(),
                                previous,
                            });
                            prior.insert(path, Some(value));
                        }
                    }
                    LedgerEntryChange::Removed(LedgerKey::ContractData(key)) => {
                        if !self.matches(&key.contract) {
                            continue;
                        }

                        if let Some(path) = scval_to_v2_path(&key.key) {
                            let previous = prior.remove(&path).flatten();

                            journal.push_back(JournalEntry {
                                ledger: *seq,
                                path: path.clone(),
                                previous,
                            });
                            prior.insert(path, None);
                        }
                    }
                    _ => {}
                }
            }

            if let Some(root) = published_root(&meta, self.ibc_contract_id.as_ref()) {
                self.published.insert(*seq, root);
            }
        }

        tracing::info!(
            floor,
            through,
            ledgers = ledgers.len(),
            writes = journal.len(),
            "rebuilt the write journal from the router's recent ledgers"
        );

        self.journal = journal;
        self.journal_floor = Some(floor);
    }

    async fn ledger_meta(&self, seq: u32) -> anyhow::Result<Option<LedgerCloseMeta>> {
        let ledger = self.api.get_ledger(seq).await?;

        ledger
            .metadata_xdr
            .map(|meta_xdr| {
                LedgerCloseMeta::from_xdr(&meta_xdr, Limits::none())
                    .map_err(|e| anyhow::anyhow!("LedgerCloseMeta XDR decode: {e}"))
            })
            .transpose()
    }

    fn provable(&self, data: &LedgerEntryData) -> Option<(Vec<u8>, Vec<u8>)> {
        let LedgerEntryData::ContractData(d) = data else {
            return None;
        };

        if !self.matches(&d.contract) {
            return None;
        }

        Some((scval_to_v2_path(&d.key)?, scval_as_bytes(&d.val)?))
    }

    pub async fn heal(&mut self, seq: u32) {
        if self.check_published_root(seq).is_err() {
            self.reseed().await;
        }
    }

    async fn reseed(&mut self) {
        let Some(router) = self.router_address.clone() else {
            return;
        };

        tracing::warn!(
            router,
            last_processed = ?self.last_processed,
            "the state tree disagrees with the root the router published — seeding it again from the router's storage"
        );

        self.seed_from_router(&router).await;

        if let Some(last) = self.last_processed {
            self.rebuild_journal(last).await;
        }
    }

    async fn seed_from_router(&mut self, router: &str) {
        tracing::info!(
            router,
            last_processed = ?self.last_processed,
            "seeding the state tree from the router's storage, which a saved snapshot or a replay can fall behind"
        );

        let reader = ApiReader {
            api: &self.api,
            router,
        };

        let seeded = match seed::seed(&reader).await {
            Ok(seeded) => seeded,
            Err(error) => {
                tracing::warn!(%error, router, "could not seed the state tree from the router's storage");

                return;
            }
        };

        let mut smt = Smt::new();

        for (path, value) in &seeded.leaves {
            smt.insert(path, value);
        }

        tracing::info!(
            router,
            ledger = seeded.ledger,
            leaves = seeded.leaves.len(),
            root = %hex::encode(seeded.root),
            "seeded the state tree from the router's storage; it rebuilds the router's stored root"
        );

        self.smt = smt;
        self.leaves = seeded.leaves;
        self.last_processed = Some(seeded.ledger);
        self.journal.clear();
        self.journal_floor = Some(seeded.ledger);
        self.roots.clear();
        self.roots.insert(seeded.ledger, seeded.root);
        self.persist().await;
    }

    fn restore_snapshot(&mut self, router: &str, snapshot: Snapshot) {
        let mut smt = Smt::new();

        for (path, value) in &snapshot.leaves {
            smt.insert(path, value);
        }

        if smt.root() != snapshot.root {
            tracing::warn!(
                router,
                stored = %hex::encode(snapshot.root),
                rebuilt = %hex::encode(smt.root()),
                "the gateway snapshot does not rebuild to its own root — ignoring it"
            );

            return;
        }

        tracing::info!(
            router,
            ledger = snapshot.last_processed,
            leaves = snapshot.leaves.len(),
            root = %hex::encode(snapshot.root),
            "restored the state tree from the gateway snapshot"
        );

        self.smt = smt;
        self.leaves = snapshot.leaves;
        self.last_processed = Some(snapshot.last_processed);
    }

    async fn persist(&self) {
        let (Some(store), Some(router), Some(last_processed)) =
            (&self.store, &self.router_address, self.last_processed)
        else {
            return;
        };

        let snapshot = Snapshot {
            last_processed,
            root: self.smt.root(),
            leaves: self.leaves.clone(),
        };

        if let Err(error) = store.save(router, &snapshot).await {
            tracing::warn!(%error, router, ledger = last_processed, "could not write the gateway snapshot");
        }
    }

    async fn replay_through(&mut self, seq: u32) -> anyhow::Result<[u8; 32]> {
        let start = match self.last_processed {
            Some(last) if last >= seq => {
                let root = self.smt.root();
                self.roots.insert(seq, root);
                return Ok(root);
            }
            Some(last) => last + 1,
            None => self.cold_start(seq)?,
        };

        if start < seq {
            tracing::info!(
                from = start,
                to = seq,
                "replaying ledger range into smt (cumulative)"
            );
        }

        if start == seq {
            self.apply_ledger(seq).await?;
            self.last_processed = Some(seq);
            let root = self.smt.root();
            self.roots.insert(seq, root);
            return Ok(root);
        }

        match self.ledgers_with_router_activity(start, seq).await {
            Some(ledgers) => {
                tracing::info!(
                    from = start,
                    to = seq,
                    scanned = ledgers.len(),
                    skipped = (seq - start + 1) as usize - ledgers.len(),
                    "replaying only ledgers with router activity"
                );
                self.apply_ledgers(ledgers).await?;
            }
            None => {
                self.apply_ledgers((start..=seq).collect()).await?;
            }
        }

        self.last_processed = Some(seq);
        let root = self.smt.root();
        self.roots.insert(seq, root);
        Ok(root)
    }

    fn cold_start(&self, seq: u32) -> anyhow::Result<u32> {
        match self.genesis_ledger {
            Some(genesis) if genesis <= seq => Ok(genesis),
            Some(genesis) => anyhow::bail!(
                "the router genesis ledger {genesis} is after the requested ledger {seq}; \
                 ROUTER_GENESIS_LEDGER does not belong to this router"
            ),
            None => {
                tracing::warn!(
                    ledger = seq,
                    "no ROUTER_GENESIS_LEDGER — replaying this ledger alone, which only matches \
                     the chain if the router first wrote here; the published-root check decides"
                );

                Ok(seq)
            }
        }
    }

    async fn ledgers_with_router_activity(&self, start: u32, end: u32) -> Option<Vec<u32>> {
        const PAGE_LIMIT: u32 = 200;
        const MAX_PAGES: usize = 512;

        let address = self.router_address.as_deref()?;
        if start > end {
            return Some(Vec::new());
        }

        let mut cursor = EventCursor::StartLedger(start);
        let mut ledgers = Vec::new();

        for _ in 0..MAX_PAGES {
            let page = match self.api.get_events(address, cursor, Some(PAGE_LIMIT)).await {
                Ok(page) => page,
                Err(error) => {
                    tracing::warn!(
                        %error,
                        start,
                        end,
                        "router event index unavailable — replaying every ledger"
                    );
                    return None;
                }
            };

            let mut past_end = false;
            for event in &page.events {
                if event.ledger > end {
                    past_end = true;
                } else if event.ledger >= start {
                    ledgers.push(event.ledger);
                }
            }

            let scanned_through = cursor_ledger(&page.cursor);
            let exhausted = page.cursor.is_empty();
            let swept_past_end = scanned_through.is_some_and(|l| l >= end);

            if past_end || exhausted || swept_past_end {
                ledgers.sort_unstable();
                ledgers.dedup();
                return Some(ledgers);
            }

            cursor = EventCursor::Cursor(page.cursor);
        }

        tracing::warn!(
            start,
            end,
            "router event index did not terminate — replaying every ledger"
        );
        None
    }

    async fn apply_ledgers(&mut self, ledgers: Vec<u32>) -> anyhow::Result<()> {
        for batch in ledgers.chunks(CONCURRENT_LEDGER_FETCHES) {
            let mut fetches = tokio::task::JoinSet::new();

            for &seq in batch {
                let api = self.api.clone();

                fetches.spawn(async move { (seq, api.get_ledger(seq).await) });
            }

            let mut fetched = BTreeMap::new();

            while let Some(joined) = fetches.join_next().await {
                let (seq, ledger) =
                    joined.map_err(|error| anyhow::anyhow!("ledger fetch task failed: {error}"))?;

                fetched.insert(seq, ledger?);
            }

            for (seq, ledger) in fetched {
                self.apply_fetched(seq, ledger)?;
            }
        }

        Ok(())
    }

    async fn apply_ledger(&mut self, seq: u32) -> anyhow::Result<()> {
        let ledger = self.api.get_ledger(seq).await?;

        self.apply_fetched(seq, ledger)
    }

    fn apply_fetched(&mut self, seq: u32, ledger: LedgerData) -> anyhow::Result<()> {
        let meta = match ledger.metadata_xdr {
            Some(meta_xdr) => Some(
                LedgerCloseMeta::from_xdr(&meta_xdr, Limits::none())
                    .map_err(|e| anyhow::anyhow!("LedgerCloseMeta XDR decode: {e}"))?,
            ),
            None => None,
        };

        if self.journal_floor.is_none() {
            self.journal_floor = Some(seq.saturating_sub(1));
        }

        self.applying = seq;

        let mut changes_applied = 0usize;
        if let Some(meta) = &meta {
            for change in ledger_changes(meta) {
                if self.apply(change) {
                    changes_applied += 1;
                }
            }
        }

        if changes_applied > 0 {
            tracing::info!(
                ledger = seq,
                ibc_writes = changes_applied,
                root = %hex::encode(self.smt.root()),
                "[gateway] SMT updated — committed IBC state change(s)"
            );
        } else {
            tracing::debug!(sequence = seq, "ledger processed into smt (no ibc changes)");
        }

        if let Some(root) = meta
            .as_ref()
            .and_then(|meta| published_root(meta, self.ibc_contract_id.as_ref()))
        {
            self.published.insert(seq, root);
        }

        self.prune_journal(seq);

        Ok(())
    }

    fn check_published_root(&self, seq: u32) -> anyhow::Result<()> {
        let Some(published) = self.published.get(&seq).copied() else {
            return Ok(());
        };

        let computed = self.smt.root();

        if computed == published {
            tracing::debug!(ledger = seq, "replayed root matches the published one");

            return Ok(());
        }

        anyhow::bail!(
            "the replayed state root at ledger {seq} is {} but the router published {}; the \
             replay is missing writes, so this proof would carry a root the chain never had. \
             Check ROUTER_GENESIS_LEDGER covers the router's first write",
            hex::encode(computed),
            hex::encode(published)
        )
    }

    fn apply(&mut self, change: LedgerEntryChange) -> bool {
        match change {
            LedgerEntryChange::Created(entry) | LedgerEntryChange::Updated(entry) => {
                self.apply_contract_data_write(entry.data)
            }
            LedgerEntryChange::Removed(LedgerKey::ContractData(key)) => {
                if !self.matches(&key.contract) {
                    return false;
                }
                let Some(path) = scval_to_v2_path(&key.key) else {
                    return false;
                };
                self.record(&path);
                self.smt.remove(&path);
                self.leaves.remove(&path);
                true
            }
            _ => false,
        }
    }

    fn apply_contract_data_write(&mut self, data: LedgerEntryData) -> bool {
        let LedgerEntryData::ContractData(d) = data else {
            return false;
        };
        if !self.matches(&d.contract) {
            return false;
        }
        let Some(path) = scval_to_v2_path(&d.key) else {
            return false;
        };
        let Some(value) = scval_as_bytes(&d.val) else {
            return false;
        };
        self.record(&path);
        self.smt.insert(&path, &value);
        self.leaves.insert(path, value);

        true
    }

    fn matches(&self, addr: &ScAddress) -> bool {
        let ScAddress::Contract(hash) = addr else {
            return false;
        };
        match &self.ibc_contract_id {
            None => true,
            Some(id) => hash.0 == soroban_client::xdr::Hash(*id),
        }
    }
}

pub const ROOT_EVENT_TOPIC: &[u8] = b"ibc_root";

fn published_root(meta: &LedgerCloseMeta, router: Option<&[u8; 32]>) -> Option<[u8; 32]> {
    let mut latest = None;

    for event in ledger_events(meta) {
        if let Some(router) = router {
            let Some(id) = event.contract_id.as_ref() else {
                continue;
            };

            if id.0 .0 != *router {
                continue;
            }
        }

        let ContractEventBody::V0(body) = &event.body;

        let is_root = body.topics.first().is_some_and(
            |topic| matches!(topic, ScVal::Symbol(s) if s.0.as_slice() == ROOT_EVENT_TOPIC),
        );

        if !is_root {
            continue;
        }

        if let Some(bytes) = scval_as_bytes(&body.data) {
            if let Ok(root) = <[u8; 32]>::try_from(bytes.as_slice()) {
                latest = Some(root);
            }
        }
    }

    latest
}

fn ledger_events(meta: &LedgerCloseMeta) -> Vec<ContractEvent> {
    let mut out = vec![];

    match meta {
        LedgerCloseMeta::V0(v) => {
            for tx in v.tx_processing.iter() {
                collect_tx_events(&tx.tx_apply_processing, &mut out);
            }
        }
        LedgerCloseMeta::V1(v) => {
            for tx in v.tx_processing.iter() {
                collect_tx_events(&tx.tx_apply_processing, &mut out);
            }
        }
        LedgerCloseMeta::V2(v) => {
            for tx in v.tx_processing.iter() {
                collect_tx_events(&tx.tx_apply_processing, &mut out);
            }
        }
    }

    out
}

fn collect_tx_events(meta: &TransactionMeta, out: &mut Vec<ContractEvent>) {
    match meta {
        TransactionMeta::V3(v) => {
            if let Some(soroban) = &v.soroban_meta {
                out.extend(soroban.events.iter().cloned());
            }
        }
        TransactionMeta::V4(v) => {
            for op in v.operations.iter() {
                out.extend(op.events.iter().cloned());
            }
        }
        _ => {}
    }
}

fn ledger_changes(meta: &LedgerCloseMeta) -> Vec<LedgerEntryChange> {
    let mut out = vec![];
    match meta {
        LedgerCloseMeta::V0(v) => {
            for tx in v.tx_processing.iter() {
                collect_tx_changes(&tx.tx_apply_processing, &mut out);
            }
        }
        LedgerCloseMeta::V1(v) => {
            for tx in v.tx_processing.iter() {
                collect_tx_changes(&tx.tx_apply_processing, &mut out);
            }
        }
        LedgerCloseMeta::V2(v) => {
            for tx in v.tx_processing.iter() {
                collect_tx_changes(&tx.tx_apply_processing, &mut out);
            }
        }
    }
    out
}

fn collect_tx_changes(meta: &TransactionMeta, out: &mut Vec<LedgerEntryChange>) {
    match meta {
        TransactionMeta::V0(operations) => {
            for op in operations.iter() {
                out.extend(op.changes.iter().cloned());
            }
        }
        TransactionMeta::V1(v) => {
            out.extend(v.tx_changes.iter().cloned());
            for op in v.operations.iter() {
                out.extend(op.changes.iter().cloned());
            }
        }
        TransactionMeta::V2(v) => {
            out.extend(v.tx_changes_before.iter().cloned());
            for op in v.operations.iter() {
                out.extend(op.changes.iter().cloned());
            }
            out.extend(v.tx_changes_after.iter().cloned());
        }
        TransactionMeta::V3(v) => {
            out.extend(v.tx_changes_before.iter().cloned());
            for op in v.operations.iter() {
                out.extend(op.changes.iter().cloned());
            }
            out.extend(v.tx_changes_after.iter().cloned());
        }
        TransactionMeta::V4(v) => {
            out.extend(v.tx_changes_before.iter().cloned());
            for op in v.operations.iter() {
                out.extend(op.changes.iter().cloned());
            }
            out.extend(v.tx_changes_after.iter().cloned());
        }
        #[allow(unreachable_patterns)]
        _ => {}
    }
}
