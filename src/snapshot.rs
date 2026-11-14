use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use anyhow::Context;
use tokio::sync::Mutex;
use tokio_postgres::Client;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS gateway_snapshot (
    router TEXT PRIMARY KEY,
    last_processed BIGINT NOT NULL,
    root BYTEA NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS gateway_leaf (
    router TEXT NOT NULL,
    path BYTEA NOT NULL,
    value BYTEA NOT NULL,
    PRIMARY KEY (router, path)
);
";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    pub last_processed: u32,
    pub root: [u8; 32],
    pub leaves: BTreeMap<Vec<u8>, Vec<u8>>,
}

#[derive(Clone)]
pub enum SnapshotStore {
    Memory(Arc<Mutex<HashMap<String, Snapshot>>>),
    Postgres(Arc<String>),
}

impl SnapshotStore {
    pub fn memory() -> Self {
        Self::Memory(Arc::new(Mutex::new(HashMap::new())))
    }

    pub fn postgres(url: impl Into<String>) -> Self {
        Self::Postgres(Arc::new(url.into()))
    }

    pub async fn load(&self, router: &str) -> anyhow::Result<Option<Snapshot>> {
        match self {
            Self::Memory(snapshots) => Ok(snapshots.lock().await.get(router).cloned()),
            Self::Postgres(url) => load_postgres(url, router).await,
        }
    }

    pub async fn save(&self, router: &str, snapshot: &Snapshot) -> anyhow::Result<()> {
        match self {
            Self::Memory(snapshots) => {
                snapshots
                    .lock()
                    .await
                    .insert(router.to_string(), snapshot.clone());

                Ok(())
            }
            Self::Postgres(url) => save_postgres(url, router, snapshot).await,
        }
    }
}

async fn connect(url: &str) -> anyhow::Result<Client> {
    let config = crate::db_tls::config(url).context("parsing GATEWAY_DB_URL")?;

    let tls = crate::db_tls::tls().context("loading the database root certificates")?;

    let (client, connection) = config
        .connect(tls)
        .await
        .context("connecting to the gateway snapshot database")?;

    tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::warn!(%error, "gateway snapshot database connection closed");
        }
    });

    client
        .batch_execute(SCHEMA)
        .await
        .context("creating the gateway snapshot tables")?;

    Ok(client)
}

async fn load_postgres(url: &str, router: &str) -> anyhow::Result<Option<Snapshot>> {
    let client = connect(url).await?;

    let Some(row) = client
        .query_opt(
            "SELECT last_processed, root FROM gateway_snapshot WHERE router = $1",
            &[&router],
        )
        .await
        .context("reading the gateway snapshot")?
    else {
        return Ok(None);
    };

    let last_processed: i64 = row.get(0);
    let root: Vec<u8> = row.get(1);

    let leaves = client
        .query(
            "SELECT path, value FROM gateway_leaf WHERE router = $1",
            &[&router],
        )
        .await
        .context("reading the gateway snapshot leaves")?
        .into_iter()
        .map(|row| (row.get::<_, Vec<u8>>(0), row.get::<_, Vec<u8>>(1)))
        .collect();

    Ok(Some(Snapshot {
        last_processed: u32::try_from(last_processed)
            .context("the stored last processed ledger is out of range")?,
        root: <[u8; 32]>::try_from(root.as_slice())
            .map_err(|_| anyhow::anyhow!("the stored root is not 32 bytes"))?,
        leaves,
    }))
}

async fn save_postgres(url: &str, router: &str, snapshot: &Snapshot) -> anyhow::Result<()> {
    let mut client = connect(url).await?;
    let transaction = client
        .transaction()
        .await
        .context("starting the snapshot transaction")?;

    transaction
        .execute(
            "INSERT INTO gateway_snapshot (router, last_processed, root) VALUES ($1, $2, $3)
             ON CONFLICT (router) DO UPDATE
             SET last_processed = EXCLUDED.last_processed, root = EXCLUDED.root, updated_at = now()",
            &[
                &router,
                &i64::from(snapshot.last_processed),
                &snapshot.root.as_slice(),
            ],
        )
        .await
        .context("writing the gateway snapshot")?;

    transaction
        .execute("DELETE FROM gateway_leaf WHERE router = $1", &[&router])
        .await
        .context("clearing the gateway snapshot leaves")?;

    for (path, value) in &snapshot.leaves {
        transaction
            .execute(
                "INSERT INTO gateway_leaf (router, path, value) VALUES ($1, $2, $3)",
                &[&router, &path.as_slice(), &value.as_slice()],
            )
            .await
            .context("writing a gateway snapshot leaf")?;
    }

    transaction
        .commit()
        .await
        .context("committing the gateway snapshot")
}
