# ipsa-gateway

The keyless gRPC gateway for Stellar in [ipsa](https://ipsa.dev), light clients for IBC v2. It proves Stellar packets against its copy of the router's state tree and builds the Stellar and Cosmos Hub side of each relay, which the relayer reaches through the proof API. It holds no key.

**No component holds funds or attests to events off-chain.** Authenticity is verified by on-chain light clients; the relayer, the gateway and the api are untrusted transport. A malicious gateway can stall a relay, but cannot make a light client accept a forged proof.

> [!WARNING]
> **Early, under active development: a test implementation, not production-ready.** No third-party security audit has been done.

## What it serves

- **`ProofApiService`**, the proof API's own protocol, which the proof API's `forward` module calls for the Stellar ↔ Cosmos Hub pairs. Given a source transaction it returns what relays it: Soroban host functions for client updates, receives, acknowledgements and timeouts on Stellar, or Cosmos messages for the Hub, for the relayer to sign.
- **`StellarGatewayQuery` and `StellarGatewayMsg`**: heights, headers, packet commitments, receipts and acknowledgements with proofs, tree siblings, and unsigned transactions for creating and updating clients and registering counterparties. The `ipsa` CLI uses them, and the proof API's `stellar-to-eth` module reads `QueryTreeSiblings`. The relayer never calls these two services.

Both protocols come from the [`ipsa`](https://crates.io/crates/ipsa) crate's `gateway` feature, the one copy the relayer, the `ipsa` CLI and anyone relaying for themselves share. Every chain read goes through the api in [ipsa-backend](https://github.com/ipsadev/ipsa-backend).

## Running it

```sh
cp .env.example .env                    # every setting, with what it is for
set -a; . ./.env; set +a
cargo run                               # gRPC on :50052, reaching the api at STELLAR_API_URL
```

The gateway reads only its environment, hence the export. It needs a running api; ipsa-backend's README says how to start one against Stellar testnet. The image is `amandagonsalvesx/ipsa-gateway`, and `just image` builds it locally.

## Development

Requires `cargo`, `just` and `cargo-audit`.

```sh
just ci                                 # formatting, clippy, the tests and cargo audit, as CI runs them
just test                               # the tests only
```

The tests are in `tests/`: one file per area, and `tests/integration/`, which runs the gateway against a mock api and mock chains. The live tests in `tests/integration/live_seed.rs` run only when `STELLAR_LIVE_RPC_URL` or `IPSA_LIVE_API_URL` is set.

`cargo audit` runs with the advisories listed in the `justfile` ignored, the same set as ipsa-backend's, because the gateway shares its dependency versions. Remove an ignore once the crate holding it back upgrades.

## Releasing

Push a tag `vX.Y.Z` matching the version in `Cargo.toml`. `.github/workflows/release.yml` runs the checks, builds the image for `linux/amd64` and `linux/arm64`, pushes it to Docker Hub as `amandagonsalvesx/ipsa-gateway:vX.Y.Z` with `latest` and a commit tag, and writes the GitHub release. Running it by hand with `dry_run` runs the checks without publishing anything. `cd.yml` can also be run by hand to publish `latest` and a commit tag from any branch. The image name comes from the `GATEWAY_IMAGE` repository variable, and Docker Hub access from the `DOCKER_USERNAME` and `DOCKER_TOKEN` secrets of the `staging` environment.

Staging is deployed from [ipsa-infrastructure](https://github.com/ipsadev/ipsa-infrastructure) with `scripts/deploy-ecs-service.sh -s gateway -t vX.Y.Z --sync-env`.

## The state tree

The gateway proves Stellar packets against the router's sparse Merkle tree, which it keeps as a map from IBC path to value. It saves that map to `GATEWAY_DB_URL` and brings it forward by replaying the ledgers with router activity, found through the router's events.

On every start the gateway seeds the tree from the router's storage, and falls back to its saved snapshot only when seeding fails. A snapshot can fall behind the router without noticing, and Soroban RPC keeps only about a week of ledgers to replay, so the router's storage is the source of truth. The seed lists the router's clients (the api's list plus each known client type), reads each client's commitments up to the last sequence it sent (`NextSeqSend` holds the last one, not the next), probes receipts and acknowledgements in batches of 64 sequences until a batch holds none, and accepts the result only if it rebuilds the root the router has stored. RPC returns archived entries too, so leaves whose lifetime has lapsed are still read. A seed that does not rebuild the stored root is refused and logged, never served.

A follower brings the tree up to the latest ledger every `GATEWAY_FOLLOW_SECONDS` (60 by default, 0 turns it off) and saves it, so the gateway never falls out of the RPC's window again. After a seed, and after restoring a saved snapshot, the gateway rebuilds its write journal from the router's ledgers over the previous 34,560 ledgers (about two days): each ledger's metadata records the value an entry held before it was written, so the tree can be rewound to any of those ledgers.

Every proof is built from the tree as it was at the ledger asked for, rewound through that journal, because the follower has usually moved the tree past the ledger a packet was sent in. The rewound tree must match the root the router published at that ledger. When it does not, or when the follower finds the latest tree disagreeing with the latest published root, the proof is refused and the gateway seeds its tree again from the router's storage, so the relayer's next attempt is answered from a correct tree.

Building a relay keeps its reads of ledgers off the critical path, because each `/ledger/{sequence}` carries the ledger's whole close metadata and Soroban RPC can take seconds to serve one. The gateway reads the latest ledger number from `/ledger/latest/sequence`, which makes only RPC's `getLatestLedger` call, and falls back to `/ledger/latest` against an api that predates the route. It looks for a relayed transaction's packet events in the last 720 ledgers (about an hour) first, and scans the full `RELAY_EVENT_LOOKBACK_LEDGERS` (17,280 by default) only when one of the transactions is not there. A replay fetches the ledgers it applies eight at a time and applies them in ledger order, so the tree and its journal are the same as a sequential replay. A Stellar header's evidence (both ledgers' SCP messages, the header, the next transaction set and the close metadata) is fetched concurrently, and `RelayByTx` builds the packet proofs, the timeouts and the client update concurrently.

ipsa-backend's `POST /stellar/contract-data` serves the seed: up to 200 persistent entries of a contract, by ScVal key, read in one RPC call at one ledger.

## Postgres and TLS

The gateway saves its state tree to `GATEWAY_DB_URL` through `src/db_tls.rs`, which offers TLS with Postgres' default `sslmode=prefer`. ipsa-backend's api keeps an identical copy for its own database.

- A server that accepts TLS gets it, and its certificate must chain to the web roots or to the roots in the file named by `PGSSLROOTCERT`, so RDS, which requires TLS, is verified rather than merely encrypted. A `PGSSLROOTCERT` that cannot be read, or holds no usable certificates, fails every connection with that error instead of falling back to the web roots. An untrusted certificate is refused, not downgraded.
- A server without TLS, such as the compose stack's Postgres, gets a plain connection, as does any URL with `sslmode=disable`.
- A connection that is not established within 5 seconds fails, unless the URL sets its own `connect_timeout`.

The bundle is not in the code. The image downloads Amazon's global bundle from `https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem` to `/etc/ssl/rds-global-bundle.pem` when it is built, and sets `PGSSLROOTCERT` to it, so a rotation of RDS' certificate authorities needs a rebuild, not a code change. Outside the image, leave `PGSSLROOTCERT` unset to trust only the web roots, or point it at a downloaded bundle.

## Creating the Stellar client

`CreateClient` from Stellar to the Hub builds the `08-wasm` Stellar client state from `parameters`. Its trust root is the operator's decision, so the gateway takes it from `parameters.quorum_configs`, a JSON array whose entries carry:

- `quorum_set_xdr`: the `SCPQuorumSet` XDR, hex;
- `valid_from`: the first slot the set is in force for, defaulting to 0;
- `quorum_set_fingerprint`: `sha256(quorum_set_xdr)`, hex, 32 bytes.

The fingerprint is the operator's pin, copied into the client state as given; the gateway never derives it. A missing fingerprint, one that is not 32 bytes, or a quorum set that does not hash to it is refused with `invalid_argument` naming the entry's `valid_from`, before anything is built. The Stellar light client from the release that closes ipsadev/stellar-light-client#10 requires the field and refuses the same mismatches on chain; an older light client ignores it. `QueryStellarClientParams` returns each quorum set it serves with its fingerprint, for the operator to compare with their pins; nothing trusts that response.

## Stellar and Cosmos encoding

Soroban values, IBC v2 messages, Tendermint client and consensus states and the router's packet events come from the [`ipsa-chains`](https://crates.io/crates/ipsa-chains) crate, published from the [ipsa](https://github.com/ipsadev/ipsa) repository and shared with ipsa-backend's api and provider. Its README covers revision numbers and the trusted validator set a Tendermint header carries.

## History

The gateway moved here from [ipsa-backend](https://github.com/ipsadev/ipsa-backend), where it was `crates/gateway`, with its history. Pull request numbers in older commit messages refer to ipsa-backend.

## License

Copyright 2026 Amanda Gonsalves. Licensed under the [Apache License 2.0](LICENSE); see also [NOTICE](NOTICE).
