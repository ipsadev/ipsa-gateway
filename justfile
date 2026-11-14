# ipsa-gateway: the keyless gRPC gateway for Stellar

lints := "-D warnings -A clippy::manual_is_multiple_of -A clippy::too_many_arguments -A clippy::result_large_err"

ignored_advisories := "--ignore RUSTSEC-2026-0009 --ignore RUSTSEC-2026-0258 --ignore RUSTSEC-2026-0367 --ignore RUSTSEC-2026-0368 --ignore RUSTSEC-2026-0369 --ignore RUSTSEC-2026-0387 --ignore RUSTSEC-2026-0429"

default:
    @just --list

# everything CI runs, in CI's order
ci: fmt-check lint test audit

fmt:
    cargo fmt --all

fmt-check:
    cargo fmt --all -- --check

lint:
    cargo clippy --locked --all-targets -- {{lints}}

test:
    cargo test --locked

audit:
    cargo audit --file Cargo.lock {{ignored_advisories}}

# the gateway image, built the way the cd workflow builds it
image tag="ipsa-gateway:local":
    docker build -t {{tag}} .

clean:
    cargo clean
