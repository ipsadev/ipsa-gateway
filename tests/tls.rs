use std::path::{Path, PathBuf};

use ipsa_gateway::db_tls::{config, read_certificates, root_store, tls, DEFAULT_CONNECT_TIMEOUT};

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

#[test]
fn without_a_file_the_roots_are_the_web_roots() {
    let roots = root_store(None).unwrap();

    assert_eq!(roots.len(), webpki_roots::TLS_SERVER_ROOTS.len());
}

#[test]
fn a_file_adds_its_roots_to_the_web_roots() {
    let roots = root_store(Some(&fixture("roots.pem"))).unwrap();

    assert_eq!(roots.len(), webpki_roots::TLS_SERVER_ROOTS.len() + 2);
}

#[test]
fn every_certificate_in_the_file_is_read() {
    assert_eq!(read_certificates(&fixture("root.pem")).unwrap().len(), 1);
    assert_eq!(read_certificates(&fixture("roots.pem")).unwrap().len(), 2);
}

#[test]
fn a_missing_file_is_an_error() {
    assert!(root_store(Some(&fixture("missing.pem"))).is_err());
}

#[test]
fn a_file_without_certificates_is_an_error() {
    let empty = std::env::temp_dir().join("ipsa-gateway-empty-roots.pem");

    std::fs::write(&empty, "not a certificate\n").unwrap();

    let result = read_certificates(&empty);

    std::fs::remove_file(&empty).unwrap();

    assert!(result.is_err());
}

#[test]
fn the_connector_builds_more_than_once() {
    let _first = tls().unwrap();
    let _second = tls().unwrap();
}

#[test]
fn a_url_without_a_timeout_gets_the_default() {
    let config = config("postgres://user:password@localhost:5432/database").unwrap();

    assert_eq!(config.get_connect_timeout(), Some(&DEFAULT_CONNECT_TIMEOUT));
}

#[test]
fn a_url_with_a_timeout_keeps_it() {
    let config =
        config("postgres://user:password@localhost:5432/database?connect_timeout=30").unwrap();

    assert_eq!(
        config.get_connect_timeout(),
        Some(&std::time::Duration::from_secs(30))
    );
}

#[tokio::test]
async fn an_unanswering_database_fails_within_the_timeout() {
    let started = std::time::Instant::now();

    let result = config("postgres://user:password@10.255.255.1:5432/database")
        .unwrap()
        .connect(tls().unwrap())
        .await;

    assert!(result.is_err());
    assert!(started.elapsed() < DEFAULT_CONNECT_TIMEOUT * 2);
}
