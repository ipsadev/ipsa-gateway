use std::{
    env, fs, io,
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
    time::Duration,
};

use rustls::{crypto::ring, ClientConfig, RootCertStore};
use rustls_pki_types::{pem::PemObject, CertificateDer};
use tokio_postgres::Config;
use tokio_postgres_rustls::MakeRustlsConnect;

pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

pub const ROOT_CERTIFICATE_VARIABLE: &str = "PGSSLROOTCERT";

pub fn read_certificates(path: &Path) -> io::Result<Vec<CertificateDer<'static>>> {
    let pem = fs::read(path)?;

    let certificates = CertificateDer::pem_slice_iter(&pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;

    if certificates.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "the file holds no certificates",
        ));
    }

    Ok(certificates)
}

pub fn root_store(path: Option<&Path>) -> io::Result<RootCertStore> {
    let mut roots = RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    };

    let Some(path) = path else {
        return Ok(roots);
    };

    let (_, ignored) = roots.add_parsable_certificates(read_certificates(path)?);

    if ignored > 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{ignored} certificates are not usable roots"),
        ));
    }

    Ok(roots)
}

pub fn tls() -> io::Result<MakeRustlsConnect> {
    static CONFIG: OnceLock<Result<Arc<ClientConfig>, String>> = OnceLock::new();

    let config = CONFIG.get_or_init(|| {
        let path = env::var_os(ROOT_CERTIFICATE_VARIABLE)
            .filter(|path| !path.is_empty())
            .map(PathBuf::from);

        let roots = root_store(path.as_deref())
            .map_err(|error| format!("reading {ROOT_CERTIFICATE_VARIABLE} {path:?}: {error}"))?;

        let config = ClientConfig::builder_with_provider(Arc::new(ring::default_provider()))
            .with_safe_default_protocol_versions()
            .expect("ring supports the default protocol versions")
            .with_root_certificates(roots)
            .with_no_client_auth();

        Ok(Arc::new(config))
    });

    match config {
        Ok(config) => Ok(MakeRustlsConnect::new(config.as_ref().clone())),
        Err(error) => Err(io::Error::new(io::ErrorKind::InvalidData, error.clone())),
    }
}

pub fn config(url: &str) -> Result<Config, tokio_postgres::Error> {
    let mut config: Config = url.parse()?;

    if config.get_connect_timeout().is_none() {
        config.connect_timeout(DEFAULT_CONNECT_TIMEOUT);
    }

    Ok(config)
}
