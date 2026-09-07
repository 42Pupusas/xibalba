use std::sync::Arc;

use rustls::ClientConfig;
use rustls::crypto::CryptoProvider;

use xibalba_proto::error::Error;

/// Builds a rustls `ClientConfig` from a caller-supplied `CryptoProvider`.
///
/// The provider is a parameter rather than a default. `ClientConfig::builder()`
/// would instead resolve rustls' process-wide default provider, which makes the
/// active cryptography depend on enabled features and installation order
/// elsewhere in the binary.
pub struct RustlsConfig;

impl RustlsConfig {
    /// # Errors
    ///
    /// Returns [`Error::Tls`] if the provider does not support the safe
    /// default protocol versions.
    pub fn with_provider(provider: CryptoProvider) -> Result<Arc<ClientConfig>, Error> {
        let mut root_store = rustls::RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        Self::with_provider_and_roots(provider, root_store)
    }

    /// The same configuration against an explicit trust anchor set, which is
    /// what a test with a self-signed certificate needs.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Tls`] if the provider does not support the safe
    /// default protocol versions.
    pub fn with_provider_and_roots(
        provider: CryptoProvider,
        root_store: rustls::RootCertStore,
    ) -> Result<Arc<ClientConfig>, Error> {
        let config = ClientConfig::builder_with_provider(Arc::new(provider))
            .with_safe_default_protocol_versions()
            .map_err(|e| xibalba_proto::error::TlsError {
                message: e.to_string(),
            })?
            .with_root_certificates(root_store)
            .with_no_client_auth();
        Ok(Arc::new(config))
    }
}
