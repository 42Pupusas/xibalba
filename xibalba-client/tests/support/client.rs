//! Constructing clients pointed at a loopback test server.

use xibalba_client::PlainConnector;
use xibalba_client::async_client::AsyncClient;
use xibalba_client::client::{Client, Config};

/// The head buffer used by the size-limit tests, small enough that an
/// ordinary response head overruns it.
pub(crate) const SMALL_HEAD_SIZE: usize = 256;

/// Builds clients addressed at `127.0.0.1:<port>`.
pub(crate) struct TestClient;

impl TestClient {
    /// The loopback URL a test server on `port` answers.
    pub(crate) fn url(port: u16) -> String {
        format!("http://127.0.0.1:{port}/")
    }

    /// A synchronous client with the default configuration.
    pub(crate) fn connect(port: u16) -> Client<PlainConnector> {
        Client::<PlainConnector>::connect_default(Self::url(port).as_bytes(), ()).unwrap()
    }

    /// A synchronous client with a caller-supplied configuration.
    pub(crate) fn with_config(port: u16, config: Config) -> Client<PlainConnector> {
        Client::<PlainConnector>::connect(Self::url(port).as_bytes(), (), config).unwrap()
    }

    /// A synchronous client whose head buffer is [`SMALL_HEAD_SIZE`].
    pub(crate) fn with_small_head_limit(
        port: u16,
        config: Config,
    ) -> Client<PlainConnector, SMALL_HEAD_SIZE> {
        Client::<PlainConnector, SMALL_HEAD_SIZE>::connect(Self::url(port).as_bytes(), (), config)
            .unwrap()
    }

    /// An asynchronous client with the default configuration.
    pub(crate) fn connect_async(port: u16) -> AsyncClient {
        AsyncClient::connect::<PlainConnector>(Self::url(port).as_bytes(), (), Config::default())
            .unwrap()
    }

    /// An asynchronous client with a caller-supplied configuration.
    pub(crate) fn async_with_config(port: u16, config: Config) -> AsyncClient {
        AsyncClient::connect::<PlainConnector>(Self::url(port).as_bytes(), (), config).unwrap()
    }
}
