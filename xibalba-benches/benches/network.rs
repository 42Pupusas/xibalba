use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::OnceLock;
use std::time::Duration;

use xibalba_client::client::Client;
use xibalba_client::connector::{Connector, SetReadTimeout};
use xibalba_proto::error::Error;
use xibalba_proto::method::Method;
use xibalba_proto::url::Url;

fn main() {
    divan::main();
}

// ── Minimal plain-TCP connector for the xibalba-client bench ──────────────

struct PlainConnector;

struct PlainStream(TcpStream);

impl Read for PlainStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.0.read(buf)
    }
}

impl Write for PlainStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.0.flush()
    }
}

impl SetReadTimeout for PlainStream {
    fn set_read_timeout(&self, dur: Option<Duration>) -> std::io::Result<()> {
        self.0.set_read_timeout(dur)
    }
}

impl Connector for PlainConnector {
    type Stream = PlainStream;
    type TlsConfig = ();

    fn connect(url: &Url<'_>, _tls_config: &()) -> Result<Self::Stream, Error> {
        let host = std::str::from_utf8(url.host)
            .map_err(|_| Error::Connection("invalid UTF-8 in host".into()))?;
        let addr = format!("{}:{}", host, url.effective_port());
        let stream = TcpStream::connect(&addr)?;
        Ok(PlainStream(stream))
    }
}

// ── Echo server ───────────────────────────────────────────────────────────

static ECHO_PORT: OnceLock<u16> = OnceLock::new();

fn echo_server_port() -> u16 {
    *ECHO_PORT.get_or_init(|| {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut s) = stream else { continue };
                std::thread::spawn(move || {
                    let mut buf = [0u8; 1024];
                    let _ = s.read(&mut buf);
                    let _ = s.write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 13\r\nConnection: close\r\n\r\nHello, World!",
                    );
                    // half-close write side; drain until client closes
                    let _ = s.shutdown(std::net::Shutdown::Write);
                    let mut drain = [0u8; 64];
                    while s.read(&mut drain).unwrap_or(0) > 0 {}
                });
            }
        });
        port
    })
}

fn network_enabled() -> bool {
    std::env::var("BENCH_NETWORK").is_ok()
}

// ── Comparisons: xibalba-client vs ureq ───────────────────────────────────

mod compare {
    use std::io::Read;

    use divan::black_box;

    use super::{Client, Method, PlainConnector, echo_server_port, network_enabled};

    #[divan::bench(skip_ext_time)]
    fn round_trip_xibalba(bencher: divan::Bencher) {
        let enabled = network_enabled();
        let port = if enabled { echo_server_port() } else { 0 };
        let url_str = format!("http://127.0.0.1:{port}/");
        let url_bytes = url_str.into_bytes();

        bencher.bench_local(|| {
            if !enabled {
                return;
            }
            let client: Client<PlainConnector> = Client::new(());
            let mut resp = client
                .request(black_box(Method::Get), black_box(&url_bytes))
                .unwrap();
            let mut body = Vec::new();
            resp.body().read_to_end(&mut body).unwrap();
        });
    }

    #[divan::bench(skip_ext_time)]
    fn round_trip_ureq(bencher: divan::Bencher) {
        let enabled = network_enabled();
        let port = if enabled { echo_server_port() } else { 0 };
        let url = format!("http://127.0.0.1:{port}/");

        bencher.bench_local(|| {
            if !enabled {
                return;
            }
            let _body = ureq::get(black_box(&url))
                .call()
                .unwrap()
                .body_mut()
                .read_to_string()
                .unwrap();
        });
    }
}
