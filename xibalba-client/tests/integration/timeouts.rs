//! The silence budget firing against a server that never answers.

use std::net::TcpListener;
use std::thread;
use std::time::Duration;

use crate::support::client::TestClient;
use crate::support::park::StopSignal;
use crate::support::server::RequestReader;
use xibalba_client::client::Config;
use xibalba_client::proto::method::Method;

#[test]
fn timeout_fires_on_stalled_server() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let (stop, park) = StopSignal::new();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        RequestReader::read_head(&mut stream);
        // Never send a response; hold the socket open so the client waits on
        // its silence budget rather than seeing EOF.
        park.wait();
        drop(stream);
    });

    // Silence tolerance is the explicit head_silence budget, not a
    // multiple of read_timeout — configure it short so the test is fast.
    let config = Config {
        read_timeout: Some(Duration::from_millis(100)),
        head_silence: Duration::from_millis(400),
        ..Config::default()
    };
    let mut client = TestClient::with_config(port, config);
    let start = std::time::Instant::now();
    let result = client.request(Method::Get, b"/", None, None);
    assert!(result.is_err(), "expected timeout error");
    assert!(
        start.elapsed() < Duration::from_secs(2),
        "should time out quickly"
    );
    // The surfaced error must be a descriptive TimedOut, never the raw
    // EAGAIN/WouldBlock ("os error 11") from the socket.
    let err = result.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("silence budget"),
        "expected the silence-budget error, got: {msg}"
    );
    drop(stop);
    server.join().unwrap();
}
