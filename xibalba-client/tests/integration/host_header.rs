//! The Host header the client derives from the request URL.

use crate::support::client::TestClient;
use crate::support::server::TestServer;

#[test]
fn host_header_sent() {
    let (port, server) = TestServer::echo_request();
    let mut client = TestClient::connect(port);
    client.get(b"/test").unwrap();
    let req = server.join().unwrap();
    let req_str = String::from_utf8_lossy(&req);
    assert!(
        req_str.contains(&format!("Host: 127.0.0.1:{port}")),
        "expected Host header with port, got:\n{req_str}"
    );
}

#[test]
fn host_header_with_default_port() {
    let (port, server) = TestServer::echo_request();
    // Non-default port so Host should include it
    let mut client = TestClient::connect(port);
    client.get(b"/").unwrap();
    let req = server.join().unwrap();
    let req_str = String::from_utf8_lossy(&req);
    assert!(
        req_str.contains("Host: 127.0.0.1:"),
        "Host header must include non-default port"
    );
}
