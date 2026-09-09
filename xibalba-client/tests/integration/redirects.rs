//! Following redirects: method rewriting, relative resolution, and the hop
//! budget.

use std::io::Read;
use std::io::Write;
use std::net::TcpListener;
use std::thread;

use crate::support::client::TestClient;
use crate::support::server::RequestReader;
use crate::support::server::TestServer;
use xibalba_client::client::Config;
use xibalba_client::proto::error::ConnectionError;
use xibalba_client::proto::error::Error;
use xibalba_client::proto::method::Method;

#[test]
fn redirect_301_followed() {
    let final_resp = b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\ndone";
    let (port, server) = TestServer::redirect(301, "/final", final_resp);
    let mut client = TestClient::connect(port);

    let resp = client.get(b"/start").unwrap();
    assert_eq!(resp.status, xibalba_client::proto::status::StatusCode::OK);
    assert_eq!(resp.text().unwrap(), "done");
    server.join().unwrap();
}

#[test]
fn relative_redirect_resolves_against_current_directory_and_strips_fragment() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        RequestReader::read_head(&mut stream);
        stream
            .write_all(
                b"HTTP/1.1 302 Found\r\nContent-Length: 0\r\nLocation: next?q=1#ignored\r\n\r\n",
            )
            .unwrap();
        stream.flush().unwrap();
        let request = RequestReader::read_head(&mut stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
            .unwrap();
        request
    });

    let mut client = TestClient::connect(port);
    assert_eq!(client.get(b"/dir/start").unwrap().text().unwrap(), "ok");
    let redirected = server.join().unwrap();
    assert!(
        redirected.starts_with(b"GET /dir/next?q=1 HTTP/1.1\r\n"),
        "unexpected redirect target: {}",
        String::from_utf8_lossy(&redirected)
    );
}

#[test]
fn query_only_redirect_preserves_path() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        RequestReader::read_head(&mut stream);
        stream
            .write_all(b"HTTP/1.1 302 Found\r\nContent-Length: 0\r\nLocation: ?page=2\r\n\r\n")
            .unwrap();
        stream.flush().unwrap();
        let request = RequestReader::read_head(&mut stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
            .unwrap();
        request
    });

    let mut client = TestClient::connect(port);
    client.get(b"/items").unwrap();
    let redirected = server.join().unwrap();
    assert!(redirected.starts_with(b"GET /items?page=2 HTTP/1.1\r\n"));
}

#[test]
fn head_redirect_stays_head() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        RequestReader::read_head(&mut stream);
        stream
            .write_all(b"HTTP/1.1 302 Found\r\nContent-Length: 0\r\nLocation: /final\r\n\r\n")
            .unwrap();
        stream.flush().unwrap();
        let request = RequestReader::read_head(&mut stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 99\r\n\r\n")
            .unwrap();
        request
    });

    let mut client = TestClient::connect(port);
    let response = client.request(Method::Head, b"/start", None, None).unwrap();
    assert_eq!(response.text().unwrap(), "");
    let redirected = server.join().unwrap();
    assert!(redirected.starts_with(b"HEAD /final HTTP/1.1\r\n"));
}

#[test]
fn status_304_with_location_is_not_followed() {
    let (port, server) = TestServer::one_shot(
        b"HTTP/1.1 304 Not Modified\r\nLocation: /must-not-follow\r\nContent-Length: 0\r\n\r\n",
    );
    let mut client = TestClient::connect(port);
    let response = client.get(b"/cached").unwrap();
    assert_eq!(
        response.status,
        xibalba_client::proto::status::StatusCode::NOT_MODIFIED
    );
    server.join().unwrap();
}

#[test]
fn redirect_chain() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        // Hop 1: 301 → /hop2
        RequestReader::read_head(&mut stream);
        stream
            .write_all(b"HTTP/1.1 301 Moved\r\nContent-Length: 0\r\nLocation: /hop2\r\n\r\n")
            .unwrap();
        stream.flush().unwrap();

        // Hop 2: 302 → /final
        RequestReader::read_head(&mut stream);
        stream
            .write_all(b"HTTP/1.1 302 Found\r\nContent-Length: 0\r\nLocation: /final\r\n\r\n")
            .unwrap();
        stream.flush().unwrap();

        // Final
        RequestReader::read_head(&mut stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 7\r\n\r\narrived")
            .unwrap();
    });
    let mut client = TestClient::connect(port);
    let resp = client.get(b"/start").unwrap();
    assert_eq!(resp.text().unwrap(), "arrived");
    server.join().unwrap();
}

#[test]
fn redirect_max_exceeded() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        // Infinite redirect loop
        loop {
            let req = RequestReader::read_head(&mut stream);
            if req.is_empty() {
                break;
            }
            let resp = b"HTTP/1.1 301 Moved\r\nContent-Length: 0\r\nLocation: /loop\r\n\r\n";
            if stream.write_all(resp).is_err() {
                break;
            }
            stream.flush().ok();
        }
    });

    let config = Config {
        max_redirects: 3,
        ..Config::default()
    };
    let mut client = TestClient::with_config(port, config);
    let result = client.get(b"/start");
    assert_eq!(
        result.unwrap_err(),
        Error::Connection(ConnectionError::TooManyRedirects)
    );
    drop(server);
}

#[test]
fn redirect_307_preserves_method_and_body() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        // First request: 307 redirect
        let req1 = RequestReader::read_head(&mut stream);
        assert!(String::from_utf8_lossy(&req1).starts_with("POST "));
        stream
            .write_all(b"HTTP/1.1 307 Temporary\r\nContent-Length: 0\r\nLocation: /target\r\n\r\n")
            .unwrap();
        stream.flush().unwrap();
        // Read the body from first request
        let req1_str = String::from_utf8_lossy(&req1);
        let cl: usize = req1_str
            .lines()
            .find(|l| l.to_ascii_lowercase().starts_with("content-length:"))
            .and_then(|l| l.split(':').nth(1))
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(0);
        let head_end = req1.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
        let already = req1.len() - head_end;
        if already < cl {
            let mut rest = vec![0u8; cl - already];
            stream.read_exact(&mut rest).ok();
        }

        // Second request: should still be POST with body
        let req2 = RequestReader::read_head(&mut stream);
        let req2_str = String::from_utf8_lossy(&req2);
        assert!(
            req2_str.starts_with("POST "),
            "expected POST after 307, got: {req2_str}"
        );
        // Read the body
        let cl2: usize = req2_str
            .lines()
            .find(|l| l.to_ascii_lowercase().starts_with("content-length:"))
            .and_then(|l| l.split(':').nth(1))
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(0);
        let head_end2 = req2.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
        let already2 = req2.len() - head_end2;
        let mut body2 = req2[head_end2..].to_vec();
        if already2 < cl2 {
            let mut rest = vec![0u8; cl2 - already2];
            stream.read_exact(&mut rest).ok();
            body2.extend_from_slice(&rest);
        }
        body2.truncate(cl2);

        let resp = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body2.len());
        stream.write_all(resp.as_bytes()).unwrap();
        stream.write_all(&body2).unwrap();
    });

    let mut client = TestClient::connect(port);
    let resp = client.post(b"/original", b"preserved").unwrap();
    assert_eq!(resp.text().unwrap(), "preserved");
    server.join().unwrap();
}

/// `allow_cross_origin_redirects: false` must refuse the hop before it is
/// ever dialled, not merely fail once connected: the target here has no
/// script registered, so a dial attempt would panic the connector rather
/// than surface `CrossOriginRedirectRefused`.
#[test]
fn cross_origin_redirects_can_be_refused_before_they_are_dialled() {
    use crate::support::registry::ScriptedServer;
    use crate::support::script::Script;

    let origin = ScriptedServer::serving_one(Script::new().expect_request().send(
        b"HTTP/1.1 302 Found\r\nContent-Length: 0\r\nLocation: http://elsewhere.example/target\r\n\r\n".to_vec(),
    ));

    let config = Config {
        allow_cross_origin_redirects: false,
        ..Config::default()
    };
    let mut client = TestClient::scripted_with_config(&origin, config);

    let error = client
        .get(b"/start")
        .expect_err("a cross-origin hop must be refused, not followed");
    assert_eq!(
        error,
        Error::Connection(ConnectionError::CrossOriginRedirectRefused)
    );
}

/// The converse of the refusal: a same-origin redirect is unaffected by
/// `allow_cross_origin_redirects`, so the flag cannot be mistaken for a
/// blanket "never redirect".
#[test]
fn same_origin_redirects_are_unaffected_by_the_cross_origin_refusal() {
    let (port, server) = TestServer::redirect(
        301,
        "/final",
        b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\ndone",
    );
    let config = Config {
        allow_cross_origin_redirects: false,
        ..Config::default()
    };
    let mut client = TestClient::with_config(port, config);

    let resp = client
        .get(b"/start")
        .expect("a same-origin redirect must still be followed");
    assert_eq!(resp.text().unwrap(), "done");
    server.join().unwrap();
}
