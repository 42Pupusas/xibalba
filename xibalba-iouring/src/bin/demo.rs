use std::io::{Read, Write};
use std::net::TcpListener;

use xibalba_iouring::driver::{ConnResult, Pool};

fn spawn_server() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        let resp = b"HTTP/1.1 200 OK\r\nContent-Length: 13\r\nConnection: keep-alive\r\n\r\nHello, World!";
        for stream in listener.incoming() {
            let Ok(mut s) = stream else { continue };
            std::thread::spawn(move || {
                let mut buf = vec![0u8; 4096];
                let mut acc = Vec::new();
                let mut req_num = 0u32;
                'conn: loop {
                    loop {
                        let n = s.read(&mut buf).unwrap_or(0);
                        if n == 0 { break 'conn; }
                        acc.extend_from_slice(&buf[..n]);
                        if acc.windows(4).any(|w| w == b"\r\n\r\n") { break; }
                    }
                    eprintln!("[server] sending response {req_num}");
                    if s.write_all(resp).is_err() { break; }
                    req_num += 1;
                    acc.clear();
                }
            });
        }
    });
    port
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let port = spawn_server();
    let url = format!("http://127.0.0.1:{port}");

    let mut pool = Pool::<256, 64, 8192, 8192>::new()?;
    let conn = pool.connect(url.as_bytes())?;

    for i in 0..5 {
        eprintln!("request {i}");
        pool.get(conn, b"/")?;
        let resp = match pool.recv().unwrap_or_else(|e| panic!("recv: {e}")) {
            ConnResult::Response(r) => r,
            ConnResult::Error { request_id, errno, .. } =>
                panic!("request {request_id} failed: errno {errno}"),
            ConnResult::Timeout => panic!("request timed out"),
        };
        eprintln!("response {i}: {} bytes, body={:?}", resp.body.len(), String::from_utf8_lossy(&resp.body));
    }
    eprintln!("done");
    Ok(())
}
