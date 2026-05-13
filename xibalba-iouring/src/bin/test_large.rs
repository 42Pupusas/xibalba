use std::io::{Read, Write};
use std::net::TcpListener;

use xibalba_iouring::driver::Pool;

fn main() {
    const N: usize = 65536;
    let mut resp_bytes =
        format!("HTTP/1.1 200 OK\r\nContent-Length: {N}\r\nConnection: keep-alive\r\n\r\n")
            .into_bytes();
    resp_bytes.extend(std::iter::repeat_n(b'x', N));
    let resp_static: &'static [u8] = Box::leak(resp_bytes.into_boxed_slice());

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();

    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut s) = stream else { continue };
            std::thread::spawn(move || {
                let mut buf = [0u8; 4096];
                loop {
                    let n = s.read(&mut buf).unwrap_or(0);
                    if n == 0 { break; }
                    if s.write_all(resp_static).is_err() { break; }
                }
            });
        }
    });

    let url = format!("http://127.0.0.1:{port}");
    let mut pool = Pool::<256, 64, 8192, 8192>::new().unwrap();
    let conn = pool.connect(url.as_bytes()).unwrap();

    for i in 0..5 {
        let id = pool.get(conn, b"/").unwrap_or_else(|e| panic!("iter {i}: get: {e}"));
        let resp = match pool.recv(id).unwrap_or_else(|e| panic!("iter {i}: recv: {e}")) {
            xibalba_iouring::driver::ConnResult::Response(r) => r,
            xibalba_iouring::driver::ConnResult::Error { request_id, errno, .. } =>
                panic!("iter {i}: request {request_id} failed: errno {errno}"),
            xibalba_iouring::driver::ConnResult::Timeout => panic!("iter {i}: timed out"),
        };
        assert_eq!(resp.request_id, id);
        assert_eq!(resp.body.len(), N, "iter {i}: wrong body length {}", resp.body.len());
        println!("iter {i}: ok ({N} bytes)");
    }
    println!("all ok");
}
