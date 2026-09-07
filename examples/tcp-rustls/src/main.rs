use tcp_rustls::{RustlsConfig, TcpConnector};
use xibalba_client::client::Client;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tls_config = RustlsConfig::with_provider(rustls::crypto::ring::default_provider())?;
    let mut client =
        Client::<TcpConnector>::connect_default(b"https://httpbin.org/get", tls_config)?;
    let response = client.get(b"/get")?;

    println!("Status: {}", response.status);
    println!("Headers:");
    for (name, value) in response.headers() {
        println!(
            "  {}: {}",
            String::from_utf8_lossy(name),
            String::from_utf8_lossy(value)
        );
    }
    println!("\nBody:\n{}", response.text()?);
    Ok(())
}
