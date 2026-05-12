fn main() {
    let client = xibalba::client::Client::new().unwrap();
    let mut response = client.get(b"https://httpbin.org/get").unwrap();
    println!("Status: {}", response.status);
    println!("Headers:");
    for (name, value) in &response.headers {
        println!(
            "  {}: {}",
            String::from_utf8_lossy(name),
            String::from_utf8_lossy(value)
        );
    }
    let body = response.text().unwrap();
    println!("\nBody:\n{body}");
}
