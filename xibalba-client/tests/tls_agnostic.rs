//! Guards the crate's TLS-agnostic contract.
//!
//! `xibalba-client` reaches TLS only through the `Connector` trait, so adding a
//! TLS or crypto dependency to its manifest would silently bind every consumer
//! to one stack. These tests fail when that happens.

struct Manifest {
    text: String,
}

impl Manifest {
    const FORBIDDEN: &'static [&'static str] = &[
        "rustls",
        "native-tls",
        "openssl",
        "boring",
        "ring",
        "aws-lc-rs",
        "aws-lc-sys",
        "schannel",
        "security-framework",
        "webpki",
        "tls-api",
        "s2n-tls",
    ];

    fn load() -> Self {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml");
        Self {
            text: std::fs::read_to_string(path).expect("read xibalba-client/Cargo.toml"),
        }
    }

    fn dependency_names(&self) -> Vec<String> {
        let mut names = Vec::new();
        let mut in_deps = false;
        for line in self.text.lines() {
            let line = line.trim();
            if line.starts_with('[') {
                in_deps = line.contains("dependencies");
                continue;
            }
            if !in_deps || line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Some((name, _)) = line.split_once('=') {
                names.push(name.trim().trim_matches('"').to_owned());
            }
        }
        names
    }

    fn feature_lines(&self) -> Vec<String> {
        let mut lines = Vec::new();
        let mut in_features = false;
        for line in self.text.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with('[') {
                in_features = trimmed == "[features]";
                continue;
            }
            if in_features && !trimmed.is_empty() && !trimmed.starts_with('#') {
                lines.push(trimmed.to_owned());
            }
        }
        lines
    }
}

#[test]
fn client_manifest_declares_no_tls_or_crypto_dependency() {
    let manifest = Manifest::load();
    for dep in manifest.dependency_names() {
        let lowered = dep.to_ascii_lowercase();
        for forbidden in Manifest::FORBIDDEN {
            assert!(
                !lowered.contains(forbidden),
                "xibalba-client must stay TLS-agnostic, but its manifest depends on `{dep}`. \
                 TLS belongs behind the `Connector` trait, chosen by the application."
            );
        }
    }
}

#[test]
fn client_exposes_no_feature_selecting_a_crypto_provider() {
    let manifest = Manifest::load();
    for line in manifest.feature_lines() {
        let lowered = line.to_ascii_lowercase();
        for forbidden in Manifest::FORBIDDEN {
            assert!(
                !lowered.contains(forbidden),
                "xibalba-client must not expose a feature selecting a crypto provider, \
                 but found `{line}`. The provider is the application's choice."
            );
        }
    }
}

#[test]
fn connector_tls_config_is_fully_caller_defined() {
    struct Plaintext;
    struct PlainStream(std::net::TcpStream);

    impl std::io::Read for PlainStream {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            self.0.read(buf)
        }
    }

    impl std::io::Write for PlainStream {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.write(buf)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            self.0.flush()
        }
    }

    impl xibalba_client::connector::SetReadTimeout for PlainStream {
        fn set_read_timeout(&self, dur: Option<std::time::Duration>) -> std::io::Result<()> {
            self.0.set_read_timeout(dur)
        }
    }

    impl xibalba_client::connector::Connector for Plaintext {
        type Stream = PlainStream;
        type TlsConfig = ();

        fn connect(
            _url: &xibalba_proto::url::Url<'_>,
            _tls_config: &(),
            _deadline: xibalba_client::Deadline,
        ) -> Result<Self::Stream, xibalba_proto::error::Error> {
            unreachable!("compile-time proof only")
        }
    }

    let _ = std::marker::PhantomData::<Plaintext>;
}
