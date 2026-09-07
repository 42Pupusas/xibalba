//! Guards the workspace's stated minimum supported Rust version.
//!
//! The MSRV is a promise to consumers: raising it is a breaking change, so the
//! number must not drift silently. It lives in one place — `[workspace.package]`
//! in the root manifest — and every member inherits it. These tests fail when a
//! member reintroduces its own, which is how a floor quietly diverges.
//!
//! Whether the code actually *builds* on that version is not something a test
//! can answer, since it runs on whatever toolchain compiled it. That question
//! belongs to the `msrv` CI job, which installs the pinned version and builds
//! the workspace with it. This file guards the declaration; CI guards the fact.

use std::{fs, path::PathBuf};

/// One member's manifest, read as text so an inherited key is distinguishable
/// from a literal one — a parsed value would report the resolved number for
/// both and miss the drift entirely.
struct MemberManifest {
    name: &'static str,
    text: String,
}

impl MemberManifest {
    /// Workspace members only. `examples/tls-providers` is deliberately excluded
    /// from the workspace and cannot inherit anything, so it is not listed here;
    /// its own floor is checked by building it in CI.
    const MEMBERS: &'static [&'static str] = &[
        "xibalba-proto",
        "xibalba-client",
        "xibalba-iouring",
        "xibalba-benches",
        "xibalba-fuzz",
        "examples/tcp-rustls",
    ];

    fn workspace_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("xibalba-proto sits one level below the workspace root")
            .to_path_buf()
    }

    fn load_all() -> Vec<Self> {
        Self::MEMBERS
            .iter()
            .map(|name| {
                let path = Self::workspace_root().join(name).join("Cargo.toml");
                Self {
                    name,
                    text: fs::read_to_string(&path)
                        .unwrap_or_else(|e| panic!("read {}: {e}", path.display())),
                }
            })
            .collect()
    }

    fn load_workspace() -> Self {
        let path = Self::workspace_root().join("Cargo.toml");
        Self {
            name: "workspace root",
            text: fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("read {}: {e}", path.display())),
        }
    }

    fn value_of(&self, key: &str) -> Option<String> {
        self.text
            .lines()
            .map(str::trim)
            .filter(|line| !line.starts_with('#'))
            .find_map(|line| {
                let rest = line.strip_prefix(key)?;
                let rest = rest.trim_start();
                let value = rest.strip_prefix('=')?.trim();
                Some(value.trim_matches('"').to_owned())
            })
    }

    fn inherits(&self, key: &str) -> bool {
        self.text
            .lines()
            .map(str::trim)
            .any(|line| line == format!("{key}.workspace = true"))
    }
}

#[test]
fn every_member_inherits_the_workspace_rust_version() {
    for manifest in MemberManifest::load_all() {
        assert!(
            manifest.inherits("rust-version"),
            "{} declares its own rust-version instead of inheriting it. \
             The MSRV is one promise made once, in [workspace.package]; a second \
             copy is a floor that can drift without anyone noticing.",
            manifest.name
        );
    }
}

#[test]
fn every_member_inherits_the_workspace_edition() {
    for manifest in MemberManifest::load_all() {
        assert!(
            manifest.inherits("edition"),
            "{} declares its own edition instead of inheriting it. \
             The edition sets a language floor of its own and belongs beside \
             the MSRV.",
            manifest.name
        );
    }
}

#[test]
fn the_workspace_states_an_msrv_and_ci_pins_the_same_one() {
    let workspace = MemberManifest::load_workspace();
    let declared = workspace
        .value_of("rust-version")
        .expect("the workspace must state an MSRV in [workspace.package]");

    let ci_path = MemberManifest::workspace_root().join(".github/workflows/ci.yml");
    let ci = fs::read_to_string(&ci_path).unwrap_or_else(|e| panic!("read CI workflow: {e}"));

    // An MSRV nothing builds against is a number in a file. The job that proves
    // it must use this exact version, so the two cannot drift apart.
    assert!(
        ci.contains(&format!("MSRV: \"{declared}\"")),
        "the workspace declares MSRV {declared}, but the CI workflow does not \
         pin that version. The declaration is only worth what the job proves."
    );
}
