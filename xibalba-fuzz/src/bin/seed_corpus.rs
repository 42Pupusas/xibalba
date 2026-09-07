use std::path::PathBuf;
use std::process::ExitCode;
use xibalba_fuzz::{Seeds, Surface};

/// Writes each surface's seeds into `fuzz/corpus/<target>/`, which is where
/// `cargo fuzz run` looks for them.
struct Seeding {
    root: PathBuf,
}

impl Seeding {
    fn from_args() -> Self {
        let root = std::env::args().nth(1).map_or_else(
            || {
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .parent()
                    .expect("xibalba-fuzz sits one level below the workspace root")
                    .join("fuzz")
                    .join("corpus")
            },
            PathBuf::from,
        );
        Self { root }
    }

    fn run(&self) -> ExitCode {
        for surface in Surface::all() {
            let dir = self.root.join(surface.name());
            match Seeds::write(surface, &dir) {
                Ok(count) => println!("{}: {count} seeds -> {}", surface.name(), dir.display()),
                Err(error) => {
                    eprintln!("{}: {error}", dir.display());
                    return ExitCode::FAILURE;
                }
            }
        }
        ExitCode::SUCCESS
    }
}

fn main() -> ExitCode {
    Seeding::from_args().run()
}
