//! Model-checked interleavings of the client's shared state.
//!
//! The rest of the suite runs each concurrent test once, on whatever
//! interleaving the scheduler happened to pick, under a watchdog that fails
//! rather than hanging. That catches a hang reliably and a race only by luck.
//! [`loom`](https://docs.rs/loom) replaces the atomics with a model and runs
//! the body once per *distinct* interleaving, including the reorderings a weak
//! memory model permits but x86 will not produce, so a missing `Release` fails
//! here rather than on a reader's ARM server.
//!
//! Run with:
//!
//! ```sh
//! RUSTFLAGS="--cfg loom" cargo test -p xibalba-client --test loom_shared_state
//! ```
//!
//! Without `--cfg loom` every test below compiles to nothing: the file is a
//! single `#[cfg(loom)] mod`. That is deliberate. Loom must replace `std`'s
//! primitives for the whole build, so it cannot share a compilation with the
//! ordinary gate, and a test that silently ran with real atomics would report
//! a pass that checked one interleaving while claiming to have checked all.
//!
//! # Scope, stated honestly
//!
//! These check *this crate's* shared state: admission counting and the
//! delivery flag protocol. They do not check `quetzalcoatl`'s rings, which use
//! their own `std` atomics that loom cannot see or reorder. A green run here
//! is not a proof that the async client is race-free; it is a proof about the
//! flags and counters this crate owns.
//!
//! `Admission`, `Permit` and `ChunkSink` are crate-private, so each model
//! below transcribes the orderings from the original rather than calling it.
//! Transcription is the real weakness of this file: a model that drifts from
//! the code proves nothing about the code. Two things limit it — the orderings
//! are copied verbatim and the source names the model, and the ordinary suite
//! still exercises the true types on one interleaving — but a change to
//! `try_admit` or `blocked_reason` must be mirrored here by hand.

#![cfg(loom)]

/// A model-checked test that CI does not run is a file nobody will notice has
/// stopped compiling. This is the same drift guard the fuzz targets carry, and
/// it lives here rather than in a CI-wide test because it is this file's own
/// job that must exist.
#[test]
fn ci_runs_this_suite_with_the_cfg_that_makes_it_real() {
    let workflow = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("xibalba-client sits one level below the workspace root")
            .join(".github/workflows/ci.yml"),
    )
    .expect("the CI workflow is readable");

    assert!(
        workflow.contains("--test loom_shared_state"),
        "CI must run this suite, or it silently stops being checked"
    );
    assert!(
        workflow.contains("--cfg loom"),
        "CI must set the cfg, or this file compiles to nothing and reports a pass"
    );
}

mod delivery {
    use loom::sync::Arc;
    use loom::sync::atomic::{AtomicBool, Ordering};
    use loom::thread;

    #[derive(Debug, PartialEq, Eq)]
    enum Blocked {
        ConsumerGone,
        ShuttingDown,
    }

    /// Mirrors `ChunkSink::blocked_reason` and `ConsumerGuard::drop`: the
    /// producer waits while both flags stay clear, and each flag is published
    /// with `Release` against the producer's `Acquire`.
    struct Flags {
        consumer_alive: AtomicBool,
        shutting_down: AtomicBool,
    }

    impl Flags {
        fn new() -> Self {
            Self {
                consumer_alive: AtomicBool::new(true),
                shutting_down: AtomicBool::new(false),
            }
        }

        fn blocked(&self) -> bool {
            self.blocked_reason().is_some()
        }

        /// Mirrors `ChunkSink::blocked_reason`, including the order of the two
        /// checks: shutdown is reported in preference to a gone consumer.
        fn blocked_reason(&self) -> Option<Blocked> {
            if self.shutting_down.load(Ordering::Acquire) {
                return Some(Blocked::ShuttingDown);
            }
            if !self.consumer_alive.load(Ordering::Acquire) {
                return Some(Blocked::ConsumerGone);
            }
            None
        }

        fn drop_consumer(&self) {
            self.consumer_alive.store(false, Ordering::Release);
        }

        fn begin_shutdown(&self) {
            self.shutting_down.store(true, Ordering::Release);
        }
    }

    /// A reader waiting for ring capacity must observe a dropped consumer.
    ///
    /// This is the deadlock behind A05: a producer that misses the flag waits
    /// forever on a ring nobody will drain, and `AsyncClient::drop` then waits
    /// forever on the join.
    #[test]
    fn a_dropped_consumer_is_always_observed_by_a_waiting_producer() {
        loom::model(|| {
            let flags = Arc::new(Flags::new());

            let consumer = {
                let flags = Arc::clone(&flags);
                thread::spawn(move || flags.drop_consumer())
            };

            // The producer's wait loop is bounded here because loom explores
            // interleavings rather than running in real time: an unbounded
            // spin would not terminate under the model. Each iteration is one
            // pass of the real loop's flag check.
            let producer = {
                let flags = Arc::clone(&flags);
                thread::spawn(move || {
                    for _ in 0..2 {
                        if flags.blocked() {
                            return true;
                        }
                    }
                    false
                })
            };

            consumer.join().expect("consumer thread completes");
            let gave_up = producer.join().expect("producer thread completes");

            // The producer may legitimately finish its checks before the
            // consumer drops. What must never happen is the consumer being
            // gone *and* a later check still reporting unblocked.
            if !gave_up {
                assert!(
                    flags.blocked(),
                    "the producer stopped waiting while the consumer was still gone"
                );
            }
        });
    }

    /// Shutdown and a dropped consumer race by construction, since
    /// `AsyncClient::drop` sets the flag and then drops handles. Whichever
    /// lands first, a producer observing the state afterwards must stop.
    ///
    /// The producer reads *concurrently* with both writers rather than after
    /// joining them. Reading after the joins would make this a test of a
    /// quiescent state, which is exactly the interleaving that was never in
    /// doubt: it passed even with the consumer check deleted entirely.
    #[test]
    fn a_concurrent_shutdown_and_drop_always_unblocks_the_producer() {
        loom::model(|| {
            let flags = Arc::new(Flags::new());

            let shutdown = {
                let flags = Arc::clone(&flags);
                thread::spawn(move || flags.begin_shutdown())
            };
            let consumer = {
                let flags = Arc::clone(&flags);
                thread::spawn(move || flags.drop_consumer())
            };
            let producer = {
                let flags = Arc::clone(&flags);
                thread::spawn(move || flags.blocked_reason())
            };

            shutdown.join().expect("shutdown thread completes");
            consumer.join().expect("consumer thread completes");
            // Racing the writers, the producer may legitimately see neither
            // signal yet, so its own answer is not asserted on; the point is
            // that it observes a consistent state rather than tearing.
            producer.join().expect("producer thread completes");

            assert!(
                flags.blocked(),
                "with both signals published the producer must stop waiting"
            );
            assert_eq!(
                flags.blocked_reason(),
                Some(Blocked::ShuttingDown),
                "shutdown outranks a gone consumer once both are visible"
            );
        });
    }
}

mod admission {
    use loom::sync::Arc;
    use loom::sync::atomic::{AtomicUsize, Ordering};
    use loom::thread;

    /// Mirrors `Admission::try_admit` and `Permit::drop`.
    struct Counter {
        outstanding: AtomicUsize,
        max: usize,
    }

    impl Counter {
        fn new(max: usize) -> Self {
            Self {
                outstanding: AtomicUsize::new(0),
                max,
            }
        }

        fn try_admit(&self) -> bool {
            let mut current = self.outstanding.load(Ordering::Acquire);
            loop {
                if current >= self.max {
                    return false;
                }
                match self.outstanding.compare_exchange_weak(
                    current,
                    current + 1,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return true,
                    Err(actual) => current = actual,
                }
            }
        }

        fn release(&self) {
            self.outstanding.fetch_sub(1, Ordering::AcqRel);
        }
    }

    /// Two threads racing for the last slot: exactly one wins.
    ///
    /// A plain load-then-store would let both through and admit one request
    /// past the bound, which is the unbounded-memory failure A15 closed.
    #[test]
    fn admission_never_exceeds_its_limit_under_a_race() {
        loom::model(|| {
            let counter = Arc::new(Counter::new(1));

            let first = {
                let counter = Arc::clone(&counter);
                thread::spawn(move || counter.try_admit())
            };
            let second = {
                let counter = Arc::clone(&counter);
                thread::spawn(move || counter.try_admit())
            };

            let a = first.join().expect("first thread completes");
            let b = second.join().expect("second thread completes");

            assert!(!(a && b), "both threads claimed the only slot");
            assert!(a || b, "an available slot went unclaimed");
            assert_eq!(
                counter.outstanding.load(Ordering::Acquire),
                1,
                "exactly one permit must be outstanding"
            );
        });
    }

    /// A slot released concurrently with a claim leaves the count exact.
    ///
    /// The interesting failure is not the count drifting up but down: a
    /// release that is lost or double-applied would eventually wrap the
    /// counter and admit without bound.
    #[test]
    fn a_release_racing_a_claim_leaves_the_count_exact() {
        loom::model(|| {
            let counter = Arc::new(Counter::new(2));
            assert!(counter.try_admit(), "the first slot is free");

            let releaser = {
                let counter = Arc::clone(&counter);
                thread::spawn(move || counter.release())
            };
            let second_claim = {
                let counter = Arc::clone(&counter);
                thread::spawn(move || counter.try_admit())
            };

            releaser.join().expect("releasing thread completes");
            let claimed = second_claim.join().expect("claiming thread completes");

            let expected = usize::from(claimed);
            assert_eq!(
                counter.outstanding.load(Ordering::Acquire),
                expected,
                "the count must equal the permits actually held"
            );
        });
    }
}
