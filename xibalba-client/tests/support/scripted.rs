//! An in-memory stream that plays a [`Script`].

use std::io::{Read, Write};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use xibalba_client::connector::SetReadTimeout;

use super::script::{Script, Step};

/// The bytes a client wrote to a scripted connection.
///
/// Shared with the test so a request can be asserted on after the exchange,
/// and shared with the stream so [`Step::ExpectRequest`] can consume one
/// request head at a time.
#[derive(Debug, Clone, Default)]
pub(crate) struct Written(Arc<Mutex<Vec<u8>>>);

impl Written {
    /// Everything written so far.
    pub(crate) fn bytes(&self) -> Vec<u8> {
        self.0.lock().expect("written buffer poisoned").clone()
    }

    /// Everything written so far, as text.
    pub(crate) fn text(&self) -> String {
        String::from_utf8_lossy(&self.bytes()).into_owned()
    }

    fn append(&self, bytes: &[u8]) {
        self.0
            .lock()
            .expect("written buffer poisoned")
            .extend_from_slice(bytes);
    }

    /// The offset just past the first request head ending at or after `from`,
    /// or `None` while the head is still incomplete.
    fn head_end(&self, from: usize) -> Option<usize> {
        let buf = self.0.lock().expect("written buffer poisoned");
        buf.get(from..)?
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .map(|pos| from + pos + 4)
    }
}

/// How far a [`ScriptedStream`] got through its script, and how many reads it
/// took.
///
/// A script that stops early means the client did not behave as the test
/// assumed — it read fewer times than the script describes. That is invisible
/// unless the test checks, so [`ScriptedStream::progress`] hands back a
/// counter the test can assert on.
///
/// The read count exists because a barrier is otherwise unfalsifiable. Delete
/// a [`Step::AwaitRead`] and the bytes arrive in one read instead of two; the
/// response is identical, every assertion about it still holds, and the test
/// silently stops exercising the split it was written for. Counting delivered
/// reads gives a test a way to require the fragmentation it depends on.
#[derive(Debug, Clone, Default)]
pub(crate) struct Progress(Arc<Mutex<ProgressState>>);

#[derive(Debug, Default)]
struct ProgressState {
    completed: usize,
    data_reads: usize,
}

impl Progress {
    /// How many steps have been completed.
    pub(crate) fn completed(&self) -> usize {
        self.0.lock().expect("progress poisoned").completed
    }

    /// How many reads returned data, so how finely the response was split.
    pub(crate) fn data_reads(&self) -> usize {
        self.0.lock().expect("progress poisoned").data_reads
    }

    fn record(&self, step: usize) {
        self.0.lock().expect("progress poisoned").completed = step;
    }

    fn record_read(&self) {
        self.0.lock().expect("progress poisoned").data_reads += 1;
    }
}

/// A bidirectional stream whose read side is driven by a [`Script`].
///
/// Reads pull from the script, so the client's own consumption drives the
/// exchange forward and no step can run before the client is ready for it.
/// This is what removes the sleeps: the ordering a sleep was approximating is
/// the ordering the pull model enforces.
///
/// Each [`Step::AwaitRead`] is a barrier that a single `read` will not cross.
/// That matters more than it looks: without it, two consecutive sends can be
/// coalesced into one `read`, and a test meaning to exercise "the next chunk
/// has not arrived yet" silently stops doing so. The barrier makes the
/// fragmentation part of the test rather than an accident of buffering.
pub(crate) struct ScriptedStream {
    script: Script,
    step: usize,
    pending: Vec<u8>,
    consumed_request: usize,
    stall_until: Option<Instant>,
    closed: bool,
    written: Written,
    progress: Progress,
}

impl ScriptedStream {
    /// A stream that plays `script`.
    pub(crate) fn new(script: Script) -> Self {
        Self {
            script,
            step: 0,
            pending: Vec::new(),
            consumed_request: 0,
            stall_until: None,
            closed: false,
            written: Written::default(),
            progress: Progress::default(),
        }
    }

    /// Record into handles the caller already holds.
    ///
    /// A registered script hands its observation points to the test before
    /// any connection exists, so the stream adopts those rather than issuing
    /// its own and leaving the test watching a buffer nothing writes to.
    pub(crate) fn adopt(&mut self, written: Written, progress: Progress) {
        self.written = written;
        self.progress = progress;
    }

    /// The buffer recording what the client wrote.
    pub(crate) fn written(&self) -> Written {
        self.written.clone()
    }

    /// The counter recording how far the script got.
    pub(crate) fn progress(&self) -> Progress {
        self.progress.clone()
    }

    /// The tick a silent peer produces, which the client absorbs against its
    /// silence budget rather than surfacing.
    fn tick() -> std::io::Error {
        std::io::Error::new(std::io::ErrorKind::WouldBlock, "scripted stall")
    }

    /// Fill `pending` from the script, stopping where the client must act
    /// before the exchange can continue.
    ///
    /// Consecutive sends accumulate, so a script that does not ask for
    /// fragmentation does not get it. Everything else — a barrier, a stall, a
    /// request to wait for — first delivers whatever is already buffered,
    /// because the client cannot satisfy those conditions while bytes it has
    /// not seen are still queued.
    ///
    /// `false` means the caller should return a tick rather than data.
    fn advance(&mut self) -> bool {
        while !self.closed {
            let Some(step) = self.script.steps().get(self.step).cloned() else {
                self.closed = true;
                break;
            };

            if !matches!(step, Step::Send(_)) && !self.pending.is_empty() {
                return true;
            }

            match step {
                Step::Send(bytes) => self.pending.extend_from_slice(&bytes),
                Step::AwaitRead => {}
                Step::Close => self.closed = true,
                Step::Stall(dur) => {
                    let deadline = *self.stall_until.get_or_insert_with(|| Instant::now() + dur);
                    if Instant::now() < deadline {
                        return false;
                    }
                    self.stall_until = None;
                }
                Step::ExpectRequest => match self.written.head_end(self.consumed_request) {
                    Some(end) => self.consumed_request = end,
                    None => return false,
                },
            }

            self.step += 1;
            self.progress.record(self.step);
        }
        true
    }
}

impl Read for ScriptedStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if !self.advance() {
            return Err(Self::tick());
        }
        if self.pending.is_empty() {
            return Ok(0);
        }
        let n = self.pending.len().min(buf.len());
        buf[..n].copy_from_slice(&self.pending[..n]);
        self.pending.drain(..n);
        self.progress.record_read();
        Ok(n)
    }
}

impl Write for ScriptedStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.written.append(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl SetReadTimeout for ScriptedStream {
    fn set_read_timeout(&self, _dur: Option<Duration>) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read_once(stream: &mut ScriptedStream) -> std::io::Result<Vec<u8>> {
        let mut buf = [0u8; 256];
        let n = stream.read(&mut buf)?;
        Ok(buf[..n].to_vec())
    }

    /// The barrier's whole purpose: a reader with room for everything still
    /// gets only what was sent before the barrier.
    #[test]
    fn a_barrier_stops_two_sends_coalescing_into_one_read() {
        let script = Script::new()
            .send_then_await(b"first".to_vec())
            .send(b"second".to_vec());
        let mut stream = ScriptedStream::new(script);

        assert_eq!(read_once(&mut stream).unwrap(), b"first");
        assert_eq!(read_once(&mut stream).unwrap(), b"second");
    }

    #[test]
    fn sends_without_a_barrier_may_arrive_together() {
        let script = Script::new().send(b"ab".to_vec()).send(b"cd".to_vec());
        let mut stream = ScriptedStream::new(script);

        assert_eq!(read_once(&mut stream).unwrap(), b"abcd");
    }

    #[test]
    fn a_closed_script_reads_as_eof() {
        let mut stream = ScriptedStream::new(Script::new().send(b"x".to_vec()).close());

        assert_eq!(read_once(&mut stream).unwrap(), b"x");
        assert_eq!(read_once(&mut stream).unwrap(), b"");
    }

    #[test]
    fn an_exhausted_script_reads_as_eof() {
        let mut stream = ScriptedStream::new(Script::new().send(b"x".to_vec()));

        assert_eq!(read_once(&mut stream).unwrap(), b"x");
        assert_eq!(read_once(&mut stream).unwrap(), b"");
    }

    #[test]
    fn a_stall_ticks_until_it_expires_then_delivers() {
        let script = Script::new()
            .stall(Duration::from_millis(20))
            .send(b"late".to_vec());
        let mut stream = ScriptedStream::new(script);

        let err = read_once(&mut stream).expect_err("a stall must tick");
        assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock);

        std::thread::sleep(Duration::from_millis(25));
        assert_eq!(read_once(&mut stream).unwrap(), b"late");
    }

    #[test]
    fn expect_request_ticks_until_a_head_is_written() {
        let script = Script::new().expect_request().send(b"resp".to_vec());
        let mut stream = ScriptedStream::new(script);

        let err = read_once(&mut stream).expect_err("no request written yet");
        assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock);

        stream
            .write_all(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n")
            .unwrap();
        assert_eq!(read_once(&mut stream).unwrap(), b"resp");
    }

    #[test]
    fn each_expect_request_consumes_only_its_own_head() {
        let script = Script::new()
            .expect_request()
            .send_then_await(b"one".to_vec())
            .expect_request()
            .send(b"two".to_vec());
        let mut stream = ScriptedStream::new(script);

        stream.write_all(b"GET /a HTTP/1.1\r\n\r\n").unwrap();
        assert_eq!(read_once(&mut stream).unwrap(), b"one");

        let err = read_once(&mut stream).expect_err("second request not written yet");
        assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock);

        stream.write_all(b"GET /b HTTP/1.1\r\n\r\n").unwrap();
        assert_eq!(read_once(&mut stream).unwrap(), b"two");
    }

    #[test]
    fn writes_are_recorded_for_the_test_to_assert_on() {
        let mut stream = ScriptedStream::new(Script::new());
        let written = stream.written();

        stream.write_all(b"GET / HTTP/1.1\r\n").unwrap();
        stream.write_all(b"Host: example\r\n\r\n").unwrap();

        assert_eq!(written.text(), "GET / HTTP/1.1\r\nHost: example\r\n\r\n");
    }

    /// A barrier counts as completed only once the client has drained what
    /// preceded it — that consumption is the event it describes, so counting
    /// it any earlier would report progress the client had not made.
    #[test]
    fn a_barrier_completes_only_after_the_client_drains_the_send() {
        let script = Script::new().send_then_await(b"a".to_vec()).send(b"b");
        let mut stream = ScriptedStream::new(script);
        let progress = stream.progress();

        assert_eq!(progress.completed(), 0);
        read_once(&mut stream).unwrap();
        assert_eq!(progress.completed(), 1, "the send ran; the barrier has not");
        read_once(&mut stream).unwrap();
        assert_eq!(progress.completed(), 3);
    }

    /// The guard that makes a barrier falsifiable: with one, the payload takes
    /// two reads; without, a single read swallows it whole.
    #[test]
    fn a_barrier_forces_a_second_read_and_the_count_shows_it() {
        let barriered = Script::new().send_then_await(b"aa".to_vec()).send(b"bb");
        let mut stream = ScriptedStream::new(barriered);
        let progress = stream.progress();
        while !read_once(&mut stream).unwrap().is_empty() {}
        assert_eq!(progress.data_reads(), 2);

        let flat = Script::new().send(b"aa".to_vec()).send(b"bb");
        let mut stream = ScriptedStream::new(flat);
        let progress = stream.progress();
        while !read_once(&mut stream).unwrap().is_empty() {}
        assert_eq!(progress.data_reads(), 1);
    }

    /// Buffered bytes reach the client before a stall begins; a stall cannot
    /// hide data the script already queued.
    #[test]
    fn pending_bytes_are_delivered_before_a_stall_starts() {
        let script = Script::new()
            .send(b"early".to_vec())
            .stall(Duration::from_secs(30));
        let mut stream = ScriptedStream::new(script);

        assert_eq!(read_once(&mut stream).unwrap(), b"early");
        let err = read_once(&mut stream).expect_err("the stall follows the send");
        assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock);
    }

    /// A read smaller than the pending bytes must not drop the remainder.
    #[test]
    fn a_short_buffer_takes_a_prefix_and_keeps_the_rest() {
        let mut stream = ScriptedStream::new(Script::new().send(b"abcdef".to_vec()));

        let mut buf = [0u8; 2];
        assert_eq!(stream.read(&mut buf).unwrap(), 2);
        assert_eq!(&buf, b"ab");
        assert_eq!(read_once(&mut stream).unwrap(), b"cdef");
    }
}
