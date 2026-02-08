use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use quetzalcoatl::spsc::{Consumer, Producer};

use crate::error::IoError;
use crate::response::{BodyFraming, ChunkedDecoder, DecodeResult};
use crate::stream::Stream;

const BLOCK_SIZE: usize = 8192;

#[derive(Clone, Debug)]
pub struct IoBlock {
    pub data: [u8; BLOCK_SIZE],
    pub len: usize, // 0 = EOF sentinel
}

impl Default for IoBlock {
    fn default() -> Self {
        Self {
            data: [0; BLOCK_SIZE],
            len: 0,
        }
    }
}

pub(crate) fn reader_thread(
    mut stream: Stream,
    producer: &Producer<IoBlock>,
    stop: &Arc<AtomicBool>,
    error: &Arc<Mutex<Option<IoError>>>,
) {
    use std::io::Read;

    let _ = stream.set_read_timeout(Some(Duration::from_millis(100)));

    loop {
        if stop.load(Ordering::Relaxed) {
            break;
        }

        let mut block = IoBlock::default();
        match stream.read(&mut block.data) {
            Ok(0) => {
                // EOF — push sentinel and exit
                push_with_backoff(producer, IoBlock::default(), stop);
                break;
            }
            Ok(n) => {
                block.len = n;
                if !push_with_backoff(producer, block, stop) {
                    break;
                }
            }
            Err(ref e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut => {}
            Err(e) => {
                if let Ok(mut guard) = error.lock() {
                    *guard = Some(IoError {
                        kind: e.kind(),
                        message: e.to_string(),
                    });
                }
                // Push EOF sentinel so consumer knows we're done
                push_with_backoff(producer, IoBlock::default(), stop);
                break;
            }
        }
    }
}

fn push_with_backoff(producer: &Producer<IoBlock>, block: IoBlock, stop: &Arc<AtomicBool>) -> bool {
    let mut item = block;
    loop {
        match producer.push(item) {
            Ok(()) => return true,
            Err(returned) => {
                if stop.load(Ordering::Relaxed) {
                    return false;
                }
                std::thread::sleep(Duration::from_micros(100));
                item = returned;
            }
        }
    }
}

pub struct BodyReader {
    consumer: Consumer<IoBlock>,
    framing: BodyFraming,
    leftover: Vec<u8>,
    leftover_pos: usize,
    remaining: u64,
    chunked: Option<ChunkedDecoder>,
    error: Arc<Mutex<Option<IoError>>>,
    stop: Arc<AtomicBool>,
    done: bool,
}

impl BodyReader {
    #[allow(clippy::missing_const_for_fn)]
    pub(crate) fn new(
        consumer: Consumer<IoBlock>,
        framing: BodyFraming,
        leftover: Vec<u8>,
        error: Arc<Mutex<Option<IoError>>>,
        stop: Arc<AtomicBool>,
    ) -> Self {
        let remaining = match &framing {
            BodyFraming::ContentLength(n) => *n,
            _ => 0,
        };
        let chunked = match &framing {
            BodyFraming::Chunked => Some(ChunkedDecoder::new()),
            _ => None,
        };
        let done = matches!(framing, BodyFraming::None);
        Self {
            consumer,
            framing,
            leftover,
            leftover_pos: 0,
            remaining,
            chunked,
            error,
            stop,
            done,
        }
    }

    fn check_error(&self) -> std::io::Result<()> {
        if let Ok(guard) = self.error.lock()
            && let Some(e) = guard.as_ref()
        {
            return Err(std::io::Error::new(e.kind, e.message.clone()));
        }
        Ok(())
    }
}

impl std::io::Read for BodyReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.done || buf.is_empty() {
            return Ok(0);
        }

        match &self.framing {
            BodyFraming::None => Ok(0),
            BodyFraming::ContentLength(_) => self.read_content_length(buf),
            BodyFraming::Chunked => self.read_chunked(buf),
            BodyFraming::UntilClose => self.read_until_close(buf),
        }
    }
}

impl BodyReader {
    fn drain_leftover(&mut self, buf: &mut [u8]) -> usize {
        if self.leftover_pos >= self.leftover.len() {
            return 0;
        }
        let available = &self.leftover[self.leftover_pos..];
        let n = available.len().min(buf.len());
        buf[..n].copy_from_slice(&available[..n]);
        self.leftover_pos += n;
        n
    }

    fn next_block_data(&mut self) -> Option<Vec<u8>> {
        loop {
            if let Some(block) = self.consumer.pop() {
                if block.len == 0 {
                    return None; // EOF
                }
                return Some(block.data[..block.len].to_vec());
            }
            self.check_error().ok()?;
            std::thread::sleep(Duration::from_micros(50));
        }
    }

    #[allow(clippy::cast_possible_truncation)]
    fn read_content_length(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.remaining == 0 {
            self.done = true;
            return Ok(0);
        }

        // Limit buf to remaining (saturating to usize::MAX on 32-bit)
        let max = usize::try_from(self.remaining)
            .unwrap_or(usize::MAX)
            .min(buf.len());
        let buf = &mut buf[..max];

        // Try leftover first
        let n = self.drain_leftover(buf);
        if n > 0 {
            self.remaining -= n as u64;
            if self.remaining == 0 {
                self.done = true;
            }
            return Ok(n);
        }

        // Get from ring buffer
        if let Some(data) = self.next_block_data() {
            let n = data.len().min(buf.len());
            buf[..n].copy_from_slice(&data[..n]);
            self.remaining -= n as u64;
            if n < data.len() {
                self.leftover = data;
                self.leftover_pos = n;
            }
            if self.remaining == 0 {
                self.done = true;
            }
            Ok(n)
        } else {
            self.done = true;
            self.check_error()?;
            Ok(0)
        }
    }

    fn read_chunked(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // Try leftover first
        if self.leftover_pos < self.leftover.len() {
            let decoder = self.chunked.as_mut().unwrap();
            let input = &self.leftover[self.leftover_pos..];
            let (result, bytes_used) = decoder.decode(input, buf);
            self.leftover_pos += bytes_used;
            match result {
                DecodeResult::Data(n) => return Ok(n),
                DecodeResult::Done => {
                    self.done = true;
                    return Ok(0);
                }
                DecodeResult::NeedMore => {} // fall through to ring buffer
                DecodeResult::Error(e) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        e.to_string(),
                    ));
                }
            }
        }

        // Get from ring buffer
        loop {
            let Some(data) = self.next_block_data() else {
                self.done = true;
                self.check_error()?;
                return Ok(0);
            };

            let decoder = self.chunked.as_mut().unwrap();
            let (result, bytes_used) = decoder.decode(&data, buf);
            if bytes_used < data.len() {
                self.leftover = data;
                self.leftover_pos = bytes_used;
            }
            match result {
                DecodeResult::Data(n) => return Ok(n),
                DecodeResult::Done => {
                    self.done = true;
                    return Ok(0);
                }
                DecodeResult::NeedMore => {} // loop continues
                DecodeResult::Error(e) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        e.to_string(),
                    ));
                }
            }
        }
    }

    fn read_until_close(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // Try leftover first
        let n = self.drain_leftover(buf);
        if n > 0 {
            return Ok(n);
        }

        // Get from ring buffer
        if let Some(data) = self.next_block_data() {
            let n = data.len().min(buf.len());
            buf[..n].copy_from_slice(&data[..n]);
            if n < data.len() {
                self.leftover = data;
                self.leftover_pos = n;
            }
            Ok(n)
        } else {
            self.done = true;
            self.check_error()?;
            Ok(0)
        }
    }
}

impl Drop for BodyReader {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quetzalcoatl::capacity::Capacity;
    use quetzalcoatl::spsc::RingBuffer;
    use std::io::Read;

    fn make_block(data: &[u8]) -> IoBlock {
        let mut block = IoBlock::default();
        let len = data.len().min(BLOCK_SIZE);
        block.data[..len].copy_from_slice(&data[..len]);
        block.len = len;
        block
    }

    fn eof_block() -> IoBlock {
        IoBlock::default()
    }

    #[test]
    fn content_length_exact() {
        let (producer, consumer) = RingBuffer::<IoBlock>::new(Capacity::exact(16)).split();
        let stop = Arc::new(AtomicBool::new(false));
        let error = Arc::new(Mutex::new(None));

        producer.push(make_block(b"hello")).unwrap();
        producer.push(eof_block()).unwrap();

        let mut reader =
            BodyReader::new(consumer, BodyFraming::ContentLength(5), vec![], error, stop);

        let mut buf = vec![0u8; 64];
        let n = reader.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"hello");

        let n = reader.read(&mut buf).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn content_length_with_leftover() {
        let (producer, consumer) = RingBuffer::<IoBlock>::new(Capacity::exact(16)).split();
        let stop = Arc::new(AtomicBool::new(false));
        let error = Arc::new(Mutex::new(None));

        producer.push(eof_block()).unwrap();

        let mut reader = BodyReader::new(
            consumer,
            BodyFraming::ContentLength(5),
            b"helloextra".to_vec(),
            error,
            stop,
        );

        let mut buf = vec![0u8; 64];
        let n = reader.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"hello");

        let n = reader.read(&mut buf).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn chunked_body() {
        let (producer, consumer) = RingBuffer::<IoBlock>::new(Capacity::exact(16)).split();
        let stop = Arc::new(AtomicBool::new(false));
        let error = Arc::new(Mutex::new(None));

        producer
            .push(make_block(b"5\r\nhello\r\n0\r\n\r\n"))
            .unwrap();
        producer.push(eof_block()).unwrap();

        let mut reader = BodyReader::new(consumer, BodyFraming::Chunked, vec![], error, stop);

        let mut buf = vec![0u8; 64];
        let n = reader.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"hello");

        let n = reader.read(&mut buf).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn chunked_with_leftover() {
        let (producer, consumer) = RingBuffer::<IoBlock>::new(Capacity::exact(16)).split();
        let stop = Arc::new(AtomicBool::new(false));
        let error = Arc::new(Mutex::new(None));

        producer.push(eof_block()).unwrap();

        let mut reader = BodyReader::new(
            consumer,
            BodyFraming::Chunked,
            b"5\r\nhello\r\n0\r\n\r\n".to_vec(),
            error,
            stop,
        );

        let mut buf = vec![0u8; 64];
        let n = reader.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"hello");

        let n = reader.read(&mut buf).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn until_close() {
        let (producer, consumer) = RingBuffer::<IoBlock>::new(Capacity::exact(16)).split();
        let stop = Arc::new(AtomicBool::new(false));
        let error = Arc::new(Mutex::new(None));

        producer.push(make_block(b"hello")).unwrap();
        producer.push(make_block(b" world")).unwrap();
        producer.push(eof_block()).unwrap();

        let mut reader = BodyReader::new(consumer, BodyFraming::UntilClose, vec![], error, stop);

        let mut result = Vec::new();
        let mut buf = vec![0u8; 64];
        loop {
            let n = reader.read(&mut buf).unwrap();
            if n == 0 {
                break;
            }
            result.extend_from_slice(&buf[..n]);
        }
        assert_eq!(result, b"hello world");
    }

    #[test]
    fn no_body() {
        let (_, consumer) = RingBuffer::<IoBlock>::new(Capacity::exact(16)).split();
        let stop = Arc::new(AtomicBool::new(false));
        let error = Arc::new(Mutex::new(None));

        let mut reader = BodyReader::new(consumer, BodyFraming::None, vec![], error, stop);

        let mut buf = vec![0u8; 64];
        let n = reader.read(&mut buf).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn drop_sets_stop_flag() {
        let (_, consumer) = RingBuffer::<IoBlock>::new(Capacity::exact(16)).split();
        let stop = Arc::new(AtomicBool::new(false));
        let error = Arc::new(Mutex::new(None));

        let reader = BodyReader::new(
            consumer,
            BodyFraming::None,
            vec![],
            error,
            Arc::clone(&stop),
        );
        drop(reader);
        assert!(stop.load(Ordering::Relaxed));
    }
}
