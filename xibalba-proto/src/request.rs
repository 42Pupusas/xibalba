use crate::error::{Error, SerializeError};
use crate::header::{Header, Tchar};
use crate::method::{Method, Token};
use crate::version::Version;

/// A request to be serialized. All data is borrowed.
pub struct Request<'a> {
    pub method: Method,
    pub path: &'a [u8],
    pub query: Option<&'a [u8]>,
    pub version: Version,
    pub headers: &'a [Header<'a>],
}

impl Request<'_> {
    /// Reject targets and headers that could corrupt the request line or
    /// smuggle a second request: an empty target, CTLs/space/DEL in the
    /// target, non-token header names, and CTLs (other than HTAB) in
    /// header values.
    fn validate(&self) -> Result<(), Error> {
        if self.path.is_empty() || self.path.iter().any(|&b| !Self::is_target_byte(b)) {
            return Err(SerializeError::InvalidPath.into());
        }
        if let Some(query) = self.query
            && query.iter().any(|&b| !Self::is_target_byte(b))
        {
            return Err(SerializeError::InvalidPath.into());
        }
        for header in self.headers {
            if !header.name.as_bytes().iter().all(|&b| Tchar::is_valid(b)) {
                return Err(SerializeError::InvalidHeader.into());
            }
            if header
                .value
                .iter()
                .any(|&b| b != b'\t' && (b < 0x20 || b == 0x7f))
            {
                return Err(SerializeError::InvalidHeader.into());
            }
        }
        Ok(())
    }

    const fn is_target_byte(b: u8) -> bool {
        b > 0x20 && b != 0x7f
    }

    /// Serialize the request head into the provided buffer.
    ///
    /// # Errors
    ///
    /// Returns `SerializeError::BufferTooSmall` if the buffer cannot hold
    /// the full request head, or `SerializeError::InvalidPath` /
    /// `SerializeError::InvalidHeader` if the target or headers contain
    /// bytes that cannot be serialized safely.
    pub fn serialize_to_buf(&self, buf: &mut [u8]) -> Result<usize, Error> {
        self.validate()?;
        let mut w = BufWriter::new(buf);
        w.write_bytes(self.method.as_bytes())?;
        w.write_byte(b' ')?;
        w.write_bytes(self.path)?;
        if let Some(query) = self.query {
            w.write_byte(b'?')?;
            w.write_bytes(query)?;
        }
        w.write_byte(b' ')?;
        w.write_bytes(self.version.as_bytes())?;
        w.write_bytes(b"\r\n")?;

        for header in self.headers {
            w.write_bytes(header.name.as_bytes())?;
            w.write_bytes(b": ")?;
            w.write_bytes(header.value)?;
            w.write_bytes(b"\r\n")?;
        }

        w.write_bytes(b"\r\n")?;
        Ok(w.pos())
    }

    /// Serialize the request head to an `impl Write`.
    ///
    /// # Errors
    ///
    /// Returns `std::io::Error` on write failure, and
    /// `Error::Serialize` (`InvalidPath` / `InvalidHeader`) when the
    /// target or headers contain bytes that cannot be serialized safely.
    pub fn serialize_to_writer(&self, w: &mut impl std::io::Write) -> std::io::Result<()> {
        self.validate().map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string())
        })?;
        w.write_all(self.method.as_bytes())?;
        w.write_all(b" ")?;
        w.write_all(self.path)?;
        if let Some(query) = self.query {
            w.write_all(b"?")?;
            w.write_all(query)?;
        }
        w.write_all(b" ")?;
        w.write_all(self.version.as_bytes())?;
        w.write_all(b"\r\n")?;

        for header in self.headers {
            w.write_all(header.name.as_bytes())?;
            w.write_all(b": ")?;
            w.write_all(header.value)?;
            w.write_all(b"\r\n")?;
        }

        w.write_all(b"\r\n")
    }

    /// Exact byte count the serialized request head will occupy.
    #[must_use]
    pub fn serialized_len(&self) -> usize {
        let mut len = self.method.as_bytes().len();
        len += 1; // SP
        len += self.path.len();
        if let Some(query) = self.query {
            len += 1; // '?'
            len += query.len();
        }
        len += 1; // SP
        len += self.version.as_bytes().len();
        len += 2; // \r\n

        for header in self.headers {
            len += header.name.as_bytes().len();
            len += 2; // ": "
            len += header.value.len();
            len += 2; // \r\n
        }
        len += 2; // final \r\n
        len
    }
}

/// Cursor over a borrowed serialization buffer. Tracks the current write
/// position and surfaces `BufferTooSmall` errors as the cursor advances.
struct BufWriter<'a> {
    buf: &'a mut [u8],
    pos: usize,
}

impl<'a> BufWriter<'a> {
    const fn new(buf: &'a mut [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    const fn pos(&self) -> usize {
        self.pos
    }

    /// Append `data` at the current position. Errors if it would overflow.
    ///
    /// # Errors
    ///
    /// Returns `SerializeError::BufferTooSmall` when `pos + data.len() > buf.len()`.
    fn write_bytes(&mut self, data: &[u8]) -> Result<(), Error> {
        let end = self.pos + data.len();
        if end > self.buf.len() {
            return Err(SerializeError::BufferTooSmall.into());
        }
        self.buf[self.pos..end].copy_from_slice(data);
        self.pos = end;
        Ok(())
    }

    /// Append a single byte at the current position.
    ///
    /// # Errors
    ///
    /// Returns `SerializeError::BufferTooSmall` when `pos >= buf.len()`.
    fn write_byte(&mut self, byte: u8) -> Result<(), Error> {
        if self.pos >= self.buf.len() {
            return Err(SerializeError::BufferTooSmall.into());
        }
        self.buf[self.pos] = byte;
        self.pos += 1;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::header::HeaderName;

    #[test]
    fn serialize_get_request() {
        let headers = [
            Header {
                name: HeaderName::Host,
                value: b"example.com",
            },
            Header {
                name: HeaderName::UserAgent,
                value: b"xibalba/0.1",
            },
        ];
        let req = Request {
            method: Method::Get,
            path: b"/path",
            query: Some(b"q=1"),
            version: Version::Http11,
            headers: &headers,
        };

        let expected =
            b"GET /path?q=1 HTTP/1.1\r\nHost: example.com\r\nUser-Agent: xibalba/0.1\r\n\r\n";

        let mut buf = [0u8; 256];
        let len = req.serialize_to_buf(&mut buf).unwrap();
        assert_eq!(&buf[..len], &expected[..]);
    }

    #[test]
    fn serialize_post_no_query() {
        let headers = [Header {
            name: HeaderName::Host,
            value: b"example.com",
        }];
        let req = Request {
            method: Method::Post,
            path: b"/api/data",
            query: None,
            version: Version::Http11,
            headers: &headers,
        };

        let expected = b"POST /api/data HTTP/1.1\r\nHost: example.com\r\n\r\n";

        let mut buf = [0u8; 256];
        let len = req.serialize_to_buf(&mut buf).unwrap();
        assert_eq!(&buf[..len], &expected[..]);
    }

    #[test]
    fn serialized_len_matches() {
        let headers = [
            Header {
                name: HeaderName::Host,
                value: b"example.com",
            },
            Header {
                name: HeaderName::Accept,
                value: b"*/*",
            },
        ];
        let req = Request {
            method: Method::Get,
            path: b"/",
            query: None,
            version: Version::Http11,
            headers: &headers,
        };

        let mut buf = [0u8; 256];
        let len = req.serialize_to_buf(&mut buf).unwrap();
        assert_eq!(req.serialized_len(), len);
    }

    #[test]
    fn serialized_len_with_query() {
        let req = Request {
            method: Method::Get,
            path: b"/search",
            query: Some(b"q=rust&page=1"),
            version: Version::Http11,
            headers: &[],
        };

        let mut buf = [0u8; 256];
        let len = req.serialize_to_buf(&mut buf).unwrap();
        assert_eq!(req.serialized_len(), len);
    }

    #[test]
    fn buffer_too_small() {
        let req = Request {
            method: Method::Get,
            path: b"/",
            query: None,
            version: Version::Http11,
            headers: &[],
        };

        let mut buf = [0u8; 5];
        let result = req.serialize_to_buf(&mut buf);
        assert_eq!(
            result.unwrap_err(),
            Error::Serialize(SerializeError::BufferTooSmall)
        );
    }

    #[test]
    fn serialize_to_writer() {
        let headers = [Header {
            name: HeaderName::Host,
            value: b"example.com",
        }];
        let req = Request {
            method: Method::Get,
            path: b"/",
            query: None,
            version: Version::Http11,
            headers: &headers,
        };

        let mut buf = Vec::new();
        req.serialize_to_writer(&mut buf).unwrap();
        assert_eq!(buf, b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n");
    }

    #[test]
    fn serialize_no_headers() {
        let req = Request {
            method: Method::Head,
            path: b"/",
            query: None,
            version: Version::Http10,
            headers: &[],
        };

        let mut buf = [0u8; 64];
        let len = req.serialize_to_buf(&mut buf).unwrap();
        assert_eq!(&buf[..len], b"HEAD / HTTP/1.0\r\n\r\n");
    }

    // ── Adversarial request serialization tests ──────────────────────────────

    #[test]
    fn buffer_exactly_right_size() {
        let req = Request {
            method: Method::Get,
            path: b"/",
            query: None,
            version: Version::Http11,
            headers: &[],
        };
        let exact = req.serialized_len();
        let mut buf = vec![0u8; exact];
        let len = req.serialize_to_buf(&mut buf).unwrap();
        assert_eq!(len, exact);
    }

    #[test]
    fn buffer_one_byte_short() {
        let req = Request {
            method: Method::Get,
            path: b"/",
            query: None,
            version: Version::Http11,
            headers: &[],
        };
        let exact = req.serialized_len();
        let mut buf = vec![0u8; exact - 1];
        assert_eq!(
            req.serialize_to_buf(&mut buf).unwrap_err(),
            Error::Serialize(SerializeError::BufferTooSmall)
        );
    }

    #[test]
    fn many_headers() {
        let headers: Vec<Header<'_>> = (0..20)
            .map(|_| Header {
                name: HeaderName::Accept,
                value: b"*/*",
            })
            .collect();
        let req = Request {
            method: Method::Get,
            path: b"/",
            query: None,
            version: Version::Http11,
            headers: &headers,
        };
        let mut buf = vec![0u8; req.serialized_len()];
        let len = req.serialize_to_buf(&mut buf).unwrap();
        assert_eq!(len, req.serialized_len());
        // Verify all 20 headers appear
        let output = &buf[..len];
        assert_eq!(
            output
                .windows(b"Accept: */*".len())
                .filter(|w| *w == b"Accept: */*")
                .count(),
            20
        );
    }

    #[test]
    fn header_with_empty_value() {
        let headers = [Header {
            name: HeaderName::Accept,
            value: b"",
        }];
        let req = Request {
            method: Method::Get,
            path: b"/",
            query: None,
            version: Version::Http11,
            headers: &headers,
        };
        let mut buf = [0u8; 128];
        let len = req.serialize_to_buf(&mut buf).unwrap();
        assert!(buf[..len].windows(10).any(|w| w == b"Accept: \r\n"));
    }

    #[test]
    fn serialize_to_writer_matches_buf() {
        let headers = [
            Header {
                name: HeaderName::Host,
                value: b"example.com",
            },
            Header {
                name: HeaderName::ContentLength,
                value: b"42",
            },
        ];
        let req = Request {
            method: Method::Post,
            path: b"/api",
            query: Some(b"v=1"),
            version: Version::Http11,
            headers: &headers,
        };

        let mut buf_out = vec![0u8; req.serialized_len()];
        let len = req.serialize_to_buf(&mut buf_out).unwrap();

        let mut writer_out = Vec::new();
        req.serialize_to_writer(&mut writer_out).unwrap();

        assert_eq!(&buf_out[..len], writer_out.as_slice());
    }

    #[test]
    fn empty_path_rejected() {
        let req = Request {
            method: Method::Get,
            path: b"",
            query: None,
            version: Version::Http11,
            headers: &[],
        };
        let mut buf = [0u8; 64];
        assert_eq!(
            req.serialize_to_buf(&mut buf).unwrap_err(),
            Error::Serialize(SerializeError::InvalidPath)
        );
    }

    #[test]
    fn path_with_crlf_rejected() {
        let req = Request {
            method: Method::Get,
            path: b"/a\r\nX-Injected: yes",
            query: None,
            version: Version::Http11,
            headers: &[],
        };
        let mut buf = [0u8; 128];
        assert_eq!(
            req.serialize_to_buf(&mut buf).unwrap_err(),
            Error::Serialize(SerializeError::InvalidPath)
        );
    }

    #[test]
    fn space_in_path_rejected() {
        let req = Request {
            method: Method::Get,
            path: b"/a b",
            query: None,
            version: Version::Http11,
            headers: &[],
        };
        let mut buf = [0u8; 64];
        assert_eq!(
            req.serialize_to_buf(&mut buf).unwrap_err(),
            Error::Serialize(SerializeError::InvalidPath)
        );
    }

    #[test]
    fn query_with_crlf_rejected() {
        let req = Request {
            method: Method::Get,
            path: b"/search",
            query: Some(b"q=1\r\nX-Injected: yes"),
            version: Version::Http11,
            headers: &[],
        };
        let mut buf = [0u8; 128];
        assert_eq!(
            req.serialize_to_buf(&mut buf).unwrap_err(),
            Error::Serialize(SerializeError::InvalidPath)
        );
    }

    #[test]
    fn header_value_with_crlf_rejected() {
        let headers = [Header {
            name: HeaderName::Accept,
            value: b"*/*\r\nX-Injected: yes",
        }];
        let req = Request {
            method: Method::Get,
            path: b"/",
            query: None,
            version: Version::Http11,
            headers: &headers,
        };
        let mut buf = [0u8; 128];
        assert_eq!(
            req.serialize_to_buf(&mut buf).unwrap_err(),
            Error::Serialize(SerializeError::InvalidHeader)
        );
    }

    #[test]
    fn header_name_with_invalid_byte_rejected() {
        let headers = [Header {
            name: HeaderName::from_bytes(b"Bad Name"),
            value: b"ok",
        }];
        let req = Request {
            method: Method::Get,
            path: b"/",
            query: None,
            version: Version::Http11,
            headers: &headers,
        };
        let mut buf = [0u8; 128];
        assert_eq!(
            req.serialize_to_buf(&mut buf).unwrap_err(),
            Error::Serialize(SerializeError::InvalidHeader)
        );
    }

    #[test]
    fn header_value_crlf_rejected_via_writer() {
        let headers = [Header {
            name: HeaderName::Accept,
            value: b"*/*\nX-Injected: yes",
        }];
        let req = Request {
            method: Method::Get,
            path: b"/",
            query: None,
            version: Version::Http11,
            headers: &headers,
        };
        let mut out = Vec::new();
        let err = req.serialize_to_writer(&mut out).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }
}
