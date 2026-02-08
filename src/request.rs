use crate::error::{Error, SerializeError};
use crate::header::Header;
use crate::method::Method;
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
    /// Serialize the request head into the provided buffer.
    ///
    /// Returns the number of bytes written.
    ///
    /// # Errors
    ///
    /// Returns `SerializeError::BufferTooSmall` if the buffer cannot hold
    /// the full request head.
    pub fn serialize_to_buf(&self, buf: &mut [u8]) -> Result<usize, Error> {
        let mut pos = 0;

        pos = write_bytes(buf, pos, self.method.as_bytes())?;
        pos = write_byte(buf, pos, b' ')?;
        pos = write_bytes(buf, pos, self.path)?;
        if let Some(query) = self.query {
            pos = write_byte(buf, pos, b'?')?;
            pos = write_bytes(buf, pos, query)?;
        }
        pos = write_byte(buf, pos, b' ')?;
        pos = write_bytes(buf, pos, self.version.as_bytes())?;
        pos = write_bytes(buf, pos, b"\r\n")?;

        for header in self.headers {
            pos = write_bytes(buf, pos, header.name.as_bytes())?;
            pos = write_bytes(buf, pos, b": ")?;
            pos = write_bytes(buf, pos, header.value)?;
            pos = write_bytes(buf, pos, b"\r\n")?;
        }

        pos = write_bytes(buf, pos, b"\r\n")?;
        Ok(pos)
    }

    /// Serialize the request head to an `impl Write`.
    ///
    /// # Errors
    ///
    /// Returns `std::io::Error` on write failure.
    pub fn serialize_to_writer(&self, w: &mut impl std::io::Write) -> std::io::Result<()> {
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
        let mut len = self.method.as_bytes().len(); // method
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

fn write_bytes(buf: &mut [u8], pos: usize, data: &[u8]) -> Result<usize, Error> {
    let end = pos + data.len();
    if end > buf.len() {
        return Err(SerializeError::BufferTooSmall.into());
    }
    buf[pos..end].copy_from_slice(data);
    Ok(end)
}

fn write_byte(buf: &mut [u8], pos: usize, byte: u8) -> Result<usize, Error> {
    if pos >= buf.len() {
        return Err(SerializeError::BufferTooSmall.into());
    }
    buf[pos] = byte;
    Ok(pos + 1)
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

        let mut buf = [0u8; 5]; // way too small
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
}
