use core::fmt;

use crate::error::{Error, ParseError};

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum Method {
    Get,
    Head,
    Post,
    Put,
    Delete,
    Connect,
    Options,
    Trace,
    Patch,
}

impl Method {
    #[must_use]
    pub const fn as_bytes(self) -> &'static [u8] {
        match self {
            Self::Get => b"GET",
            Self::Head => b"HEAD",
            Self::Post => b"POST",
            Self::Put => b"PUT",
            Self::Delete => b"DELETE",
            Self::Connect => b"CONNECT",
            Self::Options => b"OPTIONS",
            Self::Trace => b"TRACE",
            Self::Patch => b"PATCH",
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Head => "HEAD",
            Self::Post => "POST",
            Self::Put => "PUT",
            Self::Delete => "DELETE",
            Self::Connect => "CONNECT",
            Self::Options => "OPTIONS",
            Self::Trace => "TRACE",
            Self::Patch => "PATCH",
        }
    }
}

impl AsRef<str> for Method {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<[u8]> for Method {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl fmt::Display for Method {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl TryFrom<&[u8]> for Method {
    type Error = Error;

    /// Case-sensitive exact match per RFC 7230.
    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        match bytes {
            b"GET" => Ok(Self::Get),
            b"POST" => Ok(Self::Post),
            b"HEAD" => Ok(Self::Head),
            b"PUT" => Ok(Self::Put),
            b"DELETE" => Ok(Self::Delete),
            b"CONNECT" => Ok(Self::Connect),
            b"OPTIONS" => Ok(Self::Options),
            b"TRACE" => Ok(Self::Trace),
            b"PATCH" => Ok(Self::Patch),
            _ => Err(ParseError::InvalidMethod.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_all_variants() {
        let methods = [
            (b"GET" as &[u8], Method::Get),
            (b"HEAD", Method::Head),
            (b"POST", Method::Post),
            (b"PUT", Method::Put),
            (b"DELETE", Method::Delete),
            (b"CONNECT", Method::Connect),
            (b"OPTIONS", Method::Options),
            (b"TRACE", Method::Trace),
            (b"PATCH", Method::Patch),
        ];
        for (bytes, expected) in methods {
            let parsed = Method::try_from(bytes).unwrap();
            assert_eq!(parsed, expected);
            assert_eq!(parsed.as_bytes(), bytes);
        }
    }

    #[test]
    fn rejects_lowercase() {
        assert!(Method::try_from(b"get" as &[u8]).is_err());
        assert!(Method::try_from(b"Get" as &[u8]).is_err());
    }

    #[test]
    fn rejects_unknown() {
        assert!(Method::try_from(b"FOOBAR" as &[u8]).is_err());
        assert!(Method::try_from(b"" as &[u8]).is_err());
    }

    #[test]
    fn display() {
        assert_eq!(Method::Get.to_string(), "GET");
        assert_eq!(Method::Post.to_string(), "POST");
    }

    #[test]
    fn as_ref_str() {
        let m = Method::Delete;
        let s: &str = m.as_ref();
        assert_eq!(s, "DELETE");
    }

    #[test]
    fn as_ref_bytes() {
        let m = Method::Options;
        let b: &[u8] = m.as_ref();
        assert_eq!(b, b"OPTIONS");
    }
}
