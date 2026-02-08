use core::fmt;

use crate::error::{Error, ParseError};

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum Version {
    Http10,
    Http11,
}

impl Version {
    #[must_use]
    pub const fn as_bytes(self) -> &'static [u8] {
        match self {
            Self::Http10 => b"HTTP/1.0",
            Self::Http11 => b"HTTP/1.1",
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Http10 => "HTTP/1.0",
            Self::Http11 => "HTTP/1.1",
        }
    }
}

impl AsRef<str> for Version {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<[u8]> for Version {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl TryFrom<&[u8]> for Version {
    type Error = Error;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        match bytes {
            b"HTTP/1.1" => Ok(Self::Http11),
            b"HTTP/1.0" => Ok(Self::Http10),
            _ => Err(ParseError::InvalidVersion.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_http11() {
        let v = Version::try_from(b"HTTP/1.1" as &[u8]).unwrap();
        assert_eq!(v, Version::Http11);
        assert_eq!(v.as_bytes(), b"HTTP/1.1");
        assert_eq!(v.as_str(), "HTTP/1.1");
    }

    #[test]
    fn parse_http10() {
        let v = Version::try_from(b"HTTP/1.0" as &[u8]).unwrap();
        assert_eq!(v, Version::Http10);
    }

    #[test]
    fn rejects_http2() {
        assert!(Version::try_from(b"HTTP/2.0" as &[u8]).is_err());
    }

    #[test]
    fn rejects_garbage() {
        assert!(Version::try_from(b"" as &[u8]).is_err());
        assert!(Version::try_from(b"HTTP" as &[u8]).is_err());
        assert!(Version::try_from(b"http/1.1" as &[u8]).is_err());
    }

    #[test]
    fn display() {
        assert_eq!(Version::Http11.to_string(), "HTTP/1.1");
        assert_eq!(Version::Http10.to_string(), "HTTP/1.0");
    }
}
