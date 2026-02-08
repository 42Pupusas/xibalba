use core::fmt;

use crate::error::{Error, UrlError};

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum Scheme {
    Http,
    Https,
}

impl Scheme {
    #[must_use]
    pub const fn default_port(self) -> u16 {
        match self {
            Self::Http => 80,
            Self::Https => 443,
        }
    }

    #[must_use]
    pub const fn as_bytes(self) -> &'static [u8] {
        match self {
            Self::Http => b"http",
            Self::Https => b"https",
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Https => "https",
        }
    }
}

impl AsRef<str> for Scheme {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<[u8]> for Scheme {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl fmt::Display for Scheme {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl TryFrom<&[u8]> for Scheme {
    type Error = Error;

    /// Case-insensitive match per RFC 3986.
    /// Uses the `| 0x20` bit trick to normalize ASCII alpha to lowercase.
    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        if bytes.len() == 4
            && (bytes[0] | 0x20) == b'h'
            && (bytes[1] | 0x20) == b't'
            && (bytes[2] | 0x20) == b't'
            && (bytes[3] | 0x20) == b'p'
        {
            Ok(Self::Http)
        } else if bytes.len() == 5
            && (bytes[0] | 0x20) == b'h'
            && (bytes[1] | 0x20) == b't'
            && (bytes[2] | 0x20) == b't'
            && (bytes[3] | 0x20) == b'p'
            && (bytes[4] | 0x20) == b's'
        {
            Ok(Self::Https)
        } else {
            Err(UrlError::InvalidScheme.into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_lowercase() {
        assert_eq!(Scheme::try_from(b"http" as &[u8]).unwrap(), Scheme::Http);
        assert_eq!(Scheme::try_from(b"https" as &[u8]).unwrap(), Scheme::Https);
    }

    #[test]
    fn parse_uppercase() {
        assert_eq!(Scheme::try_from(b"HTTP" as &[u8]).unwrap(), Scheme::Http);
        assert_eq!(Scheme::try_from(b"HTTPS" as &[u8]).unwrap(), Scheme::Https);
    }

    #[test]
    fn parse_mixed_case() {
        assert_eq!(Scheme::try_from(b"Http" as &[u8]).unwrap(), Scheme::Http);
        assert_eq!(Scheme::try_from(b"hTtPs" as &[u8]).unwrap(), Scheme::Https);
    }

    #[test]
    fn rejects_invalid() {
        assert!(Scheme::try_from(b"ftp" as &[u8]).is_err());
        assert!(Scheme::try_from(b"" as &[u8]).is_err());
        assert!(Scheme::try_from(b"httpx" as &[u8]).is_err());
    }

    #[test]
    fn default_ports() {
        assert_eq!(Scheme::Http.default_port(), 80);
        assert_eq!(Scheme::Https.default_port(), 443);
    }

    #[test]
    fn display() {
        assert_eq!(Scheme::Http.to_string(), "http");
        assert_eq!(Scheme::Https.to_string(), "https");
    }
}
