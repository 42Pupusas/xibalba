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

/// Types that can render themselves as a fixed byte slice and as a UTF-8 str.
pub trait Token {
    /// Fixed byte representation of this token.
    #[allow(clippy::wrong_self_convention)]
    fn as_bytes(self) -> &'static [u8];

    /// UTF-8 representation of this token.
    #[allow(clippy::wrong_self_convention)]
    fn as_str(self) -> &'static str;
}

impl Method {
    #[must_use]
    const fn token(self) -> &'static str {
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

    /// Whether resending after an ambiguous transport failure cannot repeat
    /// a side effect: the request only reads, or its semantics are defined
    /// as idempotent or safe to repeat by RFC 9110 §9.2.2. POST and PATCH
    /// are excluded because a server may already have applied them when the
    /// response went missing.
    #[must_use]
    pub const fn is_replay_eligible(self) -> bool {
        matches!(
            self,
            Self::Get | Self::Head | Self::Put | Self::Delete | Self::Options | Self::Trace
        )
    }
}

impl Token for Method {
    fn as_str(self) -> &'static str {
        self.token()
    }

    fn as_bytes(self) -> &'static [u8] {
        self.token().as_bytes()
    }
}

impl AsRef<str> for Method {
    fn as_ref(&self) -> &str {
        Token::as_str(*self)
    }
}

impl AsRef<[u8]> for Method {
    fn as_ref(&self) -> &[u8] {
        Token::as_bytes(*self)
    }
}

impl fmt::Display for Method {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(Token::as_str(*self))
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
}
