use core::fmt;

use crate::error::{Error, ParseError};

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StatusCode(u16);

impl StatusCode {
    // 1xx Informational
    pub const CONTINUE: Self = Self(100);
    pub const SWITCHING_PROTOCOLS: Self = Self(101);

    // 2xx Success
    pub const OK: Self = Self(200);
    pub const CREATED: Self = Self(201);
    pub const ACCEPTED: Self = Self(202);
    pub const NO_CONTENT: Self = Self(204);

    // 3xx Redirection
    pub const MOVED_PERMANENTLY: Self = Self(301);
    pub const FOUND: Self = Self(302);
    pub const SEE_OTHER: Self = Self(303);
    pub const NOT_MODIFIED: Self = Self(304);
    pub const TEMPORARY_REDIRECT: Self = Self(307);
    pub const PERMANENT_REDIRECT: Self = Self(308);

    // 4xx Client Error
    pub const BAD_REQUEST: Self = Self(400);
    pub const UNAUTHORIZED: Self = Self(401);
    pub const FORBIDDEN: Self = Self(403);
    pub const NOT_FOUND: Self = Self(404);
    pub const METHOD_NOT_ALLOWED: Self = Self(405);
    pub const REQUEST_TIMEOUT: Self = Self(408);
    pub const CONFLICT: Self = Self(409);
    pub const GONE: Self = Self(410);
    pub const LENGTH_REQUIRED: Self = Self(411);
    pub const PAYLOAD_TOO_LARGE: Self = Self(413);
    pub const URI_TOO_LONG: Self = Self(414);
    pub const TOO_MANY_REQUESTS: Self = Self(429);

    // 5xx Server Error
    pub const INTERNAL_SERVER_ERROR: Self = Self(500);
    pub const NOT_IMPLEMENTED: Self = Self(501);
    pub const BAD_GATEWAY: Self = Self(502);
    pub const SERVICE_UNAVAILABLE: Self = Self(503);
    pub const GATEWAY_TIMEOUT: Self = Self(504);

    /// Construct from a raw `u16`. Returns `Err` if outside 100..=999.
    ///
    /// # Errors
    ///
    /// Returns `ParseError::InvalidStatusCode` if the code is not in the
    /// valid HTTP status code range (100-999).
    pub const fn from_u16(code: u16) -> Result<Self, ParseError> {
        if code >= 100 && code <= 999 {
            Ok(Self(code))
        } else {
            Err(ParseError::InvalidStatusCode)
        }
    }

    #[must_use]
    pub const fn as_u16(self) -> u16 {
        self.0
    }

    #[must_use]
    pub const fn is_informational(self) -> bool {
        self.0 >= 100 && self.0 < 200
    }

    #[must_use]
    pub const fn is_success(self) -> bool {
        self.0 >= 200 && self.0 < 300
    }

    #[must_use]
    pub const fn is_redirect(self) -> bool {
        self.0 >= 300 && self.0 < 400
    }

    #[must_use]
    pub const fn is_client_error(self) -> bool {
        self.0 >= 400 && self.0 < 500
    }

    #[must_use]
    pub const fn is_server_error(self) -> bool {
        self.0 >= 500 && self.0 < 600
    }
}

impl TryFrom<&[u8]> for StatusCode {
    type Error = Error;

    /// Parse exactly 3 ASCII digit bytes into a status code.
    /// Pure byte arithmetic, no str conversion.
    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        if bytes.len() != 3 {
            return Err(ParseError::InvalidStatusCode.into());
        }
        let h = bytes[0].wrapping_sub(b'0');
        let t = bytes[1].wrapping_sub(b'0');
        let u = bytes[2].wrapping_sub(b'0');
        if h > 9 || t > 9 || u > 9 {
            return Err(ParseError::InvalidStatusCode.into());
        }
        let code = u16::from(h) * 100 + u16::from(t) * 10 + u16::from(u);
        Self::from_u16(code).map_err(Into::into)
    }
}

impl fmt::Display for StatusCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_200() {
        let s = StatusCode::try_from(b"200" as &[u8]).unwrap();
        assert_eq!(s, StatusCode::OK);
        assert_eq!(s.as_u16(), 200);
    }

    #[test]
    fn parse_404() {
        let s = StatusCode::try_from(b"404" as &[u8]).unwrap();
        assert_eq!(s, StatusCode::NOT_FOUND);
    }

    #[test]
    fn parse_999() {
        let s = StatusCode::try_from(b"999" as &[u8]).unwrap();
        assert_eq!(s.as_u16(), 999);
    }

    #[test]
    fn rejects_two_digits() {
        assert!(StatusCode::try_from(b"99" as &[u8]).is_err());
    }

    #[test]
    fn rejects_non_digits() {
        assert!(StatusCode::try_from(b"abc" as &[u8]).is_err());
        assert!(StatusCode::try_from(b"2x0" as &[u8]).is_err());
    }

    #[test]
    fn rejects_leading_zero() {
        assert!(StatusCode::try_from(b"099" as &[u8]).is_err());
    }

    #[test]
    fn from_u16_boundaries() {
        assert!(StatusCode::from_u16(99).is_err());
        assert!(StatusCode::from_u16(100).is_ok());
        assert!(StatusCode::from_u16(999).is_ok());
        assert!(StatusCode::from_u16(1000).is_err());
    }

    #[test]
    fn category_methods() {
        assert!(StatusCode::CONTINUE.is_informational());
        assert!(StatusCode::OK.is_success());
        assert!(StatusCode::MOVED_PERMANENTLY.is_redirect());
        assert!(StatusCode::NOT_FOUND.is_client_error());
        assert!(StatusCode::INTERNAL_SERVER_ERROR.is_server_error());

        assert!(!StatusCode::OK.is_redirect());
        assert!(!StatusCode::NOT_FOUND.is_success());
    }

}
