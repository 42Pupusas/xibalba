//! Extension trait for byte-slice operations used across the crate.
//!
//! One trait with a blanket impl on `[u8]`, so every caller reaches these
//! through the same path.

/// ASCII and parsing helpers for byte slices.
pub trait ByteSliceExt: private::Sealed {
    /// Case-insensitive ASCII equality (`A` == `a`, but `0` != `O`).
    fn ascii_eq_ignore_case(&self, other: &[u8]) -> bool;

    /// Trim optional whitespace (SP, HTAB) from both ends.
    fn trim_ows(&self) -> &[u8];

    /// Parse a `u64` from ASCII digit bytes, skipping leading/trailing OWS.
    fn parse_u64(&self) -> Option<u64>;

    /// Check if a comma-separated header value contains `token`
    /// (case-insensitive).
    fn contains_token_ignore_case(&self, token: &[u8]) -> bool;

    /// Find the first occurrence of `needle` in `self`.
    fn find_subsequence(&self, needle: &[u8]) -> Option<usize>;

    /// Find the first CRLF (`\r\n`) in `self`.
    fn find_crlf(&self) -> Option<usize>;
}

mod private {
    pub trait Sealed {}
    impl Sealed for [u8] {}
    impl Sealed for &[u8] {}
}

impl ByteSliceExt for [u8] {
    #[inline]
    fn ascii_eq_ignore_case(&self, other: &[u8]) -> bool {
        if self.len() != other.len() {
            return false;
        }
        self.iter().zip(other.iter()).all(|(&x, &y)| {
            if x == y {
                return true;
            }
            let xl = x | 0x20;
            let yl = y | 0x20;
            xl == yl && xl.is_ascii_lowercase()
        })
    }

    fn trim_ows(&self) -> &[u8] {
        let start = self
            .iter()
            .position(|&b| b != b' ' && b != b'\t')
            .unwrap_or(self.len());
        let end = self
            .iter()
            .rposition(|&b| b != b' ' && b != b'\t')
            .map_or(start, |p| p + 1);
        &self[start..end]
    }

    fn parse_u64(&self) -> Option<u64> {
        let bytes = self.trim_ows();
        if bytes.is_empty() {
            return None;
        }
        let mut result: u64 = 0;
        for &b in bytes {
            let digit = b.wrapping_sub(b'0');
            if digit > 9 {
                return None;
            }
            result = result.checked_mul(10)?.checked_add(u64::from(digit))?;
        }
        Some(result)
    }

    fn contains_token_ignore_case(&self, token: &[u8]) -> bool {
        self.split(|&b| b == b',').any(|part| {
            let trimmed = part.trim_ows();
            trimmed.ascii_eq_ignore_case(token)
        })
    }

    fn find_subsequence(&self, needle: &[u8]) -> Option<usize> {
        if needle.is_empty() {
            return Some(0);
        }
        self.windows(needle.len()).position(|w| w == needle)
    }

    fn find_crlf(&self) -> Option<usize> {
        self.find_subsequence(b"\r\n")
    }
}
