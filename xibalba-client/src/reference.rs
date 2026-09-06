/// Resolves a URI reference against a base path, per RFC 3986 §5.2.
///
/// Kept independent of the client: resolution is a pure function of the base
/// path and the reference, so it needs no connection state and can be tested
/// directly against the RFC's own examples.
pub struct UriReference;

impl UriReference {
    /// Merge `reference` with `base_path` and remove dot segments.
    ///
    /// `base_path` is the path of the request that produced the redirect;
    /// `reference` is a relative path from its `Location`.
    #[must_use]
    pub fn resolve(base_path: &[u8], reference: &[u8]) -> Vec<u8> {
        let merged = Self::merge(base_path, reference);
        Self::remove_dot_segments(&merged)
    }

    /// RFC 3986 §5.2.3: keep everything up to the base's last slash and
    /// append the reference.
    fn merge(base_path: &[u8], reference: &[u8]) -> Vec<u8> {
        let base_end = base_path
            .iter()
            .rposition(|&b| b == b'/')
            .map_or(0, |pos| pos + 1);
        let mut merged = base_path[..base_end].to_vec();
        if merged.is_empty() {
            merged.push(b'/');
        }
        merged.extend_from_slice(reference);
        merged
    }

    /// RFC 3986 §5.2.4: resolve `.` and `..` against the path, so a server
    /// never sees a target the client could have resolved itself.
    ///
    /// `..` beyond the root is discarded rather than escaping it.
    #[must_use]
    pub fn remove_dot_segments(path: &[u8]) -> Vec<u8> {
        let mut out: Vec<&[u8]> = Vec::new();
        let absolute = path.starts_with(b"/");
        let mut trailing_slash = false;

        for segment in path.split(|&b| b == b'/') {
            match segment {
                b".." => {
                    out.pop();
                    trailing_slash = true;
                }
                b"." | b"" => trailing_slash = true,
                other => {
                    out.push(other);
                    trailing_slash = false;
                }
            }
        }

        let mut resolved = Vec::with_capacity(path.len());
        for segment in &out {
            resolved.push(b'/');
            resolved.extend_from_slice(segment);
        }
        if trailing_slash {
            resolved.push(b'/');
        }
        if resolved.is_empty() {
            resolved.push(b'/');
        }
        if !absolute && resolved.starts_with(b"/") {
            resolved.remove(0);
        }
        resolved
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resolved(base: &str, reference: &str) -> String {
        String::from_utf8(UriReference::resolve(base.as_bytes(), reference.as_bytes()))
            .expect("resolution preserves UTF-8 for ASCII inputs")
    }

    #[test]
    fn parent_segments_are_removed() {
        assert_eq!(resolved("/a/b", "../c"), "/c");
        assert_eq!(resolved("/a/b/c", "../d"), "/a/d");
        assert_eq!(resolved("/a/b/c", "../../d"), "/d");
    }

    #[test]
    fn current_directory_segments_are_removed() {
        assert_eq!(resolved("/a/b", "./c"), "/a/c");
        assert_eq!(resolved("/a/b/", "./c"), "/a/b/c");
    }

    #[test]
    fn plain_relative_references_resolve_against_the_directory() {
        assert_eq!(resolved("/a/b", "c"), "/a/c");
        assert_eq!(resolved("/a/b/", "c"), "/a/b/c");
        assert_eq!(resolved("/", "c"), "/c");
    }

    #[test]
    fn parent_segments_cannot_escape_the_root() {
        // Walking above the root must not produce a target outside it.
        assert_eq!(resolved("/a", "../../../b"), "/b");
        assert_eq!(resolved("/", "../.."), "/");
    }

    #[test]
    fn rfc3986_normal_examples() {
        // RFC 3986 §5.4.1, relative-path cases against base /b/c/d;p
        assert_eq!(resolved("/b/c/d;p", "g"), "/b/c/g");
        assert_eq!(resolved("/b/c/d;p", "./g"), "/b/c/g");
        assert_eq!(resolved("/b/c/d;p", "g/"), "/b/c/g/");
        assert_eq!(resolved("/b/c/d;p", "."), "/b/c/");
        assert_eq!(resolved("/b/c/d;p", ".."), "/b/");
        assert_eq!(resolved("/b/c/d;p", "../g"), "/b/g");
        assert_eq!(resolved("/b/c/d;p", "../.."), "/");
        assert_eq!(resolved("/b/c/d;p", "../../g"), "/g");
    }

    #[test]
    fn rfc3986_abnormal_examples() {
        // §5.4.2: excess .. segments are discarded, not preserved.
        assert_eq!(resolved("/b/c/d;p", "../../../g"), "/g");
        assert_eq!(resolved("/b/c/d;p", "../../../../g"), "/g");
    }

    #[test]
    fn absolute_references_still_have_dot_segments_removed() {
        // An absolute Location replaces the path outright rather than merging
        // with the base, but it can still carry dot segments.
        assert_eq!(UriReference::remove_dot_segments(b"/./g"), b"/g".to_vec());
        assert_eq!(UriReference::remove_dot_segments(b"/../g"), b"/g".to_vec());
    }

    #[test]
    fn dot_segments_are_removed_from_absolute_paths() {
        assert_eq!(
            UriReference::remove_dot_segments(b"/a/./b/../c"),
            b"/a/c".to_vec()
        );
        assert_eq!(
            UriReference::remove_dot_segments(b"/a/b/"),
            b"/a/b/".to_vec()
        );
    }
}
