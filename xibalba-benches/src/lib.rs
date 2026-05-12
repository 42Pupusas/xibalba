// ── Response head fixtures ─────────────────────────────────────────────────

pub const RESP_MINIMAL: &[u8] = b"HTTP/1.1 200 OK\r\n\r\n";

pub const RESP_TYPICAL: &[u8] = b"HTTP/1.1 200 OK\r\n\
    Content-Type: text/html; charset=utf-8\r\n\
    Content-Length: 1234\r\n\
    Server: nginx/1.24\r\n\
    Date: Mon, 01 Jan 2024 00:00:00 GMT\r\n\
    Connection: keep-alive\r\n\
    Cache-Control: max-age=3600\r\n\r\n";

pub const RESP_HEAVY: &[u8] = b"HTTP/1.1 200 OK\r\n\
    Content-Type: application/json\r\n\
    Content-Length: 512\r\n\
    Transfer-Encoding: identity\r\n\
    Connection: keep-alive\r\n\
    Host: example.com\r\n\
    Accept: */*\r\n\
    User-Agent: bench/1.0\r\n\
    Accept-Encoding: gzip, deflate\r\n\
    Location: /redirect\r\n\
    Cache-Control: no-cache\r\n\
    Date: Mon, 01 Jan 2024 00:00:00 GMT\r\n\
    Server: apache\r\n\
    Content-Encoding: identity\r\n\
    Set-Cookie: sid=abc123\r\n\
    Cookie: sid=abc123\r\n\
    Authorization: Bearer token123\r\n\
    X-Request-Id: 550e8400-e29b-41d4-a716-446655440000\r\n\
    X-Forwarded-For: 192.0.2.1\r\n\
    X-Frame-Options: DENY\r\n\
    X-Content-Type-Options: nosniff\r\n\
    Strict-Transport-Security: max-age=31536000\r\n\
    Vary: Accept-Encoding\r\n\r\n";

pub const RESP_ALL_UNKNOWN: &[u8] = b"HTTP/1.1 200 OK\r\n\
    X-A: 1\r\nX-B: 2\r\nX-C: 3\r\nX-D: 4\r\nX-E: 5\r\n\
    X-F: 6\r\nX-G: 7\r\nX-H: 8\r\nX-I: 9\r\nX-J: 10\r\n\
    X-K: 11\r\nX-L: 12\r\nX-M: 13\r\nX-N: 14\r\nX-O: 15\r\n\
    X-P: 16\r\nX-Q: 17\r\nX-R: 18\r\nX-S: 19\r\nX-T: 20\r\n\r\n";

// ── Chunked body fixtures ──────────────────────────────────────────────────

pub const CHUNKED_SINGLE: &[u8] = b"b\r\nhello world\r\n0\r\n\r\n";

pub const CHUNKED_MULTI: &[u8] =
    b"7\r\nMozilla\r\n9\r\nDeveloper\r\n7\r\nNetwork\r\n0\r\n\r\n";

#[must_use]
pub fn chunked_large() -> Vec<u8> {
    const N: usize = 65536;
    let mut v = Vec::with_capacity(8 + N + 7);
    v.extend_from_slice(b"10000\r\n");
    v.extend(std::iter::repeat_n(b'x', N));
    v.extend_from_slice(b"\r\n0\r\n\r\n");
    v
}

// ── Request fixtures ───────────────────────────────────────────────────────

pub const REQ_PATH_SHORT: &[u8] = b"/";
pub const REQ_PATH_LONG: &[u8] =
    b"/api/v2/organizations/acme-corp/projects/main/environments/production/deployments";
pub const REQ_QUERY_LONG: &[u8] =
    b"filter=active&sort=created_at&order=desc&page=3&per_page=100\
      &include=metadata&expand=owner,team&format=json&locale=en-US&token=abc123xyz";

// ── URL fixtures ───────────────────────────────────────────────────────────

pub const URL_SIMPLE: &[u8] = b"http://example.com/";
pub const URL_FULL: &[u8] =
    b"https://user.example.com:8443/api/v2/resource?q=hello&page=2&sort=desc#section-3";
pub const URL_IPV6: &[u8] = b"http://[2001:db8::1]:8080/path";
pub const URL_MANY_PARAMS: &[u8] =
    b"http://example.com/search?\
      a=1&b=2&c=3&d=4&e=5&f=6&g=7&h=8&i=9&j=10&\
      k=11&l=12&m=13&n=14&o=15&p=16&q=17&r=18&s=19&t=20&\
      u=21&v=22&w=23&x=24&y=25&z=26&aa=27&ab=28&ac=29&ad=30&\
      ae=31&af=32&ag=33&ah=34&ai=35&af2=36&ag2=37&ah2=38&ai2=39&aj=40&\
      ak=41&al=42&am=43&an=44&ao=45&ap=46&aq=47&ar=48&as=49&at=50";
pub const URL_ONE_PARAM: &[u8] = b"http://example.com/?only=value";
pub const URL_TEN_PARAMS: &[u8] =
    b"http://example.com/?a=1&b=2&c=3&d=4&e=5&f=6&g=7&h=8&i=9&j=10";

// ── Header name fixtures ───────────────────────────────────────────────────

pub const HDR_HOST: &[u8] = b"Host";
pub const HDR_CONTENT_LENGTH: &[u8] = b"Content-Length";
pub const HDR_CONTENT_TYPE: &[u8] = b"Content-Type";
pub const HDR_TRANSFER_ENCODING: &[u8] = b"Transfer-Encoding";
pub const HDR_ACCEPT_ENCODING: &[u8] = b"Accept-Encoding";
pub const HDR_CACHE_CONTROL: &[u8] = b"Cache-Control";
pub const HDR_UNKNOWN: &[u8] = b"X-Custom-Header";

// ── Micro-helper fixtures ──────────────────────────────────────────────────

pub const OWS_NONE: &[u8] = b"application/json";
pub const OWS_BOTH: &[u8] = b"  application/json  ";
pub const OWS_TAB: &[u8] = b"\t  gzip\t  ";

pub const U64_SHORT: &[u8] = b"42";
pub const U64_LONG: &[u8] = b"18446744073709551615";
pub const U64_WITH_OWS: &[u8] = b"  9999  ";

pub const TOKEN_LIST_SHORT: &[u8] = b"chunked";
pub const TOKEN_LIST_LONG: &[u8] = b"gzip, deflate, br, zstd, chunked, identity";
pub const TOKEN_ABSENT: &[u8] = b"gzip, deflate, br, zstd, identity";
