//! A minimal HTTP/1.1 client over std's TCP, with timeouts, that tells a
//! request which surely never reached the server apart from one whose fate
//! is unknown. The difference decides how the checker may treat it.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

#[derive(Debug)]
pub enum HttpError {
    /// The request never reached the server (connection refused, or the
    /// connection could not be made in time): it had no effect.
    NotSent(String),
    /// The request may have reached the server; its effect is unknown.
    Unknown(String),
}

pub struct Response {
    pub status: u16,
    pub body: String,
}

pub fn post(
    addr: SocketAddr,
    path: &str,
    body: &str,
    timeout: Duration,
) -> Result<Response, HttpError> {
    let mut s = TcpStream::connect_timeout(&addr, timeout)
        .map_err(|e| HttpError::NotSent(e.to_string()))?;
    s.set_read_timeout(Some(timeout)).ok();
    s.set_write_timeout(Some(timeout)).ok();
    s.set_nodelay(true).ok();
    let req = format!(
        "POST {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    s.write_all(req.as_bytes())
        .map_err(|e| HttpError::Unknown(format!("write: {e}")))?;
    let mut raw = Vec::new();
    s.read_to_end(&mut raw)
        .map_err(|e| HttpError::Unknown(format!("read: {e}")))?;
    parse_response(&raw).ok_or_else(|| HttpError::Unknown("malformed or truncated response".into()))
}

fn parse_response(raw: &[u8]) -> Option<Response> {
    let split = raw.windows(4).position(|w| w == b"\r\n\r\n")?;
    let head = std::str::from_utf8(&raw[..split]).ok()?;
    let mut lines = head.split("\r\n");
    let status = lines.next()?.split(' ').nth(1)?.parse().ok()?;
    let mut chunked = false;
    let mut length = None;
    for l in lines {
        let (k, v) = l.split_once(':')?;
        let (k, v) = (k.trim().to_ascii_lowercase(), v.trim());
        if k == "transfer-encoding" && v.eq_ignore_ascii_case("chunked") {
            chunked = true;
        }
        if k == "content-length" {
            length = v.parse::<usize>().ok();
        }
    }
    let rest = &raw[split + 4..];
    let body = if chunked {
        let mut out = Vec::new();
        let mut i = 0;
        loop {
            let eol = rest[i..].windows(2).position(|w| w == b"\r\n")? + i;
            let size =
                usize::from_str_radix(std::str::from_utf8(&rest[i..eol]).ok()?.trim(), 16).ok()?;
            if size == 0 {
                break;
            }
            out.extend_from_slice(rest.get(eol + 2..eol + 2 + size)?);
            i = eol + 2 + size + 2;
        }
        out
    } else {
        match length {
            Some(n) => rest.get(..n)?.to_vec(),
            None => rest.to_vec(),
        }
    };
    Some(Response {
        status,
        body: String::from_utf8(body).ok()?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_plain_and_chunked_responses() {
        let r = parse_response(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}").unwrap();
        assert_eq!((r.status, r.body.as_str()), (200, "{}"));
        let r = parse_response(
            b"HTTP/1.1 503 x\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n2\r\nde\r\n0\r\n\r\n",
        )
        .unwrap();
        assert_eq!((r.status, r.body.as_str()), (503, "abcde"));
        assert!(parse_response(b"HTTP/1.1 200 OK\r\nContent-Length: 9\r\n\r\n{}").is_none());
    }
}
