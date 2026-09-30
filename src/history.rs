//! A test history: every operation a client invoked, when, and how it
//! ended. Times are nanoseconds since the test began.

use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Acknowledged by the database.
    Ok,
    /// Definitely did not happen (refused before it could take effect).
    Fail,
    /// Unknown: it may have happened, at any time after it was invoked.
    Info,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Call {
    /// A read, and (if it completed) the value it saw.
    Read(Option<u64>),
    Write(u64),
    /// Compare-and-set from the first value to the second.
    Cas(u64, u64),
    /// Add an element to a set.
    Add(u64),
    /// Read a whole set.
    ReadSet(Vec<u64>),
}

#[derive(Clone, Debug)]
pub struct Op {
    pub process: usize,
    pub node: usize,
    pub key: u64,
    pub call: Call,
    pub start: u64,
    /// When the client got its answer (or gave up waiting).
    pub end: u64,
    pub outcome: Outcome,
    /// For failures and unknown outcomes: what went wrong.
    pub error: Option<String>,
}

impl fmt::Display for Op {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let what = match &self.call {
            Call::Read(v) => format!("read {}", v.map_or("nil".into(), |v| v.to_string())),
            Call::Write(v) => format!("write {v}"),
            Call::Cas(a, b) => format!("cas {a} -> {b}"),
            Call::Add(v) => format!("add {v}"),
            Call::ReadSet(s) => format!("read set ({} elements)", s.len()),
        };
        write!(
            f,
            "{:>8}..{:<8}  p{:<3} n{}  key {:<4} {:<5} {what}{}",
            format!("{:.3}s", self.start as f64 / 1e9),
            format!("{:.3}s", self.end as f64 / 1e9),
            self.process,
            self.node + 1,
            self.key,
            format!("{:?}", self.outcome).to_lowercase(),
            self.error
                .as_deref()
                .map(|e| format!("  ({e})"))
                .unwrap_or_default()
        )
    }
}

/// One line of JSON per operation, for the history file.
pub fn to_json(op: &Op) -> String {
    let (f, value) = match &op.call {
        Call::Read(v) => ("read", v.map_or("null".into(), |v| v.to_string())),
        Call::Write(v) => ("write", v.to_string()),
        Call::Cas(a, b) => ("cas", format!("[{a},{b}]")),
        Call::Add(v) => ("add", v.to_string()),
        Call::ReadSet(s) => (
            "read-set",
            format!(
                "[{}]",
                s.iter().map(u64::to_string).collect::<Vec<_>>().join(",")
            ),
        ),
    };
    format!(
        "{{\"process\":{},\"node\":{},\"key\":{},\"f\":\"{f}\",\"value\":{value},\"start_ns\":{},\"end_ns\":{},\"type\":\"{}\"{}}}",
        op.process,
        op.node + 1,
        op.key,
        op.start,
        op.end,
        format!("{:?}", op.outcome).to_lowercase(),
        op.error
            .as_deref()
            .map(|e| format!(",\"error\":{}", crate::json::quote(e)))
            .unwrap_or_default()
    )
}
