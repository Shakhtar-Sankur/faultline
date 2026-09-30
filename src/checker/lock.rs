//! The lock checker. Each client, holding the lock, reads a counter and
//! writes it plus one. Two things can go wrong:
//!
//! - **Mutual exclusion:** two clients believed they held the lock at the
//!   same instant. Every client shares the test machine's clock, so their
//!   holding intervals are directly comparable.
//! - **Lost updates:** two acknowledged increments read the same value, so
//!   one overwrote the other. This is the damage a lock exists to prevent.
//!
//! A lease-based lock cannot promise the first on its own: a holder that
//! stalls past its lease loses the lock without knowing it. A fenced write
//! (one that commits only while the holder's ownership key still exists)
//! still prevents the second, so that is the verdict that counts.

use crate::history::Outcome;

#[derive(Clone, Debug)]
pub struct LockOp {
    pub process: usize,
    pub node: usize,
    /// When the client learned it held the lock, and when it began to
    /// release it (ns since the test began).
    pub acquired: u64,
    pub released: u64,
    /// The counter value it read while holding the lock.
    pub read: u64,
    /// How its write of `read + 1` ended.
    pub wrote: Outcome,
    /// Whether the client stalled (a simulated GC pause) while holding it.
    pub stalled: bool,
}

#[derive(Debug, Default)]
pub struct LockReport {
    pub acquisitions: usize,
    pub increments: usize,
    /// Increments refused because the writer had lost the lock.
    pub fenced_out: usize,
    /// Pairs of clients that held the lock at the same instant, with the
    /// time they overlapped.
    pub overlaps: Vec<(usize, usize, u64, u64)>,
    /// Counter values that two or more acknowledged increments both read.
    pub lost_updates: Vec<(u64, Vec<usize>)>,
}

impl LockReport {
    /// The data the lock protects stayed correct.
    pub fn valid(&self) -> bool {
        self.lost_updates.is_empty()
    }
}

pub fn check(ops: &[LockOp]) -> LockReport {
    let mut r = LockReport {
        acquisitions: ops.len(),
        ..LockReport::default()
    };
    let mut sorted: Vec<&LockOp> = ops.iter().collect();
    sorted.sort_by_key(|o| o.acquired);
    for (i, a) in sorted.iter().enumerate() {
        for b in &sorted[i + 1..] {
            if b.acquired >= a.released {
                break;
            }
            if a.process != b.process {
                r.overlaps
                    .push((a.process, b.process, b.acquired, a.released.min(b.released)));
            }
        }
    }
    let mut by_read: std::collections::BTreeMap<u64, Vec<usize>> = Default::default();
    for o in ops {
        match o.wrote {
            Outcome::Ok => {
                r.increments += 1;
                by_read.entry(o.read).or_default().push(o.process);
            }
            Outcome::Fail => r.fenced_out += 1,
            Outcome::Info => {}
        }
    }
    r.lost_updates = by_read.into_iter().filter(|(_, ps)| ps.len() > 1).collect();
    r
}

#[cfg(test)]
mod tests {
    use super::*;

    fn op(process: usize, acquired: u64, released: u64, read: u64, wrote: Outcome) -> LockOp {
        LockOp {
            process,
            node: 0,
            acquired,
            released,
            read,
            wrote,
            stalled: false,
        }
    }

    #[test]
    fn finds_overlaps_and_lost_updates() {
        let r = check(&[
            op(1, 0, 10, 0, Outcome::Ok),
            op(2, 10, 20, 1, Outcome::Ok),
            op(3, 15, 30, 1, Outcome::Ok), // overlaps 2, and both read 1
            op(4, 40, 50, 2, Outcome::Fail),
        ]);
        assert_eq!(r.overlaps.len(), 1);
        assert_eq!(r.lost_updates, vec![(1, vec![2, 3])]);
        assert_eq!((r.increments, r.fenced_out), (3, 1));
        assert!(!r.valid());
        let r = check(&[op(1, 0, 10, 0, Outcome::Ok), op(2, 10, 20, 1, Outcome::Ok)]);
        assert!(r.overlaps.is_empty() && r.valid());
    }
}
