//! The watch checker. Clients write unique values to a few keys while
//! watchers stream every change to those keys, reconnecting after faults
//! and resuming just after the last revision they saw, as real clients
//! do. etcd promises that watch events arrive in revision order without
//! duplicates, that every watcher sees the same event for a revision, that
//! events are only ever for committed writes, and that none are missed.
//!
//! The workload's writes are the only writes, and each creates exactly
//! one revision, so after the faults heal a watcher that has caught up
//! must have seen every revision from the first write to the last, each
//! exactly once.

use std::collections::{BTreeMap, HashMap};

use crate::db::WatchEvent;
use crate::history::Outcome;

#[derive(Clone, Debug)]
pub struct WatchWrite {
    pub process: usize,
    pub key: u64,
    pub value: u64,
    pub outcome: Outcome,
    /// The revision the write created, if acknowledged.
    pub revision: Option<u64>,
}

#[derive(Clone, Debug, Default)]
pub struct Watcher {
    pub node: usize,
    pub events: Vec<WatchEvent>,
    pub reconnects: usize,
}

#[derive(Debug, Default)]
pub struct WatchReport {
    pub writes_ok: usize,
    pub events: usize,
    pub reconnects: usize,
    /// Watchers that saw through the final revision.
    pub caught_up: usize,
    pub watchers: usize,
    pub final_revision: u64,
    pub violations: Vec<String>,
}

impl WatchReport {
    pub fn valid(&self) -> bool {
        self.violations.is_empty() && self.caught_up == self.watchers
    }
}

/// `first_revision` is the store's revision before any write.
pub fn check(
    writes: &[WatchWrite],
    watchers: &[Watcher],
    first_revision: u64,
    final_revision: u64,
) -> WatchReport {
    let mut r = WatchReport {
        writes_ok: writes.iter().filter(|w| w.outcome == Outcome::Ok).count(),
        watchers: watchers.len(),
        final_revision,
        ..WatchReport::default()
    };
    let mut v = Vec::new();
    // What each (key, value) write did.
    let mut attempted: HashMap<(u64, u64), Outcome> = HashMap::new();
    for w in writes {
        attempted.insert((w.key, w.value), w.outcome);
    }
    // The event at each revision, as the first watcher to see it reported.
    let mut at: BTreeMap<u64, (u64, u64, usize)> = BTreeMap::new();
    for (i, w) in watchers.iter().enumerate() {
        r.events += w.events.len();
        r.reconnects += w.reconnects;
        let mut last = first_revision;
        for e in &w.events {
            if e.revision <= last {
                v.push(format!(
                    "watcher {i} (n{}) saw revision {} after {last}: out of order or duplicated",
                    w.node + 1,
                    e.revision
                ));
            }
            last = last.max(e.revision);
            match at.get(&e.revision) {
                Some(&(k, val, j)) if (k, val) != (e.key, e.value) => v.push(format!(
                    "watchers disagree on revision {}: watcher {j} saw key {k} = {val}, watcher {i} saw key {} = {}",
                    e.revision, e.key, e.value
                )),
                Some(_) => {}
                None => {
                    at.insert(e.revision, (e.key, e.value, i));
                }
            }
            match attempted.get(&(e.key, e.value)) {
                None => v.push(format!(
                    "watcher {i} saw key {} = {} at revision {}, a value never written",
                    e.key, e.value, e.revision
                )),
                Some(Outcome::Fail) => v.push(format!(
                    "watcher {i} saw key {} = {} at revision {}, a write that definitely failed",
                    e.key, e.value, e.revision
                )),
                _ => {}
            }
        }
        if last >= final_revision {
            r.caught_up += 1;
            let seen: std::collections::BTreeSet<u64> =
                w.events.iter().map(|e| e.revision).collect();
            let missing: Vec<u64> = (first_revision + 1..=final_revision)
                .filter(|x| !seen.contains(x))
                .collect();
            if !missing.is_empty() {
                v.push(format!(
                    "watcher {i} (n{}) missed {} revisions, first {:?}",
                    w.node + 1,
                    missing.len(),
                    &missing[..missing.len().min(8)]
                ));
            }
        }
    }
    // Acknowledged writes must appear at the revision they were given.
    for w in writes {
        if let (Outcome::Ok, Some(rev)) = (w.outcome, w.revision)
            && let Some(&(k, val, j)) = at.get(&rev)
            && (k, val) != (w.key, w.value)
        {
            v.push(format!(
                "write of key {} = {} was acknowledged at revision {rev}, but watcher {j} saw key {k} = {val} there",
                w.key, w.value
            ));
        }
    }
    v.dedup();
    r.violations = v;
    r
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(key: u64, value: u64, revision: u64) -> WatchEvent {
        WatchEvent {
            key,
            value,
            revision,
        }
    }

    fn write(key: u64, value: u64, revision: u64) -> WatchWrite {
        WatchWrite {
            process: 0,
            key,
            value,
            outcome: Outcome::Ok,
            revision: Some(revision),
        }
    }

    #[test]
    fn a_complete_ordered_stream_is_valid() {
        let writes = [write(1, 10, 2), write(2, 11, 3)];
        let w = Watcher {
            node: 0,
            events: vec![ev(1, 10, 2), ev(2, 11, 3)],
            reconnects: 1,
        };
        let r = check(&writes, &[w], 1, 3);
        assert!(r.valid(), "{:?}", r.violations);
    }

    #[test]
    fn gaps_duplicates_phantoms_and_disagreement_are_caught() {
        let writes = [write(1, 10, 2), write(2, 11, 3), write(1, 12, 4)];
        let gap = Watcher {
            node: 0,
            events: vec![ev(1, 10, 2), ev(1, 12, 4)],
            reconnects: 0,
        };
        let dup = Watcher {
            node: 1,
            events: vec![ev(1, 10, 2), ev(1, 10, 2), ev(2, 11, 3), ev(1, 12, 4)],
            reconnects: 0,
        };
        let odd = Watcher {
            node: 2,
            events: vec![ev(1, 10, 2), ev(2, 99, 3), ev(1, 12, 4)],
            reconnects: 0,
        };
        let r = check(&writes, &[gap, dup, odd], 1, 4);
        let all = r.violations.join("\n");
        assert!(all.contains("missed 1 revisions"), "{all}");
        assert!(all.contains("out of order or duplicated"), "{all}");
        assert!(all.contains("never written"), "{all}");
        assert!(all.contains("disagree"), "{all}");
        assert!(!r.valid());
    }
}
