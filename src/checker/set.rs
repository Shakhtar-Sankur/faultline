//! The set checker: clients add unique elements; at the end, after the
//! faults heal, a final read returns the set. Every acknowledged add must
//! be there. An add that failed must not be. An add with an unknown
//! outcome may go either way.

use std::collections::BTreeSet;

use crate::history::{Call, Op, Outcome};

#[derive(Debug, Default)]
pub struct SetReport {
    pub attempted: usize,
    pub acknowledged: usize,
    /// Acknowledged adds missing from the final read: lost writes.
    pub lost: Vec<u64>,
    /// Elements in the final read that were never added, or whose add
    /// definitely failed.
    pub unexpected: Vec<u64>,
    /// Adds with unknown outcomes that turned out to have happened.
    pub recovered: usize,
    pub final_size: usize,
}

impl SetReport {
    pub fn valid(&self) -> bool {
        self.lost.is_empty() && self.unexpected.is_empty()
    }
}

/// `None` if the history holds no successful final read.
pub fn check(history: &[Op]) -> Option<SetReport> {
    let final_read = history
        .iter()
        .rev()
        .find_map(|o| match (&o.call, o.outcome) {
            (Call::ReadSet(s), Outcome::Ok) => Some(s.iter().copied().collect::<BTreeSet<u64>>()),
            _ => None,
        })?;
    let mut r = SetReport {
        final_size: final_read.len(),
        ..SetReport::default()
    };
    let mut may_exist = BTreeSet::new();
    for o in history {
        let Call::Add(v) = o.call else { continue };
        r.attempted += 1;
        match o.outcome {
            Outcome::Ok => {
                r.acknowledged += 1;
                may_exist.insert(v);
                if !final_read.contains(&v) {
                    r.lost.push(v);
                }
            }
            Outcome::Info => {
                may_exist.insert(v);
                if final_read.contains(&v) {
                    r.recovered += 1;
                }
            }
            Outcome::Fail => {}
        }
    }
    r.unexpected = final_read.difference(&may_exist).copied().collect();
    Some(r)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn add(v: u64, outcome: Outcome) -> Op {
        Op {
            process: 0,
            node: 0,
            key: 0,
            call: Call::Add(v),
            start: v,
            end: v + 1,
            outcome,
            error: None,
        }
    }

    #[test]
    fn finds_lost_and_unexpected_elements() {
        let mut h = vec![
            add(1, Outcome::Ok),
            add(2, Outcome::Ok),
            add(3, Outcome::Info),
            add(4, Outcome::Fail),
        ];
        h.push(Op {
            call: Call::ReadSet(vec![1, 3, 4]),
            ..add(9, Outcome::Ok)
        });
        let r = check(&h).unwrap();
        assert_eq!(r.lost, vec![2]);
        assert_eq!(r.unexpected, vec![4]);
        assert_eq!((r.attempted, r.acknowledged, r.recovered), (4, 2, 1));
        assert!(!r.valid());
    }
}
