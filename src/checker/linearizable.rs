//! Linearizability of a register with reads, writes and compare-and-set,
//! after Wing & Gong with Lowe's memoization (the algorithm behind Knossos
//! and Porcupine).
//!
//! A history is linearizable if every operation can be placed at one
//! instant between its invocation and its response so that the sequence
//! is a valid execution of a single register. Failed operations never
//! happened and are dropped. An operation with an unknown outcome has its
//! response at infinity: it may take effect at any point after it began,
//! or never; for a write or a compare-and-set, never is the same as last,
//! since nothing observes the register after the end.
//!
//! Keys are independent (linearizability is compositional), so a history
//! is checked one key at a time.

use std::collections::HashSet;

use crate::history::{Call, Op, Outcome};

#[derive(Clone, Copy)]
struct Event {
    op: usize,
    is_call: bool,
    prev: usize,
    next: usize,
    matching: usize,
}

const HEAD: usize = usize::MAX - 1;
const NIL: usize = usize::MAX;

/// Why a key's history is not linearizable.
#[derive(Debug)]
pub struct Violation {
    pub key: u64,
    /// The operations that could not all be linearized: the longest
    /// linearizable prefix found, and the operation that broke it.
    pub explanation: String,
}

/// The step of the register model: whether `op` may take effect in
/// `state`, and the state after it.
fn step(op: &Op, state: Option<u64>) -> Option<Option<u64>> {
    let known = op.outcome == Outcome::Ok;
    match op.call {
        Call::Write(v) => Some(Some(v)),
        Call::Read(v) => (v == state).then_some(state),
        Call::Cas(a, b) if state == Some(a) => Some(Some(b)),
        // A compare-and-set that failed its comparison has no effect; one
        // acknowledged as successful must have compared equal.
        Call::Cas(..) => (!known).then_some(state),
        Call::Add(_) | Call::ReadSet(_) => None,
    }
}

/// Check one key's history. `Ok(n)` gives the search steps it took.
pub fn check_key(key: u64, history: &[&Op], budget: u64) -> Result<u64, Violation> {
    // Failed operations never happened; reads with no answer saw nothing.
    let ops: Vec<&Op> = history
        .iter()
        .copied()
        .filter(|o| match o.outcome {
            Outcome::Fail => false,
            Outcome::Info => !matches!(o.call, Call::Read(_)),
            Outcome::Ok => true,
        })
        .collect();
    if ops.is_empty() {
        return Ok(0);
    }
    let ret = |o: &Op| {
        if o.outcome == Outcome::Ok {
            o.end
        } else {
            u64::MAX
        }
    };
    // Events in time order; at equal times calls come first, so operations
    // that touch count as concurrent.
    let mut order: Vec<(u64, bool, usize)> = Vec::with_capacity(ops.len() * 2);
    for (i, o) in ops.iter().enumerate() {
        order.push((o.start, true, i));
        order.push((ret(o), false, i));
    }
    order.sort_by_key(|&(t, is_call, i)| (t, !is_call, i));
    let mut events: Vec<Event> = order
        .iter()
        .map(|&(_, is_call, op)| Event {
            op,
            is_call,
            prev: NIL,
            next: NIL,
            matching: NIL,
        })
        .collect();
    let n = events.len();
    let mut ret_of = vec![NIL; ops.len()];
    for (i, e) in events.iter().enumerate() {
        if !e.is_call {
            ret_of[e.op] = i;
        }
    }
    for i in 0..n {
        events[i].prev = if i == 0 { HEAD } else { i - 1 };
        events[i].next = if i + 1 == n { NIL } else { i + 1 };
        if events[i].is_call {
            events[i].matching = ret_of[events[i].op];
        }
    }

    fn lift(events: &mut [Event], head: &mut usize, c: usize) {
        for idx in [c, events[c].matching] {
            let (p, nx) = (events[idx].prev, events[idx].next);
            if p == HEAD {
                *head = nx;
            } else {
                events[p].next = nx;
            }
            if nx != NIL {
                events[nx].prev = p;
            }
        }
    }
    fn unlift(events: &mut [Event], head: &mut usize, c: usize) {
        for idx in [events[c].matching, c] {
            let (p, nx) = (events[idx].prev, events[idx].next);
            if p == HEAD {
                *head = idx;
            } else {
                events[p].next = idx;
            }
            if nx != NIL {
                events[nx].prev = idx;
            }
        }
    }

    let mut head = 0usize;
    let words = ops.len().div_ceil(64);
    let mut linearized = vec![0u64; words];
    let mut cache: HashSet<(Vec<u64>, Option<u64>)> = HashSet::new();
    let mut state: Option<u64> = None;
    let mut stack: Vec<(usize, Option<u64>)> = Vec::new();
    let mut entry = head;
    let mut steps = 0u64;
    // The deepest the search got, to explain a failure.
    let mut best: (usize, Vec<usize>) = (0, Vec::new());

    loop {
        steps += 1;
        if steps > budget {
            return Err(Violation {
                key,
                explanation: format!(
                    "undecided: the search exceeded its budget of {budget} steps ({} operations)",
                    ops.len()
                ),
            });
        }
        if head == NIL {
            return Ok(steps);
        }
        if entry == NIL {
            return Err(explain(key, &ops, &best));
        }
        let e = events[entry];
        if e.is_call {
            if let Some(next_state) = step(ops[e.op], state) {
                let mut bits = linearized.clone();
                bits[e.op / 64] |= 1 << (e.op % 64);
                if cache.insert((bits.clone(), next_state)) {
                    stack.push((entry, state));
                    if stack.len() > best.0 {
                        best = (
                            stack.len(),
                            stack.iter().map(|&(c, _)| events[c].op).collect(),
                        );
                    }
                    linearized = bits;
                    state = next_state;
                    lift(&mut events, &mut head, entry);
                    entry = head;
                    continue;
                }
            }
            entry = e.next;
        } else {
            // A response before its call could be linearized: backtrack.
            let Some((c, prev_state)) = stack.pop() else {
                return Err(explain(key, &ops, &best));
            };
            let op = events[c].op;
            linearized[op / 64] &= !(1 << (op % 64));
            state = prev_state;
            unlift(&mut events, &mut head, c);
            entry = events[c].next;
        }
    }
}

fn explain(key: u64, ops: &[&Op], best: &(usize, Vec<usize>)) -> Violation {
    const SHOW: usize = 8;
    let done: HashSet<usize> = best.1.iter().copied().collect();
    let mut lines = vec![format!(
        "no order of these {} operations is a valid register history. The longest valid order found \
         places {} of them; its last steps:",
        ops.len(),
        best.1.len()
    )];
    let mut state: Option<u64> = None;
    for (n, &i) in best.1.iter().enumerate() {
        state = step(ops[i], state).unwrap_or(state);
        if n + SHOW >= best.1.len() {
            let shown = state.map_or("nil".into(), |v| v.to_string());
            lines.push(format!("  ok     {}   => register {shown}", ops[i]));
        }
    }
    // The operations that had to come next but could not: those that
    // completed earliest among the rest.
    let mut rest: Vec<&&Op> = ops
        .iter()
        .enumerate()
        .filter(|(i, _)| !done.contains(i))
        .map(|(_, o)| o)
        .collect();
    rest.sort_by_key(|o| {
        if o.outcome == Outcome::Ok {
            o.end
        } else {
            u64::MAX
        }
    });
    lines.push("then none of these can come next:".into());
    for o in rest.iter().take(4) {
        lines.push(format!("  stuck  {o}"));
    }
    Violation {
        key,
        explanation: lines.join("\n"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn op(call: Call, start: u64, end: u64, outcome: Outcome) -> Op {
        Op {
            process: 0,
            node: 0,
            key: 1,
            call,
            start,
            end,
            outcome,
            error: None,
        }
    }
    use Outcome::*;

    fn check(h: &[Op]) -> bool {
        check_key(1, &h.iter().collect::<Vec<_>>(), 1_000_000).is_ok()
    }

    #[test]
    fn sequential_and_concurrent_histories() {
        assert!(check(&[
            op(Call::Write(1), 0, 1, Ok),
            op(Call::Read(Some(1)), 2, 3, Ok)
        ]));
        // A read concurrent with a write may see either value.
        assert!(check(&[
            op(Call::Write(1), 0, 1, Ok),
            op(Call::Write(2), 2, 10, Ok),
            op(Call::Read(Some(1)), 3, 4, Ok),
            op(Call::Read(Some(2)), 5, 6, Ok),
        ]));
    }

    #[test]
    fn a_stale_read_is_caught() {
        assert!(!check(&[
            op(Call::Write(1), 0, 1, Ok),
            op(Call::Write(2), 2, 3, Ok),
            op(Call::Read(Some(1)), 4, 5, Ok),
        ]));
    }

    #[test]
    fn compare_and_set() {
        assert!(check(&[
            op(Call::Write(1), 0, 1, Ok),
            op(Call::Cas(1, 2), 2, 3, Ok),
            op(Call::Read(Some(2)), 4, 5, Ok),
        ]));
        // Two successful compare-and-sets from the same value, one after
        // the other: the second cannot have compared equal.
        assert!(!check(&[
            op(Call::Write(1), 0, 1, Ok),
            op(Call::Cas(1, 2), 2, 3, Ok),
            op(Call::Cas(1, 3), 4, 5, Ok),
        ]));
        // A failed one never happened.
        assert!(check(&[
            op(Call::Write(1), 0, 1, Ok),
            op(Call::Cas(5, 2), 2, 3, Fail),
            op(Call::Read(Some(1)), 4, 5, Ok),
        ]));
    }

    #[test]
    fn unknown_outcomes_may_apply_late_or_never_but_not_early() {
        let h = [
            op(Call::Write(1), 0, 1, Ok),
            op(Call::Cas(1, 9), 2, 3, Info),
            op(Call::Read(Some(1)), 4, 5, Ok),
            op(Call::Read(Some(9)), 6, 7, Ok),
        ];
        assert!(check(&h));
        assert!(check(&h[..3]));
        // A write that began after the read cannot explain what it saw.
        assert!(!check(&[
            op(Call::Write(1), 0, 1, Ok),
            op(Call::Read(Some(7)), 2, 3, Ok),
            op(Call::Write(7), 10, 11, Info),
        ]));
    }
}
