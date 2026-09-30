//! What a test leaves behind in its store directory: the full history, the
//! nemesis log, and a results summary, next to each node's logs.

use std::fmt::Write as _;

use crate::db::Database;
use crate::history::{self, Call, Outcome};
use crate::test::{Config, Report};

/// Whether a call is of one kind (for the per-kind table).
type IsKind = fn(&Call) -> bool;

/// The results summary, as printed and as saved.
pub fn summary(r: &Report, db: &dyn Database, cfg: &Config) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "{} on {} nodes, {} clients, {}s, faults {:?} on {:?} nodes, seed {}",
        db.name(),
        cfg.nodes,
        cfg.clients,
        cfg.time.as_secs(),
        cfg.faults,
        cfg.target,
        cfg.seed
    );
    if let Some((ops, l)) = &r.locks {
        let stalls = ops.iter().filter(|o| o.stalled).count();
        let _ = writeln!(
            out,
            "\n  workload: lock ({:?}), {} acquisitions, {} stalled past their lease",
            cfg.workload, l.acquisitions, stalls
        );
        let _ = writeln!(
            out,
            "  {} increments acknowledged, {} refused by the fence",
            l.increments, l.fenced_out
        );
        let _ = writeln!(
            out,
            "  mutual exclusion: {} times two clients held the lock at once",
            l.overlaps.len()
        );
        for (a, b, from, to) in l.overlaps.iter().take(3) {
            let _ = writeln!(
                out,
                "    p{a} and p{b} both held it from {:.3}s to {:.3}s",
                *from as f64 / 1e9,
                *to as f64 / 1e9
            );
        }
    }
    let register_ops: &[(&str, IsKind)] = if r.locks.is_some() {
        &[]
    } else {
        let _ = writeln!(
            out,
            "\n  {:<6} {:>7} {:>7} {:>8}   latency of ok (p50 / p99)",
            "op", "ok", "fail", "unknown"
        );
        &[
            ("read", |c| matches!(c, Call::Read(_))),
            ("write", |c| matches!(c, Call::Write(_))),
            ("cas", |c| matches!(c, Call::Cas(..))),
        ]
    };
    for &(name, pick) in register_ops {
        let ops: Vec<_> = r.history.iter().filter(|o| pick(&o.call)).collect();
        let count = |k: Outcome| ops.iter().filter(|o| o.outcome == k).count();
        let mut lat: Vec<u64> = ops
            .iter()
            .filter(|o| o.outcome == Outcome::Ok)
            .map(|o| o.end - o.start)
            .collect();
        lat.sort_unstable();
        let pct = |p: f64| {
            lat.get(((lat.len() as f64 * p) as usize).min(lat.len().saturating_sub(1)))
                .map_or(0.0, |&v| v as f64 / 1e6)
        };
        let _ = writeln!(
            out,
            "  {name:<6} {:>7} {:>7} {:>8}   {:.1} / {:.1} ms",
            count(Outcome::Ok),
            count(Outcome::Fail),
            count(Outcome::Info),
            pct(0.5),
            pct(0.99)
        );
    }
    let _ = writeln!(out, "\n  faults injected:");
    if r.nemesis.is_empty() {
        let _ = writeln!(out, "    none");
    }
    for (t, what) in &r.nemesis {
        let _ = writeln!(out, "    {:>8.3}s  {what}", *t as f64 / 1e9);
    }
    let _ = writeln!(out);
    if let Some((_, l)) = &r.locks {
        if l.valid() {
            let _ = writeln!(
                out,
                "  VALID: no lost updates: every acknowledged increment read a distinct value"
            );
        } else {
            let lost: usize = l.lost_updates.iter().map(|(_, ps)| ps.len() - 1).sum();
            let _ = writeln!(
                out,
                "  INVALID: {lost} lost updates: acknowledged increments that read the same value"
            );
            for (v, ps) in l.lost_updates.iter().take(3) {
                let _ = writeln!(
                    out,
                    "    counter {v}: incremented by p{ps:?}, each writing {}",
                    v + 1
                );
            }
        }
        let _ = writeln!(
            out,
            "\n  history, nemesis log and node logs: {}",
            r.dir.display()
        );
        return out;
    }
    match r.valid() {
        Some(true) => {
            let _ = writeln!(
                out,
                "  VALID: all {} keys' histories are linearizable",
                r.keys
            );
        }
        Some(false) => {
            let _ = writeln!(
                out,
                "  INVALID: {} of {} keys' histories are not linearizable",
                r.violations.len(),
                r.keys
            );
            for v in r.violations.iter().take(3) {
                let _ = writeln!(
                    out,
                    "\n  key {}: {}",
                    v.key,
                    v.explanation.replace('\n', "\n  ")
                );
            }
        }
        None => {
            let _ = writeln!(
                out,
                "  UNKNOWN: no violation found, but {} of {} keys were too complex to decide: {:?}",
                r.undecided.len(),
                r.keys,
                r.undecided
            );
        }
    }
    let _ = writeln!(
        out,
        "\n  history, nemesis log and node logs: {}",
        r.dir.display()
    );
    out
}

pub fn write(r: &Report, db: &dyn Database, cfg: &Config) -> Result<(), String> {
    let lines: Vec<String> = r.history.iter().map(history::to_json).collect();
    std::fs::write(r.dir.join("history.jsonl"), lines.join("\n") + "\n")
        .map_err(|e| e.to_string())?;
    let nemesis: Vec<String> = r
        .nemesis
        .iter()
        .map(|(t, w)| format!("{:.3}s {w}", *t as f64 / 1e9))
        .collect();
    std::fs::write(r.dir.join("nemesis.txt"), nemesis.join("\n") + "\n")
        .map_err(|e| e.to_string())?;
    std::fs::write(r.dir.join("results.txt"), summary(r, db, cfg)).map_err(|e| e.to_string())
}
