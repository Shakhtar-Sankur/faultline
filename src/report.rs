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
    if let Some((_, w)) = &r.watch {
        let _ = writeln!(
            out,
            "\n  workload: watch, {} acknowledged writes, revisions up to {}",
            w.writes_ok, w.final_revision
        );
        let _ = writeln!(
            out,
            "  {} watchers received {} events over {} reconnections; {} of {} caught up",
            w.watchers, w.events, w.reconnects, w.caught_up, w.watchers
        );
    }
    let register_ops: &[(&str, IsKind)] = if r.locks.is_some() || r.watch.is_some() {
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
    if let Some((_, w)) = &r.watch {
        if w.valid() {
            let _ = writeln!(
                out,
                "  VALID: every watcher saw every revision once, in order, and all agreed"
            );
        } else {
            let _ = writeln!(out, "  INVALID: {} watch violations", w.violations.len());
            if w.caught_up < w.watchers {
                let _ = writeln!(
                    out,
                    "    {} watchers never caught up to the final revision",
                    w.watchers - w.caught_up
                );
            }
            for v in w.violations.iter().take(8) {
                let _ = writeln!(out, "    {v}");
            }
        }
        let _ = writeln!(
            out,
            "\n  history, nemesis log and node logs: {}",
            r.dir.display()
        );
        return out;
    }
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
    std::fs::write(r.dir.join("results.txt"), summary(r, db, cfg)).map_err(|e| e.to_string())?;
    std::fs::write(r.dir.join("report.html"), html(r, db, cfg)).map_err(|e| e.to_string())
}

fn outcome_code(o: Outcome) -> u8 {
    match o {
        Outcome::Ok => 0,
        Outcome::Fail => 1,
        Outcome::Info => 2,
    }
}

fn ms(ns: u64) -> String {
    format!("{:.3}", ns as f64 / 1e6)
}

/// Pair each fault with its repair: `(from ns, to ns, what)`.
fn fault_intervals(r: &Report) -> Vec<(u64, u64, String)> {
    let mut out: Vec<(u64, u64, String)> = Vec::new();
    let mut open: Vec<usize> = Vec::new();
    for (t, what) in &r.nemesis {
        let repair = ["heal", "restart", "resume"]
            .iter()
            .any(|w| what.starts_with(w));
        if repair {
            if let Some(i) = open.pop() {
                out[i].1 = *t;
            }
        } else if !what.starts_with("nemesis error") {
            open.push(out.len());
            out.push((*t, r.duration, what.clone()));
        }
    }
    out
}

/// The run as one self-contained page: every operation's latency over
/// time, throughput by outcome, faults shaded, violations marked.
pub fn html(r: &Report, db: &dyn Database, cfg: &Config) -> String {
    use crate::json::quote;
    let mut points: Vec<String> = Vec::new();
    let mut violations: Vec<(u64, u64, String)> = Vec::new();
    let (verdict, detail);
    if let Some((ops, l)) = &r.locks {
        for o in ops {
            points.push(format!(
                "[{},{},{},\"lock held\",{},-1]",
                ms(o.acquired),
                ms(o.released - o.acquired),
                outcome_code(o.wrote),
                o.node + 1
            ));
        }
        for (v, ps) in &l.lost_updates {
            let times: Vec<&crate::checker::lock::LockOp> = ops
                .iter()
                .filter(|o| o.read == *v && ps.contains(&o.process))
                .collect();
            if let (Some(a), Some(b)) = (
                times.iter().map(|o| o.acquired).min(),
                times.iter().map(|o| o.released).max(),
            ) {
                violations.push((a, b, format!("lost update of counter {v}")));
            }
        }
        verdict = if l.valid() {
            format!(
                "Valid: no lost updates ({} increments; lock held twice at once {} times)",
                l.increments,
                l.overlaps.len()
            )
        } else {
            let lost: usize = l.lost_updates.iter().map(|(_, ps)| ps.len() - 1).sum();
            format!(
                "Invalid: {lost} lost updates; lock held twice at once {} times",
                l.overlaps.len()
            )
        };
        detail = l
            .lost_updates
            .iter()
            .take(5)
            .map(|(v, ps)| {
                format!(
                    "counter {v}: incremented by p{ps:?}, each writing {}",
                    v + 1
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
    } else if let Some((writes, w)) = &r.watch {
        for o in writes {
            points.push(format!(
                "[{},{},{},\"watched write\",{},{}]",
                ms(o.start),
                ms(o.end - o.start),
                outcome_code(o.outcome),
                o.node + 1,
                o.key
            ));
        }
        verdict = if w.valid() {
            format!(
                "Valid: {} watchers saw all {} revisions once each, in order, and agreed",
                w.watchers,
                w.final_revision.saturating_sub(1)
            )
        } else {
            format!("Invalid: {} watch violations", w.violations.len())
        };
        detail = w
            .violations
            .iter()
            .take(8)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n");
    } else {
        for o in &r.history {
            let kind = match o.call {
                Call::Read(_) => "read",
                Call::Write(_) => "write",
                Call::Cas(..) => "cas",
                Call::Add(_) => "add",
                Call::ReadSet(_) => "read set",
            };
            points.push(format!(
                "[{},{},{},\"{kind}\",{},{}]",
                ms(o.start),
                ms(o.end - o.start),
                outcome_code(o.outcome),
                o.node + 1,
                o.key
            ));
        }
        for v in &r.violations {
            let ops = r.history.iter().filter(|o| o.key == v.key);
            let (a, b) = ops.fold((u64::MAX, 0), |(a, b), o| (a.min(o.start), b.max(o.end)));
            violations.push((a, b, format!("key {} not linearizable", v.key)));
        }
        verdict = match r.valid() {
            Some(true) => format!("Valid: all {} keys' histories are linearizable", r.keys),
            Some(false) => format!(
                "Invalid: {} of {} keys' histories are not linearizable",
                r.violations.len(),
                r.keys
            ),
            None => format!(
                "Undecided: {} keys were too complex to check",
                r.undecided.len()
            ),
        };
        detail = r
            .violations
            .first()
            .map(|v| format!("key {}: {}", v.key, v.explanation))
            .unwrap_or_default();
    }
    let triple = |v: &[(u64, u64, String)]| {
        v.iter()
            .map(|(a, b, w)| format!("[{},{},{}]", ms(*a), ms(*b), quote(w)))
            .collect::<Vec<_>>()
            .join(",")
    };
    let config = format!(
        "{} nodes, {} clients, {}s, workload {:?}, faults {:?} on {:?} nodes, seed {}",
        cfg.nodes,
        cfg.clients,
        cfg.time.as_secs(),
        cfg.workload,
        cfg.faults,
        cfg.target,
        cfg.seed
    );
    let data = format!(
        "{{\"db\":{},\"config\":{},\"valid\":{},\"verdict\":{},\"detail\":{},\"duration_ms\":{},\"faults\":[{}],\"violations\":[{}],\"points\":[{}]}}",
        quote(&db.name()),
        quote(&config),
        r.valid() == Some(true),
        quote(&verdict),
        quote(&detail),
        ms(r.duration),
        triple(&fault_intervals(r)),
        triple(&violations),
        points.join(",")
    );
    // Keep the data from closing the script element.
    HTML.replace("/*DATA*/null", &data.replace("</", "<\\/"))
}

const HTML: &str = include_str!("report.html");
