//! Run a test: start the cluster, let concurrent clients operate on it
//! while the nemesis injects faults, then heal everything and check the
//! history.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::checker::linearizable::{self, Violation};
use crate::checker::lock::{self as lockcheck, LockOp, LockReport};
use crate::checker::watch::{self as watchcheck, WatchReport, WatchWrite, Watcher};
use crate::cluster::Cluster;
use crate::db::{self, Database, Error};
use crate::history::{Call, Op, Outcome};
use crate::rng::Rng;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fault {
    /// Split the nodes into a majority and a minority, or cut one off.
    Partition,
    /// SIGKILL a node, restart it later.
    Kill,
    /// SIGSTOP a node, SIGCONT it later.
    Pause,
}

/// What the clients do.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Workload {
    /// Reads, writes and compare-and-set on single keys; checked for
    /// linearizability.
    Register,
    /// Increment a counter while holding a lease-based lock; checked for
    /// mutual exclusion and lost updates. `fenced` makes each write
    /// conditional on still owning the lock; `stall_percent` of critical
    /// sections stall past the lease, as a long GC pause would.
    Lock { fenced: bool, stall_percent: u64 },
    /// Writers put unique values while one watcher per node streams every
    /// change; checked for order, agreement, phantoms and gaps.
    Watch,
}

/// Which node a fault picks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Target {
    Random,
    /// The current leader (random when none can be found), and for a
    /// partition, the leader alone against everyone else.
    Leader,
}

impl Fault {
    pub fn parse(s: &str) -> Option<Fault> {
        match s {
            "partition" => Some(Fault::Partition),
            "kill" => Some(Fault::Kill),
            "pause" => Some(Fault::Pause),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Config {
    pub nodes: usize,
    pub clients: usize,
    pub time: Duration,
    pub faults: Vec<Fault>,
    /// Quiet time before each fault, and how long each fault lasts.
    pub fault_gap: Duration,
    pub fault_for: Duration,
    pub target: Target,
    pub workload: Workload,
    /// Watchers resume after a disconnect from the last revision seen plus
    /// this. 1 is correct; 0 (duplicates) and 2 (gaps) are planted client
    /// bugs, to prove the watch checker catches them.
    pub watch_resume: u64,
    /// Operations on one key before clients move to the next, which keeps
    /// each key's history small enough to check exhaustively.
    pub ops_per_key: u64,
    /// How long a client waits for an answer before giving up (the
    /// operation's outcome is then unknown).
    pub timeout: Duration,
    /// Pause between one client's operations.
    pub op_delay: Duration,
    pub seed: u64,
    pub store: PathBuf,
}

pub struct Report {
    pub history: Vec<Op>,
    /// `(ns since start, what the nemesis did)`.
    pub nemesis: Vec<(u64, String)>,
    pub keys: usize,
    pub violations: Vec<Violation>,
    /// Keys whose check exceeded its search budget.
    pub undecided: Vec<u64>,
    /// The lock workload's operations and verdict.
    pub locks: Option<(Vec<LockOp>, LockReport)>,
    pub watch: Option<(Vec<WatchWrite>, WatchReport)>,
    pub dir: PathBuf,
    /// How long the workload ran, in ns.
    pub duration: u64,
}

impl Report {
    pub fn valid(&self) -> Option<bool> {
        if let Some((_, l)) = &self.locks {
            return Some(l.valid());
        }
        if let Some((_, w)) = &self.watch {
            return Some(w.valid());
        }
        if !self.violations.is_empty() {
            Some(false)
        } else if !self.undecided.is_empty() {
            None
        } else {
            Some(true)
        }
    }
}

/// Removes the cluster however the test ends.
struct Teardown<'a>(&'a Cluster);

impl Drop for Teardown<'_> {
    fn drop(&mut self) {
        self.0.destroy();
    }
}

fn random_call(rng: &mut Rng) -> Call {
    // Values from a small space, so compare-and-sets often succeed.
    match rng.below(4) {
        0 | 1 => Call::Read(None),
        2 => Call::Write(rng.below(5)),
        _ => Call::Cas(rng.below(5), rng.below(5)),
    }
}

pub fn run(db: &dyn Database, cfg: &Config) -> Result<Report, String> {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let slug: String = db
        .name()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect();
    let dir = cfg
        .store
        .join(slug.trim_matches('-'))
        .join(stamp.to_string());
    std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;

    let cluster = Cluster::create(cfg.nodes, &dir)?;
    let _teardown = Teardown(&cluster);
    db::start_all(db, &cluster, Duration::from_secs(60))?;

    let t0 = Instant::now();
    let ns = || t0.elapsed().as_nanos() as u64;
    let deadline = t0 + cfg.time;
    let history: Mutex<Vec<Op>> = Mutex::new(Vec::new());
    let nemesis_log: Mutex<Vec<(u64, String)>> = Mutex::new(Vec::new());
    let next_op = AtomicU64::new(0);
    let lock_ops: Mutex<Vec<LockOp>> = Mutex::new(Vec::new());
    let watch_writes: Mutex<Vec<WatchWrite>> = Mutex::new(Vec::new());
    let watchers: Mutex<Vec<Watcher>> = Mutex::new(Vec::new());
    // The revision watchers must reach before they stop (0: not known
    // yet), and whether to give up on that.
    let watch_target = AtomicU64::new(0);
    let abort = AtomicBool::new(false);
    let first_revision = match cfg.workload {
        Workload::Watch => current_revision(db, &cluster, cfg)?,
        _ => 0,
    };

    std::thread::scope(|s| -> Result<(), String> {
        let mut workers = Vec::new();
        if cfg.workload == Workload::Watch {
            let next_value = &next_op;
            for p in 0..cfg.clients {
                let (cluster, out) = (&cluster, &watch_writes);
                workers.push(s.spawn(move || {
                    watch_writer(db, cluster, cfg, p, deadline, t0, next_value, out)
                }));
            }
            for node in 0..cfg.nodes {
                let (cluster, out) = (&cluster, &watchers);
                let (target, abort) = (&watch_target, &abort);
                s.spawn(move || {
                    let w = watcher(
                        db,
                        cluster,
                        cfg,
                        node,
                        first_revision,
                        deadline,
                        target,
                        abort,
                    );
                    out.lock().unwrap().push(w);
                });
            }
        }
        if let Workload::Lock {
            fenced,
            stall_percent,
        } = cfg.workload
        {
            for p in 0..cfg.clients {
                let (cluster, out) = (&cluster, &lock_ops);
                workers.push(s.spawn(move || {
                    lock_client(
                        db,
                        cluster,
                        cfg,
                        p,
                        deadline,
                        t0,
                        fenced,
                        stall_percent,
                        out,
                    )
                }));
            }
        }
        for p in (0..cfg.clients).filter(|_| cfg.workload == Workload::Register) {
            let (history, next_op, cluster) = (&history, &next_op, &cluster);
            workers.push(s.spawn(move || {
                let node = p % cfg.nodes;
                let mut client = db.client(cluster, node, cfg.timeout);
                let mut rng = Rng::new(cfg.seed ^ ((p as u64 + 1) * 0x5851_F42D));
                // After an unknown outcome the client's old request may
                // still be in flight, so it continues as a new process.
                let mut process = p;
                while Instant::now() < deadline {
                    let key = next_op.fetch_add(1, Ordering::Relaxed) / cfg.ops_per_key;
                    let call = random_call(&mut rng);
                    let start = ns();
                    let result = client.invoke(key, &call);
                    let end = ns();
                    let (call, outcome, error) = match result {
                        Ok(c) => (c, Outcome::Ok, None),
                        Err(Error::Fail(e)) => (call, Outcome::Fail, Some(e)),
                        Err(Error::Unknown(e)) => (call, Outcome::Info, Some(e)),
                    };
                    history.lock().unwrap().push(Op {
                        process,
                        node,
                        key,
                        call,
                        start,
                        end,
                        outcome,
                        error,
                    });
                    if outcome == Outcome::Info {
                        process += cfg.clients;
                    }
                    std::thread::sleep(cfg.op_delay);
                }
            }));
        }

        if !cfg.faults.is_empty() {
            let (cluster, log) = (&cluster, &nemesis_log);
            workers.push(s.spawn(move || nemesis(db, cluster, cfg, deadline, t0, log)));
        }
        for w in workers {
            let _ = w.join();
        }

        // Heal whatever is left, so the cluster can settle (and watchers
        // catch up) before it is torn down.
        let healed = (|| {
            cluster.heal()?;
            for i in 0..cluster.nodes.len() {
                let _ = cluster.resume(i);
                if !cluster.is_running(i) {
                    db.start(&cluster, i)?;
                }
            }
            if cfg.workload == Workload::Watch {
                watch_target.store(current_revision(db, &cluster, cfg)?, Ordering::SeqCst);
            }
            Ok(())
        })();
        if healed.is_err() {
            abort.store(true, Ordering::SeqCst);
        }
        healed
    })?;

    let mut history = history.into_inner().unwrap();
    history.sort_by_key(|o| (o.start, o.process));
    let mut by_key: BTreeMap<u64, Vec<&Op>> = BTreeMap::new();
    for o in &history {
        by_key.entry(o.key).or_default().push(o);
    }
    let mut violations = Vec::new();
    let mut undecided = Vec::new();
    for (&key, ops) in &by_key {
        match linearizable::check_key(key, ops, 20_000_000) {
            Ok(_) => {}
            Err(v) if v.explanation.starts_with("undecided") => undecided.push(key),
            Err(v) => violations.push(v),
        }
    }
    let locks = match cfg.workload {
        Workload::Lock { .. } => {
            let mut ops = lock_ops.into_inner().unwrap();
            ops.sort_by_key(|o| o.acquired);
            let r = lockcheck::check(&ops);
            Some((ops, r))
        }
        Workload::Register | Workload::Watch => None,
    };
    let watch = (cfg.workload == Workload::Watch).then(|| {
        let mut ws = watchers.into_inner().unwrap();
        ws.sort_by_key(|w| w.node);
        let writes = watch_writes.into_inner().unwrap();
        let r = watchcheck::check(
            &writes,
            &ws,
            first_revision,
            watch_target.load(Ordering::SeqCst),
        );
        (writes, r)
    });
    let report = Report {
        duration: cfg.time.as_nanos() as u64,
        watch,
        keys: by_key.len(),
        history,
        nemesis: nemesis_log.into_inner().unwrap(),
        violations,
        undecided,
        locks,
        dir,
    };
    crate::report::write(&report, db, cfg)?;
    Ok(report)
}

/// Inject faults until the deadline: a quiet gap, then one fault held for
/// a while, then its repair.
fn nemesis(
    db: &dyn Database,
    cluster: &Cluster,
    cfg: &Config,
    deadline: Instant,
    t0: Instant,
    log: &Mutex<Vec<(u64, String)>>,
) {
    let mut rng = Rng::new(cfg.seed ^ 0xFA17);
    let note = |what: String| {
        log.lock()
            .unwrap()
            .push((t0.elapsed().as_nanos() as u64, what))
    };
    let n = cluster.nodes.len();
    loop {
        if Instant::now() + cfg.fault_gap + cfg.fault_for >= deadline {
            return;
        }
        std::thread::sleep(cfg.fault_gap);
        let fault = cfg.faults[rng.below(cfg.faults.len() as u64) as usize];
        let random = rng.below(n as u64) as usize;
        let leader = match cfg.target {
            Target::Leader => db.leader(cluster),
            Target::Random => None,
        };
        let victim = leader.unwrap_or(random);
        let name = |i: usize| {
            if Some(i) == leader {
                format!("{} (leader)", cluster.nodes[i].name)
            } else {
                cluster.nodes[i].name.clone()
            }
        };
        let result = match fault {
            Fault::Partition => {
                let mut order: Vec<usize> = (0..n).collect();
                rng.shuffle(&mut order);
                // Half the time a majority and a minority, half the time
                // one node cut off from everyone; with a leader target, the
                // leader cut off from everyone.
                let cut = if rng.below(2) == 0 { n / 2 } else { 1 };
                let sides = match leader {
                    Some(l) => vec![vec![l], (0..n).filter(|&i| i != l).collect()],
                    None => vec![order[..cut].to_vec(), order[cut..].to_vec()],
                };
                let show =
                    |v: &Vec<usize>| v.iter().map(|&i| name(i)).collect::<Vec<_>>().join(" ");
                note(format!(
                    "partition: [{}] | [{}]",
                    show(&sides[0]),
                    show(&sides[1])
                ));
                cluster.partition(&sides).and_then(|_| {
                    std::thread::sleep(cfg.fault_for);
                    let r = cluster.heal();
                    note("heal: network restored".into());
                    r
                })
            }
            Fault::Kill => {
                note(format!("kill -9 {}", name(victim)));
                cluster.kill(victim).and_then(|_| {
                    std::thread::sleep(cfg.fault_for);
                    note(format!("restart {}", cluster.nodes[victim].name));
                    db.start(cluster, victim)
                })
            }
            Fault::Pause => {
                note(format!("pause {} (SIGSTOP)", name(victim)));
                cluster.pause(victim).and_then(|_| {
                    std::thread::sleep(cfg.fault_for);
                    note(format!("resume {} (SIGCONT)", cluster.nodes[victim].name));
                    cluster.resume(victim)
                })
            }
        };
        if let Err(e) = result {
            note(format!("nemesis error: {e}"));
        }
    }
}

/// One lock-workload client: take a lease, keep it alive in the
/// background, acquire the lock, read the counter, write it plus one,
/// release. Holding intervals and outcomes go to `out`.
#[allow(clippy::too_many_arguments)]
fn lock_client(
    db: &dyn Database,
    cluster: &Cluster,
    cfg: &Config,
    p: usize,
    deadline: Instant,
    t0: Instant,
    fenced: bool,
    stall_percent: u64,
    out: &Mutex<Vec<LockOp>>,
) {
    const TTL: u64 = 2;
    let ns = || t0.elapsed().as_nanos() as u64;
    let node = p % cfg.nodes;
    let Some(mut c) = db.lock_client(cluster, node, cfg.timeout) else {
        return;
    };
    let mut rng = Rng::new(cfg.seed ^ ((p as u64 + 7) * 0x2545_F491));
    while Instant::now() < deadline {
        let Ok(lease) = c.grant(TTL) else {
            std::thread::sleep(Duration::from_millis(100));
            continue;
        };
        let alive = AtomicBool::new(true);
        std::thread::scope(|ks| {
            // Renew the lease three times per TTL, as etcd's own clients do,
            // until told to stop.
            ks.spawn(|| {
                let Some(mut k) = db.lock_client(cluster, node, cfg.timeout) else {
                    return;
                };
                while alive.load(Ordering::Relaxed) {
                    let _ = k.keep_alive(&lease);
                    for _ in 0..(TTL * 1000 / 3 / 50) {
                        if !alive.load(Ordering::Relaxed) {
                            return;
                        }
                        std::thread::sleep(Duration::from_millis(50));
                    }
                }
            });
            let Ok(owner) = c.acquire(&lease, Duration::from_secs(4)) else {
                alive.store(false, Ordering::Relaxed);
                return;
            };
            let acquired = ns();
            let Ok(read) = c.read_counter() else {
                let _ = c.release(&owner);
                alive.store(false, Ordering::Relaxed);
                return;
            };
            let stalled = rng.below(100) < stall_percent;
            if stalled {
                // A long pause of the lock holder: its keep-alives stop and
                // the lease runs out while it still thinks it holds the lock.
                alive.store(false, Ordering::Relaxed);
                std::thread::sleep(Duration::from_secs(TTL + 1));
            }
            let wrote = match c.write_counter(read + 1, fenced.then_some(owner.as_str())) {
                Ok(()) => Outcome::Ok,
                Err(Error::Fail(_)) => Outcome::Fail,
                Err(Error::Unknown(_)) => Outcome::Info,
            };
            let released = ns();
            let _ = c.release(&owner);
            alive.store(false, Ordering::Relaxed);
            out.lock().unwrap().push(LockOp {
                process: p,
                node,
                acquired,
                released,
                read,
                wrote,
                stalled,
            });
        });
        let _ = c.revoke(&lease);
        std::thread::sleep(cfg.op_delay);
    }
}

/// The store's revision, from whichever node answers first (retrying for
/// up to half a minute while the cluster recovers).
fn current_revision(db: &dyn Database, cluster: &Cluster, cfg: &Config) -> Result<u64, String> {
    let until = Instant::now() + Duration::from_secs(30);
    loop {
        for node in 0..cfg.nodes {
            if let Some(mut c) = db.watch_client(cluster, node, cfg.timeout)
                && let Ok(r) = c.revision()
            {
                return Ok(r);
            }
        }
        if Instant::now() > until {
            return Err("no node would report the current revision".into());
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// A watch-workload writer: unique values to a handful of keys.
#[allow(clippy::too_many_arguments)]
fn watch_writer(
    db: &dyn Database,
    cluster: &Cluster,
    cfg: &Config,
    p: usize,
    deadline: Instant,
    t0: Instant,
    next_value: &AtomicU64,
    out: &Mutex<Vec<WatchWrite>>,
) {
    let Some(mut c) = db.watch_client(cluster, p % cfg.nodes, cfg.timeout) else {
        return;
    };
    let mut rng = Rng::new(cfg.seed ^ ((p as u64 + 3) * 0x9E37_79B9));
    while Instant::now() < deadline {
        let (key, value) = (rng.below(5), next_value.fetch_add(1, Ordering::Relaxed));
        let start = t0.elapsed().as_nanos() as u64;
        let result = c.put(key, value);
        let end = t0.elapsed().as_nanos() as u64;
        let (outcome, revision) = match result {
            Ok(rev) => (Outcome::Ok, Some(rev)),
            Err(Error::Fail(_)) => (Outcome::Fail, None),
            Err(Error::Unknown(_)) => (Outcome::Info, None),
        };
        out.lock().unwrap().push(WatchWrite {
            process: p,
            key,
            value,
            outcome,
            revision,
            node: p % cfg.nodes,
            start,
            end,
        });
        std::thread::sleep(cfg.op_delay);
    }
}

/// A watcher on one node: stream from just after the last revision seen,
/// reconnecting whenever the stream breaks, until it has seen `target`
/// (once known) or catching up takes a minute past the deadline.
#[allow(clippy::too_many_arguments)]
fn watcher(
    db: &dyn Database,
    cluster: &Cluster,
    cfg: &Config,
    node: usize,
    first_revision: u64,
    deadline: Instant,
    target: &AtomicU64,
    abort: &AtomicBool,
) -> Watcher {
    let mut w = Watcher {
        node,
        ..Watcher::default()
    };
    let Some(mut c) = db.watch_client(cluster, node, cfg.timeout) else {
        return w;
    };
    let last = std::cell::Cell::new(first_revision);
    let give_up = deadline + Duration::from_secs(60);
    let done = |last: u64| {
        let t = target.load(Ordering::SeqCst);
        (t != 0 && last >= t) || abort.load(Ordering::SeqCst) || Instant::now() > give_up
    };
    while !done(last.get()) {
        // The first connection starts right after the initial revision;
        // resumptions after a break use the configured offset.
        let from = if w.reconnects == 0 && w.events.is_empty() {
            last.get() + 1
        } else {
            last.get() + cfg.watch_resume
        };
        let events = &mut w.events;
        let r = c.watch(
            from,
            &mut |e| {
                last.set(last.get().max(e.revision));
                events.push(e);
                !done(last.get())
            },
            &mut || !done(last.get()),
        );
        if r.is_err() {
            w.reconnects += 1;
            std::thread::sleep(Duration::from_millis(200));
        }
    }
    w
}
