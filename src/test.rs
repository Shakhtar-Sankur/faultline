//! Run a test: start the cluster, let concurrent clients operate on it
//! while the nemesis injects faults, then heal everything and check the
//! history.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::checker::linearizable::{self, Violation};
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
    pub dir: PathBuf,
}

impl Report {
    pub fn valid(&self) -> Option<bool> {
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

    std::thread::scope(|s| {
        for p in 0..cfg.clients {
            let (history, next_op, cluster) = (&history, &next_op, &cluster);
            s.spawn(move || {
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
            });
        }

        if !cfg.faults.is_empty() {
            let (cluster, log) = (&cluster, &nemesis_log);
            s.spawn(move || nemesis(db, cluster, cfg, deadline, t0, log));
        }
    });

    // Heal whatever is left and let the cluster settle before tearing down.
    cluster.heal()?;
    for i in 0..cluster.nodes.len() {
        let _ = cluster.resume(i);
        if !cluster.is_running(i) {
            db.start(&cluster, i)?;
        }
    }

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
    let report = Report {
        keys: by_key.len(),
        history,
        nemesis: nemesis_log.into_inner().unwrap(),
        violations,
        undecided,
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
