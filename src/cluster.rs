//! A cluster of database nodes on one Linux machine, each in its own
//! network namespace with its own IP address, joined by a bridge, so the
//! nemesis can cut real network links between them with iptables, the
//! way Jepsen does across machines. Clients run in the root namespace and
//! reach every node through the bridge.
//!
//! Needs root (or CAP_NET_ADMIN and CAP_SYS_ADMIN) and the `ip` and
//! `iptables` commands.

use std::fs::File;
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;

const BRIDGE: &str = "fl-br";
const SUBNET: [u8; 3] = [10, 77, 0];

pub struct Node {
    pub index: usize,
    pub name: String,
    pub ip: Ipv4Addr,
    pub netns: String,
    pub dir: PathBuf,
}

pub struct Cluster {
    pub nodes: Vec<Node>,
    /// Each node's running processes.
    procs: Mutex<Vec<Vec<Child>>>,
    pub store: PathBuf,
}

fn run(cmd: &str, args: &[&str]) -> Result<String, String> {
    let out = Command::new(cmd)
        .args(args)
        .output()
        .map_err(|e| format!("{cmd}: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "{cmd} {}: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

fn netns(ns: &str, cmd: &[&str]) -> Result<String, String> {
    let mut args = vec!["netns", "exec", ns];
    args.extend_from_slice(cmd);
    run("ip", &args)
}

impl Cluster {
    /// Create `n` namespaces, `store/n1..nN` as their data directories.
    pub fn create(n: usize, store: &Path) -> Result<Cluster, String> {
        Self::destroy_leftovers();
        run("ip", &["link", "add", BRIDGE, "type", "bridge"])?;
        let host = format!("{}.{}.{}.1/24", SUBNET[0], SUBNET[1], SUBNET[2]);
        run("ip", &["addr", "add", &host, "dev", BRIDGE])?;
        run("ip", &["link", "set", BRIDGE, "up"])?;
        let mut nodes = Vec::new();
        for i in 0..n {
            let name = format!("n{}", i + 1);
            let ns = format!("fl-{name}");
            let veth = format!("fl-v{}", i + 1);
            let ip = Ipv4Addr::new(SUBNET[0], SUBNET[1], SUBNET[2], 11 + i as u8);
            run("ip", &["netns", "add", &ns])?;
            run(
                "ip",
                &[
                    "link", "add", &veth, "type", "veth", "peer", "name", "eth0", "netns", &ns,
                ],
            )?;
            run("ip", &["link", "set", &veth, "master", BRIDGE, "up"])?;
            netns(
                &ns,
                &["ip", "addr", "add", &format!("{ip}/24"), "dev", "eth0"],
            )?;
            netns(&ns, &["ip", "link", "set", "eth0", "up"])?;
            netns(&ns, &["ip", "link", "set", "lo", "up"])?;
            let dir = store.join(&name);
            std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
            nodes.push(Node {
                index: i,
                name,
                ip,
                netns: ns,
                dir,
            });
        }
        Ok(Cluster {
            procs: Mutex::new((0..n).map(|_| Vec::new()).collect()),
            nodes,
            store: store.to_path_buf(),
        })
    }

    /// Remove namespaces and the bridge a previous run left behind.
    pub fn destroy_leftovers() {
        if let Ok(list) = run("ip", &["netns", "list"]) {
            for ns in list.lines().filter_map(|l| l.split_whitespace().next()) {
                if ns.starts_with("fl-n") {
                    if let Ok(pids) = run("ip", &["netns", "pids", ns]) {
                        for pid in pids.split_whitespace() {
                            let _ = run("kill", &["-9", pid]);
                        }
                    }
                    let _ = run("ip", &["netns", "del", ns]);
                }
            }
        }
        let _ = run("ip", &["link", "del", BRIDGE]);
    }

    /// Start a process on a node: `argv` runs inside the node's namespace,
    /// with output appended to the node's `log`.
    pub fn start(&self, node: usize, log: &str, argv: &[String]) -> Result<(), String> {
        let n = &self.nodes[node];
        let log = File::options()
            .create(true)
            .append(true)
            .open(n.dir.join(log))
            .map_err(|e| e.to_string())?;
        let child = Command::new("ip")
            .args(["netns", "exec", &n.netns])
            .args(argv)
            .stdin(Stdio::null())
            .stdout(log.try_clone().map_err(|e| e.to_string())?)
            .stderr(log)
            .spawn()
            .map_err(|e| format!("start {}: {e}", n.name))?;
        self.procs.lock().unwrap()[node].push(child);
        Ok(())
    }

    /// Whether the node has processes, all still running.
    pub fn is_running(&self, node: usize) -> bool {
        let mut procs = self.procs.lock().unwrap();
        !procs[node].is_empty()
            && procs[node]
                .iter_mut()
                .all(|c| matches!(c.try_wait(), Ok(None)))
    }

    fn signal(&self, node: usize, sig: &str) -> Result<(), String> {
        let procs = self.procs.lock().unwrap();
        if procs[node].is_empty() {
            return Err(format!("{} is not running", self.nodes[node].name));
        }
        for c in &procs[node] {
            run("kill", &[sig, &c.id().to_string()])?;
        }
        Ok(())
    }

    /// Crash a node: SIGKILL every process, so none gets a chance to flush
    /// or say goodbye.
    pub fn kill(&self, node: usize) -> Result<(), String> {
        let r = self.signal(node, "-KILL");
        for mut c in self.procs.lock().unwrap()[node].drain(..) {
            let _ = c.wait();
        }
        r
    }

    /// Freeze a node's process, as a long GC pause or a stalled VM would.
    pub fn pause(&self, node: usize) -> Result<(), String> {
        self.signal(node, "-STOP")
    }

    pub fn resume(&self, node: usize) -> Result<(), String> {
        self.signal(node, "-CONT")
    }

    /// Cut the network into `sides`: nodes in different sides cannot
    /// exchange packets. Nodes not listed are cut off from everyone.
    /// Clients (on the bridge's host address) still reach every node.
    pub fn partition(&self, sides: &[Vec<usize>]) -> Result<(), String> {
        self.heal()?;
        let side_of = |i: usize| sides.iter().position(|s| s.contains(&i));
        for a in &self.nodes {
            for b in &self.nodes {
                if a.index != b.index
                    && (side_of(a.index) != side_of(b.index) || side_of(a.index).is_none())
                {
                    let peer = b.ip.to_string();
                    netns(
                        &a.netns,
                        &["iptables", "-A", "INPUT", "-s", &peer, "-j", "DROP"],
                    )?;
                    netns(
                        &a.netns,
                        &["iptables", "-A", "OUTPUT", "-d", &peer, "-j", "DROP"],
                    )?;
                }
            }
        }
        Ok(())
    }

    pub fn heal(&self) -> Result<(), String> {
        for n in &self.nodes {
            netns(&n.netns, &["iptables", "-F"])?;
        }
        Ok(())
    }

    /// Kill every process and remove the namespaces and bridge.
    pub fn destroy(&self) {
        for i in 0..self.nodes.len() {
            let _ = self.resume(i);
            let _ = self.kill(i);
        }
        Self::destroy_leftovers();
    }
}
