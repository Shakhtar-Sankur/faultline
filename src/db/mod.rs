//! The databases under test. Each knows how to start a node inside the
//! cluster and how to make a client that talks to one node.

use std::time::Duration;

use crate::cluster::Cluster;
use crate::history::Call;

pub mod etcd;

/// How an operation failed.
#[derive(Debug)]
pub enum Error {
    /// It definitely did not take effect.
    Fail(String),
    /// It may or may not have taken effect.
    Unknown(String),
}

pub trait Client: Send {
    /// Perform `call` on `key`. On success, returns the call with its
    /// result filled in (the value a read saw), or `Err(Fail)` for a
    /// compare-and-set whose comparison failed.
    fn invoke(&mut self, key: u64, call: &Call) -> Result<Call, Error>;
}

/// A lease-based distributed lock, and a counter it is meant to protect.
pub trait LockClient: Send {
    /// A lease that expires `ttl` seconds after its last renewal.
    fn grant(&mut self, ttl: u64) -> Result<String, Error>;
    /// Renew a lease; `Err` if it has already expired.
    fn keep_alive(&mut self, lease: &str) -> Result<(), Error>;
    /// Wait up to `wait` for the lock, held under `lease`. Returns the key
    /// that proves ownership while it exists.
    fn acquire(&mut self, lease: &str, wait: Duration) -> Result<String, Error>;
    fn read_counter(&mut self) -> Result<u64, Error>;
    /// Write the counter. With `fence`, only if that ownership key still
    /// exists, atomically (`Err(Fail)` if it does not).
    fn write_counter(&mut self, value: u64, fence: Option<&str>) -> Result<(), Error>;
    fn release(&mut self, owner_key: &str) -> Result<(), Error>;
    fn revoke(&mut self, lease: &str) -> Result<(), Error>;
}

/// One event a watcher received.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WatchEvent {
    pub key: u64,
    pub value: u64,
    pub revision: u64,
}

/// Writes that report their revision, and watches over the same keys.
pub trait WatchClient: Send {
    /// Write `value` to `key`; returns the revision the write created.
    fn put(&mut self, key: u64, value: u64) -> Result<u64, Error>;
    /// The store's current revision (a linearizable read).
    fn revision(&mut self) -> Result<u64, Error>;
    /// Stream events for the workload's keys from revision `from` into
    /// `sink` until the stream breaks (`Err`) or `sink` returns false.
    /// `idle` is called while no event arrives; returning false ends it.
    fn watch(
        &mut self,
        from: u64,
        sink: &mut dyn FnMut(WatchEvent) -> bool,
        idle: &mut dyn FnMut() -> bool,
    ) -> Result<(), Error>;
}

pub trait Database: Send + Sync {
    fn name(&self) -> String;
    /// Start `node`'s processes (also used to restart it after a crash).
    fn start(&self, cluster: &Cluster, node: usize) -> Result<(), String>;
    /// Whether `node` is up and serving.
    fn ready(&self, cluster: &Cluster, node: usize) -> bool;
    /// One-time setup once every node is ready (e.g., forming replication).
    fn setup(&self, _cluster: &Cluster) -> Result<(), String> {
        Ok(())
    }
    fn client(&self, cluster: &Cluster, node: usize, timeout: Duration) -> Box<dyn Client>;
    /// A client for the lock workload, if the database offers locks.
    fn lock_client(
        &self,
        _cluster: &Cluster,
        _node: usize,
        _timeout: Duration,
    ) -> Option<Box<dyn LockClient>> {
        None
    }
    /// A client for the watch workload, if the database offers watches.
    fn watch_client(
        &self,
        _cluster: &Cluster,
        _node: usize,
        _timeout: Duration,
    ) -> Option<Box<dyn WatchClient>> {
        None
    }
    /// The node that currently leads, if the database has one and some
    /// node can say which.
    fn leader(&self, _cluster: &Cluster) -> Option<usize> {
        None
    }
}

/// Start every node and wait until all are ready.
pub fn start_all(db: &dyn Database, cluster: &Cluster, wait: Duration) -> Result<(), String> {
    for i in 0..cluster.nodes.len() {
        db.start(cluster, i)?;
    }
    let deadline = std::time::Instant::now() + wait;
    for i in 0..cluster.nodes.len() {
        while !db.ready(cluster, i) {
            if std::time::Instant::now() > deadline {
                return Err(format!(
                    "{} did not become ready; see the logs in {}",
                    cluster.nodes[i].name,
                    cluster.nodes[i].dir.display()
                ));
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }
    db.setup(cluster)
}
