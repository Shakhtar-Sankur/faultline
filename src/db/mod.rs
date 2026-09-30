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
