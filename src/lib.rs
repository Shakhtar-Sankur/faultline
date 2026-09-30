//! faultline: Jepsen-style fault injection for real databases. It runs a
//! real cluster on one Linux machine, one network namespace per node,
//! drives it with concurrent clients while it partitions the network,
//! kills and pauses nodes, and checks the recorded history.

pub mod checker;
pub mod cluster;
pub mod db;
pub mod history;
pub mod http;
pub mod json;
pub mod report;
pub mod rng;
pub mod test;
