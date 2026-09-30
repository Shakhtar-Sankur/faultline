//! etcd, through its JSON gateway to the v3 API: reads are range
//! requests, writes are puts, and compare-and-set is a transaction that
//! compares the value and puts on success.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use super::{Client, Database, Error};
use crate::cluster::Cluster;
use crate::history::Call;
use crate::http::{self, HttpError};
use crate::json::{self, Json};

pub struct Etcd {
    pub binary: PathBuf,
    /// Serve reads with `serializable: true`: from the local node without
    /// consulting a quorum. Faster, and documented as possibly stale, so a
    /// correct checker must flag it under partitions. The planted bug.
    pub serializable_reads: bool,
}

const CLIENT_PORT: u16 = 2379;
const PEER_PORT: u16 = 2380;

impl Database for Etcd {
    fn name(&self) -> String {
        if self.serializable_reads {
            "etcd (serializable reads)".into()
        } else {
            "etcd".into()
        }
    }

    fn start(&self, cluster: &Cluster, node: usize) -> Result<(), String> {
        let n = &cluster.nodes[node];
        let initial: Vec<String> = cluster
            .nodes
            .iter()
            .map(|m| format!("{}=http://{}:{PEER_PORT}", m.name, m.ip))
            .collect();
        let argv = vec![
            self.binary.display().to_string(),
            "--name".into(),
            n.name.clone(),
            "--data-dir".into(),
            n.dir.join("data").display().to_string(),
            "--listen-peer-urls".into(),
            format!("http://{}:{PEER_PORT}", n.ip),
            "--initial-advertise-peer-urls".into(),
            format!("http://{}:{PEER_PORT}", n.ip),
            "--listen-client-urls".into(),
            format!("http://{}:{CLIENT_PORT}", n.ip),
            "--advertise-client-urls".into(),
            format!("http://{}:{CLIENT_PORT}", n.ip),
            "--initial-cluster".into(),
            initial.join(","),
            "--initial-cluster-state".into(),
            "new".into(),
            "--log-level".into(),
            "warn".into(),
        ];
        cluster.start(node, "etcd.log", &argv)
    }

    fn ready(&self, cluster: &Cluster, node: usize) -> bool {
        let addr = SocketAddr::new(cluster.nodes[node].ip.into(), CLIENT_PORT);
        // A linearizable read succeeds only once there is a leader.
        http::post(
            addr,
            "/v3/kv/range",
            r#"{"key":"AA=="}"#,
            Duration::from_millis(500),
        )
        .is_ok_and(|r| r.status == 200)
    }

    fn client(&self, cluster: &Cluster, node: usize, timeout: Duration) -> Box<dyn Client> {
        Box::new(EtcdClient {
            addr: SocketAddr::new(cluster.nodes[node].ip.into(), CLIENT_PORT),
            timeout,
            serializable_reads: self.serializable_reads,
        })
    }
}

struct EtcdClient {
    addr: SocketAddr,
    timeout: Duration,
    serializable_reads: bool,
}

fn key_b64(key: u64) -> String {
    json::base64(format!("r{key}").as_bytes())
}

fn val_b64(v: u64) -> String {
    json::base64(v.to_string().as_bytes())
}

impl EtcdClient {
    /// POST and parse, classifying failures. Reads are idempotent, so any
    /// failure of theirs counts as definite.
    fn request(&self, path: &str, body: &str, is_read: bool) -> Result<Json, Error> {
        let unknown = |m: String| {
            if is_read {
                Error::Fail(m)
            } else {
                Error::Unknown(m)
            }
        };
        match http::post(self.addr, path, body, self.timeout) {
            Ok(r) if r.status == 200 => {
                json::parse(&r.body).map_err(|e| unknown(format!("bad JSON: {e}")))
            }
            Ok(r) => {
                let msg = json::parse(&r.body)
                    .ok()
                    .and_then(|j| {
                        j.get("message")
                            .or(j.get("error"))
                            .and_then(Json::as_str)
                            .map(String::from)
                    })
                    .unwrap_or_else(|| format!("HTTP {}", r.status));
                Err(unknown(msg))
            }
            Err(HttpError::NotSent(m)) => Err(Error::Fail(m)),
            Err(HttpError::Unknown(m)) => Err(unknown(m)),
        }
    }
}

impl Client for EtcdClient {
    fn invoke(&mut self, key: u64, call: &Call) -> Result<Call, Error> {
        let k = key_b64(key);
        match call {
            Call::Read(_) => {
                let body = if self.serializable_reads {
                    format!(r#"{{"key":"{k}","serializable":true}}"#)
                } else {
                    format!(r#"{{"key":"{k}"}}"#)
                };
                let r = self.request("/v3/kv/range", &body, true)?;
                let value = r
                    .get("kvs")
                    .and_then(|kvs| kvs.as_arr().first())
                    .and_then(|kv| kv.get("value"))
                    .and_then(Json::as_str)
                    .and_then(json::unbase64)
                    .and_then(|b| String::from_utf8(b).ok())
                    .and_then(|s| s.parse().ok());
                Ok(Call::Read(value))
            }
            Call::Write(v) => {
                self.request(
                    "/v3/kv/put",
                    &format!(r#"{{"key":"{k}","value":"{}"}}"#, val_b64(*v)),
                    false,
                )?;
                Ok(call.clone())
            }
            Call::Cas(a, b) => {
                let body = format!(
                    r#"{{"compare":[{{"key":"{k}","target":"VALUE","result":"EQUAL","value":"{}"}}],"success":[{{"requestPut":{{"key":"{k}","value":"{}"}}}}]}}"#,
                    val_b64(*a),
                    val_b64(*b)
                );
                let r = self.request("/v3/kv/txn", &body, false)?;
                if r.get("succeeded").and_then(Json::as_bool) == Some(true) {
                    Ok(call.clone())
                } else {
                    Err(Error::Fail("compare failed".into()))
                }
            }
            Call::Add(_) | Call::ReadSet(_) => {
                Err(Error::Fail("etcd tests use the register workload".into()))
            }
        }
    }
}
