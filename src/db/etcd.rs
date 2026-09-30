//! etcd, through its JSON gateway to the v3 API: reads are range
//! requests, writes are puts, and compare-and-set is a transaction that
//! compares the value and puts on success.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use super::{Client, Database, Error, LockClient};
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

    fn leader(&self, cluster: &Cluster) -> Option<usize> {
        // Each node reports its own member id and the leader's; the leader
        // is the node whose two ids agree. Asking every node survives the
        // leader itself being unreachable.
        let mut leader_id = None;
        let mut ids = Vec::new();
        for (i, n) in cluster.nodes.iter().enumerate() {
            let addr = SocketAddr::new(n.ip.into(), CLIENT_PORT);
            let Ok(r) = http::post(
                addr,
                "/v3/maintenance/status",
                "{}",
                Duration::from_millis(300),
            ) else {
                continue;
            };
            let Ok(j) = json::parse(&r.body) else {
                continue;
            };
            let member = j
                .get("header")
                .and_then(|h| h.get("member_id"))
                .and_then(Json::as_str);
            if let Some(m) = member {
                ids.push((i, m.to_string()));
            }
            if let Some(l) = j.get("leader").and_then(Json::as_str).filter(|l| *l != "0") {
                leader_id = Some(l.to_string());
            }
        }
        let leader_id = leader_id?;
        ids.into_iter()
            .find(|(_, m)| *m == leader_id)
            .map(|(i, _)| i)
    }

    fn lock_client(
        &self,
        cluster: &Cluster,
        node: usize,
        timeout: Duration,
    ) -> Option<Box<dyn LockClient>> {
        Some(Box::new(EtcdClient {
            addr: SocketAddr::new(cluster.nodes[node].ip.into(), CLIENT_PORT),
            timeout,
            serializable_reads: false,
        }))
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

const LOCK_NAME: &str = "faultline-lock";
const COUNTER: &str = "faultline-counter";

fn str_field<'a>(j: &'a Json, key: &str) -> Option<&'a str> {
    j.get(key).and_then(Json::as_str)
}

/// etcd's lock service (the gateway to its `concurrency.Mutex`): the lock
/// is held by whoever owns the oldest key under the lock's name, and each
/// key is attached to its owner's lease, so it vanishes when the lease
/// expires, whether or not its owner has noticed.
impl LockClient for EtcdClient {
    fn grant(&mut self, ttl: u64) -> Result<String, Error> {
        let r = self.request("/v3/lease/grant", &format!(r#"{{"TTL":"{ttl}"}}"#), false)?;
        str_field(&r, "ID")
            .map(String::from)
            .ok_or_else(|| Error::Unknown("grant: no lease ID".into()))
    }

    fn keep_alive(&mut self, lease: &str) -> Result<(), Error> {
        let r = self.request(
            "/v3/lease/keepalive",
            &format!(r#"{{"ID":"{lease}"}}"#),
            true,
        )?;
        let ttl = r
            .get("result")
            .and_then(|x| str_field(x, "TTL"))
            .unwrap_or("0");
        if ttl == "0" {
            return Err(Error::Fail("lease expired".into()));
        }
        Ok(())
    }

    fn acquire(&mut self, lease: &str, wait: Duration) -> Result<String, Error> {
        let saved = self.timeout;
        self.timeout = wait;
        let body = format!(
            r#"{{"name":"{}","lease":"{lease}"}}"#,
            json::base64(LOCK_NAME.as_bytes())
        );
        let r = self.request("/v3/lock/lock", &body, false);
        self.timeout = saved;
        str_field(&r?, "key")
            .map(String::from)
            .ok_or_else(|| Error::Unknown("lock: no key".into()))
    }

    fn read_counter(&mut self) -> Result<u64, Error> {
        let body = format!(r#"{{"key":"{}"}}"#, json::base64(COUNTER.as_bytes()));
        let r = self.request("/v3/kv/range", &body, true)?;
        Ok(r.get("kvs")
            .and_then(|kvs| kvs.as_arr().first())
            .and_then(|kv| str_field(kv, "value"))
            .and_then(json::unbase64)
            .and_then(|b| String::from_utf8(b).ok())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0))
    }

    fn write_counter(&mut self, value: u64, fence: Option<&str>) -> Result<(), Error> {
        let (k, v) = (json::base64(COUNTER.as_bytes()), val_b64(value));
        let Some(owner) = fence else {
            self.request(
                "/v3/kv/put",
                &format!(r#"{{"key":"{k}","value":"{v}"}}"#),
                false,
            )?;
            return Ok(());
        };
        // Put only if our ownership key still exists: etcd's documented way
        // to act safely under a lock.
        let body = format!(
            r#"{{"compare":[{{"key":"{owner}","target":"CREATE","result":"GREATER","create_revision":"0"}}],"success":[{{"requestPut":{{"key":"{k}","value":"{v}"}}}}]}}"#
        );
        let r = self.request("/v3/kv/txn", &body, false)?;
        if r.get("succeeded").and_then(Json::as_bool) == Some(true) {
            Ok(())
        } else {
            Err(Error::Fail("fenced out: the lock was lost".into()))
        }
    }

    fn release(&mut self, owner_key: &str) -> Result<(), Error> {
        self.request(
            "/v3/lock/unlock",
            &format!(r#"{{"key":"{owner_key}"}}"#),
            false,
        )
        .map(|_| ())
    }

    fn revoke(&mut self, lease: &str) -> Result<(), Error> {
        self.request("/v3/lease/revoke", &format!(r#"{{"ID":"{lease}"}}"#), false)
            .map(|_| ())
    }
}
