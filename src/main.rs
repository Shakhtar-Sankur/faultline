use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use faultline::db::etcd::Etcd;
use faultline::test::{self, Config, Fault};

const USAGE: &str = "\
usage:
  faultline test etcd --etcd PATH [options]    test an etcd cluster
  faultline cleanup                            remove namespaces a crashed run left

options:
  --nodes N             cluster size (default 3)
  --clients N           concurrent clients, spread over the nodes (default 10)
  --time S              seconds of workload (default 60)
  --faults LIST         any of partition,kill,pause, or none (default partition,kill,pause)
  --fault-gap S         quiet seconds before each fault (default 5)
  --fault-for S         seconds each fault lasts (default 5)
  --ops-per-key N       operations on a key before moving on (default 150)
  --timeout-ms N        client timeout (default 1000)
  --seed N              seed for the operation and fault schedule (default: time)
  --store DIR           where results go (default ./store)
  --serializable-reads  etcd: read without consulting a quorum (may be stale)

Needs root: nodes run in network namespaces, faults use iptables.
Exit status: 0 valid, 1 invalid, 2 undecided, 3 error.";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("test") if args.get(1).map(String::as_str) == Some("etcd") => {
            match run_etcd(&args[2..]) {
                Ok(code) => code,
                Err(e) => {
                    eprintln!("error: {e}");
                    ExitCode::from(3)
                }
            }
        }
        Some("cleanup") => {
            faultline::cluster::Cluster::destroy_leftovers();
            ExitCode::SUCCESS
        }
        _ => {
            eprintln!("{USAGE}");
            ExitCode::from(3)
        }
    }
}

fn run_etcd(args: &[String]) -> Result<ExitCode, String> {
    let seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(1);
    let mut cfg = Config {
        nodes: 3,
        clients: 10,
        time: Duration::from_secs(60),
        faults: vec![Fault::Partition, Fault::Kill, Fault::Pause],
        fault_gap: Duration::from_secs(5),
        fault_for: Duration::from_secs(5),
        ops_per_key: 150,
        timeout: Duration::from_millis(1000),
        op_delay: Duration::from_millis(10),
        seed,
        store: PathBuf::from("store"),
    };
    let (mut binary, mut serializable_reads) = (None, false);
    let mut i = 0;
    while i < args.len() {
        let flag = args[i].as_str();
        if flag == "--serializable-reads" {
            serializable_reads = true;
            i += 1;
            continue;
        }
        let value = args
            .get(i + 1)
            .ok_or_else(|| format!("{flag} needs a value\n{USAGE}"))?;
        let num = || {
            value
                .parse::<u64>()
                .map_err(|_| format!("{flag}: not a number: {value}"))
        };
        match flag {
            "--etcd" => binary = Some(PathBuf::from(value)),
            "--nodes" => cfg.nodes = num()?.clamp(1, 9) as usize,
            "--clients" => cfg.clients = num()?.max(1) as usize,
            "--time" => cfg.time = Duration::from_secs(num()?),
            "--fault-gap" => cfg.fault_gap = Duration::from_secs(num()?),
            "--fault-for" => cfg.fault_for = Duration::from_secs(num()?),
            "--ops-per-key" => cfg.ops_per_key = num()?.max(1),
            "--timeout-ms" => cfg.timeout = Duration::from_millis(num()?),
            "--seed" => cfg.seed = num()?,
            "--store" => cfg.store = PathBuf::from(value),
            "--faults" if value == "none" => cfg.faults.clear(),
            "--faults" => {
                cfg.faults = value
                    .split(',')
                    .map(|f| Fault::parse(f).ok_or_else(|| format!("unknown fault {f}")))
                    .collect::<Result<_, _>>()?
            }
            other => return Err(format!("unknown flag {other}\n{USAGE}")),
        }
        i += 2;
    }
    let binary = binary.ok_or_else(|| format!("--etcd PATH is required\n{USAGE}"))?;
    let db = Etcd {
        binary,
        serializable_reads,
    };
    let report = test::run(&db, &cfg)?;
    print!("{}", faultline::report::summary(&report, &db, &cfg));
    Ok(match report.valid() {
        Some(true) => ExitCode::SUCCESS,
        Some(false) => ExitCode::from(1),
        None => ExitCode::from(2),
    })
}
