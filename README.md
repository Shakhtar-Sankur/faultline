# faultline

[![ci](https://github.com/Shakhtar-Sankur/faultline/actions/workflows/ci.yml/badge.svg)](https://github.com/Shakhtar-Sankur/faultline/actions/workflows/ci.yml)

Jepsen-style fault injection for real databases, in Rust, with zero
dependencies.

faultline runs a real cluster on one Linux machine, each node in its own
network namespace with its own IP address, and drives it with concurrent
clients while it cuts the network with iptables, crashes nodes with
`kill -9`, and freezes them with `SIGSTOP`. It records every operation (when
it started, when it ended, and whether it succeeded, failed, or timed out
with an unknown outcome) and then checks the history for
**linearizability**, the strongest single-object consistency guarantee.

The target today is **etcd**, the consensus store behind Kubernetes.

## Does it work? It catches stale reads, and passes correct ones

A checker that never fails proves nothing, so the first test is one it
must fail. etcd offers *serializable* reads, served by any node from its
local copy without consulting a quorum: faster, and documented as possibly
stale. Under network partitions faultline catches them:

```
$ sudo faultline test etcd --etcd ./etcd --time 60 --faults partition --serializable-reads
etcd (serializable reads) on 3 nodes, 10 clients, 60s, faults [Partition], seed 7

  op          ok    fail  unknown   latency of ok (p50 / p99)
  read     19019       0        0   1.0 / 2.9 ms
  write     9483       0       62   2.3 / 6.1 ms
  cas       1796    7652       66   2.5 / 6.6 ms

  faults injected:
       3.003s  partition: [n1] | [n3 n2]
       9.115s  heal: network restored
  ...
  INVALID: 47 of 254 keys' histories are not linearizable
```

and for each key it shows why: the longest valid order of operations it
could find, and the reads that no order can explain. From a 30-second run
(seed 11):

```
  key 16: no order of these 118 operations is a valid register history. The longest valid order found places 112 of them; its last steps:
    ...
    ok       3.037s..3.039s    p2   n3  key 16   ok    write 3   => register 3
    ok       3.038s..3.041s    p9   n1  key 16   ok    read 3   => register 3
    ok       3.038s..3.042s    p5   n3  key 16   ok    cas 3 -> 0   => register 0
    ok       3.043s..3.044s    p8   n3  key 16   ok    read 0   => register 0
  then none of these can come next:
    stuck    3.045s..3.046s    p4   n2  key 16   ok    read 4
    stuck    3.047s..3.048s    p1   n2  key 16   ok    read 4
```

Node 2 kept answering 4 after the register had moved on to 3 and then 0.

The same test with etcd's default, linearizable reads, under partitions,
crashes and pauses together, passes:

```
$ sudo faultline test etcd --etcd ./etcd --time 120 --faults partition,kill,pause --fault-gap 3 --fault-for 6 --seed 7
etcd on 3 nodes, 10 clients, 120s, faults [Partition, Kill, Pause], seed 7

  op          ok    fail  unknown   latency of ok (p50 / p99)
  read     34110    3076        0   2.0 / 6.6 ms
  write    17048    1500       60   2.5 / 6.9 ms
  cas       3261   15280       61   2.7 / 7.8 ms

  faults injected:
       3.002s  partition: [n1] | [n3 n2]
       9.104s  heal: network restored
      12.104s  kill -9 n3
      18.112s  restart n3
      21.113s  pause n2 (SIGSTOP)
      ...    (25 fault events in all)

  VALID: all 496 keys' histories are linearizable
```

(A failed `cas` is a compare-and-set whose comparison did not match: a
definite no, which the checker knows had no effect.)

CI runs both directions on every push, against a real etcd release.

## How it works

- **Nodes in network namespaces.** Each node gets a namespace, a veth pair
  onto a shared bridge, and its own address (10.77.0.11, .12, ...), so
  partitions are real dropped packets between real addresses, not a
  simulation. Clients run in the host namespace and reach every node.
- **Faults.** A partition splits the nodes into a majority and a minority,
  or cuts one node off, with iptables rules inside the namespaces. A crash
  is `SIGKILL` and a restart from the node's data directory. A pause is
  `SIGSTOP`/`SIGCONT`, like a long GC pause or a stalled VM. The schedule
  is seeded, so a run can be repeated.
- **Honest outcomes.** A request that never reached the server (connection
  refused) definitely failed. One that timed out after it was sent may or
  may not have happened: it is recorded as *unknown*, and the checker lets
  it take effect at any time after it began, or never. Getting this wrong
  is how testers report bugs that are not there, or miss ones that are.
- **The checker.** Wing and Gong's linearizability search with Lowe's
  memoization, the algorithm behind Knossos and Porcupine, for a register
  with reads, writes and compare-and-set. Clients move to a fresh key every
  150 operations, and keys are checked independently (linearizability is
  compositional), which keeps every search exhaustive.
- **Everything saved.** Each run leaves `history.jsonl` (every operation),
  `nemesis.txt` (every fault, timestamped), `results.txt`, and each node's
  logs under `store/`.

## Run it

Linux and root (namespaces and iptables need them), `ip` and `iptables`,
and an etcd release binary:

```
curl -L https://github.com/etcd-io/etcd/releases/download/v3.5.17/etcd-v3.5.17-linux-amd64.tar.gz | tar xz
cargo build --release
sudo ./target/release/faultline test etcd --etcd etcd-v3.5.17-linux-amd64/etcd --time 60
sudo ./target/release/faultline cleanup   # only if a run was interrupted
```

Exit status: 0 valid, 1 invalid, 2 undecided, 3 error. Options: `faultline`
with no arguments lists them.

## Honest limits

- One database so far. Faults are partitions, crashes and pauses; clock
  skew, disk faults and membership changes are not yet modeled.
- The workload is a single-key register. Multi-key transactions, watches
  and leases are not yet checked.
- Every node shares one machine, so timing differs from a real
  multi-machine deployment; it finds ordering bugs, not performance ones.

## License

Apache-2.0. See [LICENSE](LICENSE).
