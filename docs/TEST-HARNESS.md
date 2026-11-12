# Test environments

The scripts run against two interchangeable environments, selected by `MODE`.

| | `MODE=docker` (default; **reference results**) | `MODE=native` (development harness) |
|---|---|---|
| Nodes | 3 containers, `rabbitmq:4.1-management` | 3 Linux network namespaces, RabbitMQ 3.12.1 / Erlang 25 (Ubuntu 24.04 package) |
| Node names | `rabbit@rabbit1..3` | `rabbit@rabbit1..3` |
| Network | Docker bridge `rabbitnet` | Linux bridge `br-rmq`, one veth per node (10.10.0.11–13) |
| Config | `rabbitmq-cluster/rabbitmq/rabbitmq.conf` | same file, minus `default_queue_type` (a 3.13+ key) |
| AMQP from host | `localhost:5672 / 5673 / 5674` | `rabbit1..3:5672` |
| Crash | `docker kill rabbitN` (SIGKILL) | SIGKILL of every process in the namespace |
| Partition | `docker network disconnect` | `ip link set vethN down` (silent packet drop) |
| Monitoring | Prometheus + Grafana containers | Prometheus 3.5 binary with the same `prometheus.yml` / `alerts.yml` |

**Reference results** (`results/`, `docs/charts/`, `docs/REPORT.md`) come from `MODE=docker` on a
MacBook Pro (Apple Silicon, 10 CPU cores) with Docker Desktop, all brokers and clients on the same machine.

The native harness reproduces the same topology in Linux network namespaces (same node names, ports
and broker configuration) for environments without Docker. It was used during development.

To regenerate every result:

```bash
make up && make build
scripts/chaos.sh all && scripts/bench.sh all
python3 scripts/plot_results.py && python3 scripts/make_report.py
```

Notes from the Docker runs:

* Mount the broker config as a **folder**, not a single file. Docker Desktop keeps a single-file bind
  mount pointing at the old file when the host file is replaced (for example by a git branch switch);
  a node that restarts after a partition then cannot read its config and fails to boot.
* Never delete and re-create a quorum queue while a member is down. A member that misses the delete
  keeps its old Raft log and can later win an election with that stale data. `reset_queue` refuses to
  run unless all three members are online.
