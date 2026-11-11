# Test environments

The scripts run against two interchangeable environments, selected by `MODE`.

| | `MODE=docker` (default, submission target) | `MODE=native` (reference results) |
|---|---|---|
| Nodes | 3 containers, `rabbitmq:4.1-management` | 3 Linux network namespaces, RabbitMQ 3.12.1 / Erlang 25 (Ubuntu 24.04 package) |
| Node names | `rabbit@rabbit1..3` | `rabbit@rabbit1..3` |
| Network | Docker bridge `rabbitnet` | Linux bridge `br-rmq`, one veth per node (10.10.0.11–13) |
| Config | `rabbitmq-cluster/rabbitmq/rabbitmq.conf` | same file, minus `default_queue_type` (a 3.13+ key) |
| AMQP from host | `localhost:5672 / 5673 / 5674` | `rabbit1..3:5672` |
| Crash | `docker kill rabbitN` (SIGKILL) | SIGKILL of every process in the namespace |
| Partition | `docker network disconnect` | `ip link set vethN down` (silent packet drop) |
| Monitoring | Prometheus + Grafana containers | Prometheus 3.5 binary with the same `prometheus.yml` / `alerts.yml` |

**Why there are two.** The cloud workspace used to produce the reference results cannot pull
images from Docker Hub. The native harness reproduces the same topology, node names, ports and
broker configuration, and its partitions behave like `docker network disconnect` (silent drops
rather than resets). Quorum-queue and Raft behaviour is the same in RabbitMQ 3.12 and 4.1.
Absolute throughput numbers will differ between machines.

To regenerate every result on your own Docker host:

```bash
make up && make build
scripts/chaos.sh all && scripts/bench.sh all && python3 scripts/plot_results.py
```

Hardware of the reference run: one VM with **2 vCPU (Intel Xeon @ 2.1 GHz) and 8 GB RAM**, shared by
all three brokers, Prometheus and all clients. Numbers are therefore conservative and relative.
