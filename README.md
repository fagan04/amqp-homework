# INFT 6000 HW2 — Distributed AMQP Messaging Cluster

A 3-node **RabbitMQ 4.1** cluster with **Raft-replicated quorum queues**, **Prometheus + Grafana**
monitoring, resilient **Go** producer/consumer test clients, and an automated **chaos + benchmark** suite.

| Deliverable (assignment §2) | Where |
|---|---|
| 1. Infrastructure & configuration scripts | [`rabbitmq-cluster/`](rabbitmq-cluster) (docker-compose, rabbitmq.conf, init job, Prometheus, alerts, Grafana) |
| 2. Architecture design document & C4 diagrams | [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md), [`docs/c4/`](docs/c4) (PlantUML sources + PNG/SVG) |
| 3. Producer & consumer test suite | [`producer/`](producer), [`consumer/`](consumer), [`internal/`](internal), [`cmd/clustermon`](cmd/clustermon) |
| 4. Resilience & benchmark report | [`docs/REPORT.md`](docs/REPORT.md), raw data in [`results/`](results), scripts in [`scripts/`](scripts) |

## Repository layout

```
rabbitmq-cluster/
  docker-compose.yml        3 RabbitMQ nodes + rabbit-init + Prometheus + Grafana
  rabbitmq/rabbitmq.conf    clustering (classic_config), pause_minority, quorum default
  init/init.sh              one-shot: wait for 3 nodes, apply queue policy (delivery-limit + DLQ)
  prometheus/               scrape config (aggregated + per-object) and 6 alert rules
  grafana/                  provisioned datasource + "RabbitMQ Cluster - HW2" dashboard (17 panels)
producer/                   multi-worker publisher: confirms, failover, retry, stress flags
consumer/                   validating consumer: manual ack, failover + switch-back, dedup, loss/order check
internal/amqpx              shared failover dialer, topology, structured logging
internal/stats              latency percentiles, CSV time series
cmd/clustermon              250 ms poller of quorum-queue leader / node availability
scripts/chaos.sh            7 failure scenarios (node crash, leader crash, partition, quorum loss, ...)
scripts/bench.sh            throughput / message size / prefetch / latency-under-load benchmarks
scripts/plot_results.py     turns results/ into the charts used in the report
scripts/make_report.py      generates docs/REPORT.md (data appendix) from results/
docs/                       architecture document, report, C4 diagrams, charts
results/                    JSON summaries + CSV time series from every run
```

## Quick start

Requirements: Docker + Docker Compose v2, Go ≥ 1.22, Python 3 (only for the scripts' JSON parsing / charts).

```bash
# 1. Create the shared Erlang cookie (once). .env is git-ignored.
cd rabbitmq-cluster
echo "ERLANG_COOKIE=$(openssl rand -hex 32)" > .env && chmod 600 .env

# 2. Start everything (rabbit1 -> rabbit2 -> rabbit3 -> rabbit-init, Prometheus, Grafana)
docker compose up -d
docker compose logs rabbit-init        # should end with "[init] done"
docker exec rabbit1 rabbitmqctl cluster_status

# 3. Build the clients
cd .. && make build                     # or: go build -o bin/producer ./producer && ...

# 4. Send and receive 40,000 validated messages
./bin/consumer -run-id demo -expect 40000 -expect-per-producer 10000 -summary /tmp/c.json &
./bin/producer -run-id demo -workers 4 -count 10000 -rate 2000
```

| Endpoint | URL | Credentials |
|---|---|---|
| AMQP rabbit1 / rabbit2 / rabbit3 | `localhost:5672` / `5673` / `5674` | admin / admin |
| Management UI | http://localhost:15672 (also 15673, 15674) | admin / admin |
| Prometheus (targets, alerts) | http://localhost:9090 | – |
| Grafana dashboard | http://localhost:3000 | admin / admin |

## Running the tests

```bash
scripts/chaos.sh baseline          # or node-crash | leader-crash | partition | quorum-loss | rolling-restart | full-restart
scripts/chaos.sh all               # ~10 minutes, writes results/<scenario>/
scripts/bench.sh all               # ~5 minutes, writes results/bench/
python3 scripts/plot_results.py    # regenerates docs/charts/*.png
python3 scripts/make_report.py     # regenerates docs/REPORT.md from results/
```

Each chaos scenario prints a verdict such as:

```
producer: confirmed 128000/128000  republished=100 reconnects=1
consumer: unique=128000 duplicates=93 missing=0 out_of_order=0 failovers=1 switch_backs=1
VERDICT zero_loss=True
```

## Client flags (most useful)

| Producer | Default | Meaning |
|---|---|---|
| `-workers` | 4 | concurrent publishers (1 connection + channel each) |
| `-count` | 10000 | messages per worker |
| `-rate` | 0 | total msg/s (0 = as fast as possible) |
| `-inflight` | 100 | unconfirmed messages per worker (1 = synchronous confirms) |
| `-size` | 512 | body size in bytes |
| `-nodes` | localhost:5672,5673,5674 | failover list |

| Consumer | Default | Meaning |
|---|---|---|
| `-nodes` | 5673,5674,5672 | preference order (first = preferred, switch-back target) |
| `-prefetch` | 50 | QoS window |
| `-run-id`, `-expect`, `-expect-per-producer` | – | enable loss / ordering validation and exit condition |
| `-work-ms` | 0 | simulated processing time |

Both write JSON logs to stderr, and `-stats-csv` / `-summary` files.

## Team

See commit history. Original cluster + monitoring: Murphy052; original clients: fagan; HW2 completion
(client resilience rewrite, chaos/benchmark suite, docs): Aysu.
