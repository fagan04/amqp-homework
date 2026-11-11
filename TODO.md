# Task status (HW2)

## 1. Producer and Consumer
- [x] Connection handling and automatic reconnection (failover dialer, backoff, 5 s write deadline)
- [x] Switch back to Node 2 when it becomes available (probe + graceful drain)
- [x] Handle duplicate message delivery safely (idempotent consumer)
- [x] Publisher confirms and manual acknowledgments verified under failure conditions
- [x] Multi-worker stress testing, structured logging, ordering/persistence validation

## 2. Cluster and Management
- [x] Three-node RabbitMQ cluster configuration
- [x] Quorum queue replication and availability
- [x] RabbitMQ Management UI
- [x] Prometheus (+ alert rules) and Grafana (classic-format provisioned dashboard, all queries validated)
- [x] rabbit-init job: queue policy (delivery-limit 5 + DLQ)

## 3. Architecture Documentation
- [x] System Context, Container, Component (broker + clients), Deployment diagrams (docs/c4)
- [x] Design document (docs/ARCHITECTURE.md)

## 4. Testing and Benchmarking
- [x] Normal publishing and consumption (baseline)
- [x] Node 2 failure, consumer failover to Node 3, recovery + switch-back
- [x] Leader crash / Raft leader election
- [x] Network partition (follower and leader)
- [x] Quorum loss (2 of 3 nodes), rolling restart, full-cluster crash persistence
- [x] Throughput, message size, prefetch, latency-under-load benchmarks

## 5. Final Deliverables
- [x] docs/REPORT.md (resilience & benchmark report), results/ raw data, docs/charts
- [ ] Re-run `scripts/chaos.sh all` + `scripts/bench.sh all` on the Docker setup (MODE=docker) and attach Grafana screenshots
- [ ] Final presentation
