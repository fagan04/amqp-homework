# Remaining Tasks

## 1. Producer and Consumer
- [x] Improve connection handling and automatic reconnection.
- [x] Implement switching back to Node 2 when it becomes available.
- [x] Handle duplicate message delivery safely.
- [x] Test publisher confirms and manual acknowledgments under failure conditions.

## 2. Cluster and Management
- [x] Verify three-node RabbitMQ cluster configuration.
- [x] Verify Quorum Queue replication and availability.
- [x] Verify RabbitMQ Management UI.
- [ ] Verify Prometheus and Grafana monitoring.

## 3. Architecture Documentation
- [ ] System Context diagram.
- [ ] Container diagram.
- [ ] Component diagram.
- [ ] Deployment diagram.

## 4. Testing and Benchmarking
- [ ] Test normal message publishing and consumption.
- [ ] Test Node 2 failure and consumer failover to Node 3.
- [ ] Test Node 2 recovery.
- [ ] Test network partition behavior.
- [ ] Measure message throughput.
- [ ] Measure message latency.

## 5. Final Deliverables
- [ ] Document architecture and implementation.
- [ ] Document resilience and benchmark results.
- [ ] Prepare final report.
- [ ] Organize repository for submission.