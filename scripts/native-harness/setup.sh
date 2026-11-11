#!/usr/bin/env bash
# Builds the native test harness used for the reference results: three
# RabbitMQ nodes in separate Linux network namespaces joined by a bridge,
# reproducing the docker-compose topology (hosts rabbit1..3, same ports,
# same rabbitmq.conf). Requires root, iproute2 and rabbitmq-server (apt).
set -e
B=${B:-/home/claude/native}
sudo ip link add br-rmq type bridge; sudo ip addr add 10.10.0.1/24 dev br-rmq; sudo ip link set br-rmq up
sudo iptables -I FORWARD -i br-rmq -o br-rmq -j ACCEPT
for i in 1 2 3; do
  sudo ip netns add rmq$i
  sudo ip link add veth$i type veth peer name eth0 netns rmq$i
  sudo ip link set veth$i master br-rmq up
  sudo ip netns exec rmq$i ip addr add 10.10.0.1$i/24 dev eth0
  sudo ip netns exec rmq$i ip link set eth0 up
  sudo ip netns exec rmq$i ip link set lo up
  grep -q " rabbit$i\$" /etc/hosts || echo "10.10.0.1$i rabbit$i" | sudo tee -a /etc/hosts
  mkdir -p $B/n$i
  # RabbitMQ 3.12 (Ubuntu) lacks default_queue_type; clients declare quorum explicitly
  grep -v default_queue_type rabbitmq-cluster/rabbitmq/rabbitmq.conf > $B/n$i/rabbitmq.conf
  echo "[rabbitmq_management,rabbitmq_prometheus]." > $B/n$i/enabled_plugins
done
sudo chown -R rabbitmq:rabbitmq $B
for i in 1 2 3; do scripts/native-harness/ctl.sh start $i; sleep 15; done
