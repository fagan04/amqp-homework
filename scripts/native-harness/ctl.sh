#!/bin/bash
# Native test-harness equivalent of the docker-compose cluster.
# Each node runs in its own network namespace (rmqN, IP 10.10.0.1N, host rabbitN).
# usage: ctl.sh start|stop|kill|partition|heal N
B=${B:-$HOME/rmq-harness}
n=$2
case $1 in
 start)
  sudo ip netns exec rmq$n env RABBITMQ_NODENAME=rabbit@rabbit$n \
    RABBITMQ_CONFIG_FILE=$B/n$n/rabbitmq.conf RABBITMQ_ENABLED_PLUGINS_FILE=$B/n$n/enabled_plugins \
    RABBITMQ_MNESIA_BASE=$B/n$n/data RABBITMQ_LOG_BASE=$B/n$n/log \
    nohup rabbitmq-server > $B/n$n/stdout.log 2>&1 &
  ;;
 stop) sudo rabbitmqctl -n rabbit@rabbit$n stop ;;
 kill) for p in $(sudo ip netns pids rmq$n); do sudo kill -9 $p 2>/dev/null; done ;;
 partition) sudo ip link set veth$n down ;;
 heal) sudo ip link set veth$n up ;;
esac
