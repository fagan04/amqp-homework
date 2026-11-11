#!/usr/bin/env bash
# Shared helpers for the chaos and benchmark scripts.
# Works with bash 3.2 (macOS default) and Linux.
#
# MODE=docker (default): the docker-compose cluster in rabbitmq-cluster/
# MODE=native          : the network-namespace test harness used to produce
#                        the reference results (see docs/TEST-HARNESS.md)

set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODE="${MODE:-docker}"
BIN="$ROOT/bin"
COMPOSE_NET="${COMPOSE_NET:-rabbitmq-cluster_rabbitnet}"
NATIVE_CTL="${NATIVE_CTL:-$ROOT/scripts/native-harness/ctl.sh}"

if [ "$MODE" = "docker" ]; then
  NODE_URLS="amqp://admin:admin@localhost:5672/,amqp://admin:admin@localhost:5673/,amqp://admin:admin@localhost:5674/"
  MGMT_URLS="http://localhost:15672,http://localhost:15673,http://localhost:15674"
else
  NODE_URLS="amqp://admin:admin@rabbit1:5672/,amqp://admin:admin@rabbit2:5672/,amqp://admin:admin@rabbit3:5672/"
  MGMT_URLS="http://rabbit1:15672,http://rabbit2:15672,http://rabbit3:15672"
fi

# node_url N -> AMQP URL of node N (1..3)
node_url() { echo "$NODE_URLS" | cut -d, -f"$1"; }
# urls_in_order "2 3 1" -> comma list in that preference order
urls_in_order() {
  local out="" n
  for n in $1; do out="$out${out:+,}$(node_url "$n")"; done
  echo "$out"
}

now_ms() { python3 -c 'import time; print(int(time.time()*1000))'; }

EVENTS=""
event() { # event "<type>" "<detail>"
  local t; t=$(now_ms)
  echo "$t,$1,$2" >> "$EVENTS"
  echo ">>> [$(date +%H:%M:%S)] $1 $2"
}

kill_node() {   # hard crash (SIGKILL), no graceful shutdown
  if [ "$MODE" = "docker" ]; then docker kill "rabbit$1" >/dev/null; else "$NATIVE_CTL" kill "$1"; fi
}
start_node() {
  if [ "$MODE" = "docker" ]; then docker start "rabbit$1" >/dev/null; else "$NATIVE_CTL" start "$1"; fi
}
partition_node() {  # isolate node N from the rest of the cluster (and clients)
  if [ "$MODE" = "docker" ]; then docker network disconnect "$COMPOSE_NET" "rabbit$1"; else "$NATIVE_CTL" partition "$1"; fi
}
heal_node() {
  if [ "$MODE" = "docker" ]; then docker network connect "$COMPOSE_NET" "rabbit$1"; else "$NATIVE_CTL" heal "$1"; fi
}
ctl() { # ctl <node> <rabbitmqctl args...>
  local n=$1; shift
  if [ "$MODE" = "docker" ]; then docker exec "rabbit$n" rabbitmqctl "$@"; else sudo rabbitmqctl -n "rabbit@rabbit$n" "$@"; fi
}

# current quorum-queue leader (1..3) via any reachable management API
leader_node() {
  local u
  for u in $(echo "$MGMT_URLS" | tr ',' ' '); do
    l=$(curl -s -m 2 -u admin:admin "$u/api/queues/%2F/homework" | python3 -c 'import json,sys
try: print(json.load(sys.stdin)["leader"].split("@rabbit")[1])
except Exception: pass' 2>/dev/null)
    if [ -n "$l" ]; then echo "$l"; return 0; fi
  done
  return 1
}

wait_cluster_healthy() { # wait until all 3 nodes answer and queue has 3 online members
  local i
  for i in $(seq 1 120); do
    on=$(curl -s -m 2 -u admin:admin "$(echo "$MGMT_URLS" | cut -d, -f1)/api/queues/%2F/homework" | python3 -c 'import json,sys
try: print(len(json.load(sys.stdin)["online"]))
except Exception: print(0)' 2>/dev/null)
    [ "$on" = "3" ] && return 0
    sleep 2
  done
  echo "cluster did not become healthy" >&2; return 1
}

# reset_queue N: delete and re-create the quorum queue through node N's
# management API. With the default client-local leader locator the Raft
# leader is placed on node N, which makes every scenario start from the same,
# known leader placement.
reset_queue() {
  local n=$1 u; u=$(echo "$MGMT_URLS" | cut -d, -f"$n")
  # Never delete/re-create the queue while a member is down: a member that
  # misses the delete keeps its old Raft log and can later win an election
  # with that stale data (observed in testing).
  wait_cluster_healthy || { echo "refusing to reset queue: cluster not healthy" >&2; return 1; }
  curl -s -m 10 -u admin:admin -X DELETE "$u/api/queues/%2F/homework" >/dev/null || true
  curl -s -m 10 -u admin:admin -X PUT -H 'content-type: application/json' \
    -d '{"durable":true,"auto_delete":false,"arguments":{"x-queue-type":"quorum"}}' \
    "$u/api/queues/%2F/homework" >/dev/null
  wait_cluster_healthy
  echo "    queue reset, leader=rabbit$(leader_node)"
}

purge_queue() {
  curl -s -m 5 -u admin:admin -X DELETE "$(echo "$MGMT_URLS" | cut -d, -f1)/api/queues/%2F/homework/contents" >/dev/null || true
}

build_tools() {
  ( cd "$ROOT" && mkdir -p bin && for c in producer consumer cmd/clustermon; do
      go build -o "bin/$(basename $c)" "./$c" || exit 1; done )
}
