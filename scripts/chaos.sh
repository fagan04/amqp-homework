#!/usr/bin/env bash
# Chaos / resilience test suite.
#
#   scripts/chaos.sh <scenario>|all
#
# Scenarios: baseline node-crash leader-crash partition partition-leader
#            quorum-loss rolling-restart full-restart
#
# Every scenario runs producer + consumer + clustermon concurrently, injects a
# failure on a fixed timeline and writes to results/<scenario>/:
#   producer.json / consumer.json  final summaries (zero_loss verdict etc.)
#   producer.csv / consumer.csv    per-second throughput time series
#   cluster.csv                    250 ms leader / node-availability samples
#   events.csv                     injected failures (epoch_ms,type,detail)
#   *.log                          structured JSON logs
source "$(dirname "$0")/lib.sh"

RATE="${RATE:-2000}"        # total publish rate msg/s
WORKERS="${WORKERS:-4}"

run_load() { # run_load <name> <count_per_worker> <consumer node order> <chaos function> <leader node>
  local name=$1 count=$2 order=$3 chaos=$4 leader=${5:-1}
  local out="$ROOT/results/$name"
  rm -rf "$out"; mkdir -p "$out"
  EVENTS="$out/events.csv"; echo "epoch_ms,type,detail" > "$EVENTS"
  local run="$name-$(date +%H%M%S)"
  local total=$((count * WORKERS))
  echo "=== scenario $name: $WORKERS workers x $count msgs @ ${RATE} msg/s (run $run)"
  wait_cluster_healthy || return 1
  reset_queue "$leader"
  event leader "rabbit$(leader_node)"

  "$BIN/clustermon" -mgmt "$MGMT_URLS" -csv "$out/cluster.csv" -duration 15m 2> "$out/clustermon.log" &
  local mon=$!
  "$BIN/consumer" -nodes "$(urls_in_order "$order")" -run-id "$run" -expect "$total" \
     -expect-per-producer "$count" -idle-timeout 90s -prefetch 100 \
     -stats-csv "$out/consumer.csv" -summary "$out/consumer.json" 2> "$out/consumer.log" &
  local cons=$!
  sleep 1
  event load_start "rate=$RATE workers=$WORKERS count=$count"
  "$BIN/producer" -nodes "$NODE_URLS" -workers "$WORKERS" -count "$count" -rate "$RATE" -run-id "$run" \
     -stats-csv "$out/producer.csv" -summary "$out/producer.json" 2> "$out/producer.log" &
  local prod=$!

  $chaos

  wait $prod; local prc=$?
  event producer_done "exit=$prc"
  wait $cons; local crc=$?
  event consumer_done "exit=$crc"
  kill $mon 2>/dev/null; wait $mon 2>/dev/null
  python3 - "$out" <<'EOF'
import json, sys
o = sys.argv[1]
p = json.load(open(o + "/producer.json")); c = json.load(open(o + "/consumer.json"))
print(f"  producer: confirmed {p['confirmed_total']}/{p['expected_total']}  republished={p['republished']} reconnects={p['reconnects']}")
print(f"  consumer: unique={c['unique']} duplicates={c['duplicates']} missing={c.get('missing_total')} "
      f"out_of_order={c['out_of_order']} failovers={c['failovers']} switch_backs={c['switch_backs']}")
print(f"  VERDICT zero_loss={c.get('zero_loss')}")
EOF
}

# ---- chaos timelines --------------------------------------------------------
chaos_none() { :; }

chaos_node_crash() {   # crash the consumer's node (rabbit2), bring it back
  sleep 15; event kill "rabbit2"; kill_node 2
  sleep 20; event start "rabbit2"; start_node 2
}

chaos_leader_crash() { # crash whichever node is the quorum-queue leader
  sleep 15
  local l; l=$(leader_node); LEADER_KILLED=$l
  event kill "rabbit$l (leader)"; kill_node "$l"
  sleep 20; event start "rabbit$l"; start_node "$l"
}

chaos_partition() {    # isolate rabbit3 (consumer's node; follower or leader) from the cluster
  sleep 15; event partition "rabbit3"; partition_node 3
  sleep 40; event heal "rabbit3"; heal_node 3
}

chaos_quorum_loss() {  # lose 2 of 3 nodes: queue must stop, not lose data
  sleep 15; event kill "rabbit2+rabbit3"; kill_node 2; kill_node 3
  sleep 20; event start "rabbit2"; start_node 2
  sleep 10; event start "rabbit3"; start_node 3
}

chaos_rolling_restart() { # graceful restart of every node, one at a time
  local n
  for n in 1 2 3; do
    sleep 8
    event restart "rabbit$n"
    if [ "$MODE" = "docker" ]; then docker restart "rabbit$n" >/dev/null; else ctl "$n" stop >/dev/null 2>&1; sleep 2; start_node "$n"; fi
    wait_cluster_healthy; event healthy "after rabbit$n"
  done
}

# ---- scenarios --------------------------------------------------------------
scenario() {
  case $1 in
    #                         name             msgs/worker consumer-order chaos         leader
    baseline)         run_load baseline         10000 "2 3 1" chaos_none            1 ;;
    node-crash)       run_load node-crash       32000 "2 3 1" chaos_node_crash      1 ;;
    leader-crash)     run_load leader-crash     32000 "2 3 1" chaos_leader_crash    1 ;;
    partition)        run_load partition        40000 "3 2 1" chaos_partition       1 ;;
    partition-leader) run_load partition-leader 40000 "3 2 1" chaos_partition       3 ;;
    quorum-loss)      run_load quorum-loss      30000 "2 3 1" chaos_quorum_loss     1 ;;
    rolling-restart)  run_load rolling-restart  40000 "2 3 1" chaos_rolling_restart 1 ;;
    full-restart)    full_restart ;;
    *) echo "unknown scenario $1"; exit 1 ;;
  esac
  wait_cluster_healthy
}

full_restart() {  # persistence: publish, hard-kill ALL nodes, restart, consume
  local out="$ROOT/results/full-restart"; rm -rf "$out"; mkdir -p "$out"
  EVENTS="$out/events.csv"; echo "epoch_ms,type,detail" > "$EVENTS"
  local run="full-restart-$(date +%H%M%S)" count=5000
  wait_cluster_healthy; reset_queue 1
  event publish "${WORKERS}x$count, no consumer"
  "$BIN/producer" -nodes "$NODE_URLS" -workers "$WORKERS" -count $count -run-id "$run" \
     -summary "$out/producer.json" 2> "$out/producer.log"
  event kill "rabbit1+rabbit2+rabbit3 (SIGKILL)"; kill_node 1; kill_node 2; kill_node 3
  sleep 5
  event start "all nodes"; start_node 1; start_node 2; start_node 3
  wait_cluster_healthy; event healthy "cluster back"
  "$BIN/consumer" -nodes "$NODE_URLS" -run-id "$run" -expect $((WORKERS * count)) -expect-per-producer $count \
     -idle-timeout 30s -summary "$out/consumer.json" 2> "$out/consumer.log"
  event consumed "exit=$?"
  python3 -c "import json;c=json.load(open('$out/consumer.json'));print('  recovered',c['unique'],'missing',c['missing_total'],'VERDICT zero_loss=',c['zero_loss'])"
}

[ $# -ge 1 ] || { sed -n '2,20p' "$0"; exit 1; }
build_tools || exit 1
if [ "$1" = "all" ]; then
  for s in baseline node-crash leader-crash partition partition-leader quorum-loss rolling-restart full-restart; do scenario $s; done
else
  scenario "$1"
fi
