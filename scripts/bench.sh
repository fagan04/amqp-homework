#!/usr/bin/env bash
# Performance benchmark suite.
#
#   scripts/bench.sh [throughput|size|consumer|latency|all]
#
#   throughput : max publish throughput vs. workers x in-flight confirm window
#   size       : publish throughput vs. message size
#   consumer   : drain throughput vs. consumer prefetch
#   latency    : end-to-end and confirm latency at fixed offered loads
#
# Results: results/bench/<suite>/*.json and results/bench/<suite>.csv
source "$(dirname "$0")/lib.sh"
OUT="$ROOT/results/bench"

prod() { # prod <file> <args...>
  local f=$1; shift
  "$BIN/producer" -nodes "$NODE_URLS" -summary "$f.json" "$@" 2> "$f.log"
}

throughput() {
  local d="$OUT/throughput"; rm -rf "$d"; mkdir -p "$d"
  echo "workers,inflight,size,msgs,throughput_msg_s,confirm_p50_ms,confirm_p99_ms" > "$OUT/throughput.csv"
  for w in 1 4 8; do for inf in 1 16 128 512; do
    local c=$((inf == 1 ? 1500 : 12000))
    purge_queue
    prod "$d/w${w}_i${inf}" -workers $w -inflight $inf -count $c -size 512 -run-id "tp-$w-$inf"
    python3 - "$d/w${w}_i${inf}.json" >> "$OUT/throughput.csv" <<'EOF'
import json,sys; p=json.load(open(sys.argv[1]))
print(f"{p['workers']},{p['inflight']},{p['size_bytes']},{p['confirmed_total']},{p['throughput_msg_s']:.0f},{p['confirm_latency']['p50_ms']},{p['confirm_latency']['p99_ms']}")
EOF
    tail -1 "$OUT/throughput.csv"
  done; done
  purge_queue
}

size() {
  local d="$OUT/size"; rm -rf "$d"; mkdir -p "$d"
  echo "size,msgs,throughput_msg_s,throughput_MB_s,confirm_p50_ms,confirm_p99_ms" > "$OUT/size.csv"
  for sz in 128 1024 4096 16384 65536; do
    local c=$((sz >= 16384 ? 2500 : 10000))
    purge_queue
    prod "$d/s$sz" -workers 4 -inflight 128 -count $c -size $sz -run-id "sz-$sz"
    python3 - "$d/s$sz.json" >> "$OUT/size.csv" <<'EOF'
import json,sys; p=json.load(open(sys.argv[1]))
t=p['throughput_msg_s']
print(f"{p['size_bytes']},{p['confirmed_total']},{t:.0f},{t*p['size_bytes']/1e6:.2f},{p['confirm_latency']['p50_ms']},{p['confirm_latency']['p99_ms']}")
EOF
    tail -1 "$OUT/size.csv"
  done
  purge_queue
}

consumer() {
  local d="$OUT/consumer"; rm -rf "$d"; mkdir -p "$d"
  echo "prefetch,msgs,elapsed_s,throughput_msg_s" > "$OUT/consumer.csv"
  for pf in 1 10 100 500; do
    purge_queue
    local run="cons-$pf"
    prod "$d/p$pf-producer" -workers 4 -inflight 128 -count 10000 -size 512 -run-id "$run"
    "$BIN/consumer" -nodes "$NODE_URLS" -run-id "$run" -prefetch $pf -expect 40000 -expect-per-producer 10000 \
       -idle-timeout 20s -summary "$d/p$pf.json" 2> "$d/p$pf.log"
    python3 - "$d/p$pf.json" $pf >> "$OUT/consumer.csv" <<'EOF'
import json,sys; c=json.load(open(sys.argv[1]))
print(f"{sys.argv[2]},{c['unique']},{c['elapsed_s']:.2f},{c['unique']/c['elapsed_s']:.0f}")
EOF
    tail -1 "$OUT/consumer.csv"
  done
}

latency() {
  local d="$OUT/latency"; rm -rf "$d"; mkdir -p "$d"
  echo "offered_rate,achieved_msg_s,e2e_p50_ms,e2e_p95_ms,e2e_p99_ms,e2e_max_ms,confirm_p50_ms,confirm_p99_ms" > "$OUT/latency.csv"
  for r in 500 1000 2000 4000 8000; do
    purge_queue
    local per=$((r * 20 / 4))   # 20 s of load over 4 workers
    local run="lat-$r"
    "$BIN/consumer" -nodes "$NODE_URLS" -run-id "$run" -prefetch 100 -expect $((per * 4)) -expect-per-producer $per \
       -idle-timeout 20s -summary "$d/r$r-consumer.json" 2> "$d/r$r-consumer.log" &
    local cpid=$!
    sleep 1
    prod "$d/r$r-producer" -workers 4 -inflight 128 -count $per -size 512 -rate $r -run-id "$run"
    wait $cpid
    python3 - "$d/r$r-producer.json" "$d/r$r-consumer.json" $r >> "$OUT/latency.csv" <<'EOF'
import json,sys; p=json.load(open(sys.argv[1])); c=json.load(open(sys.argv[2])); e=c['e2e_latency']; cl=p['confirm_latency']
print(f"{sys.argv[3]},{p['throughput_msg_s']:.0f},{e['p50_ms']},{e['p95_ms']},{e['p99_ms']},{e['max_ms']},{cl['p50_ms']},{cl['p99_ms']}")
EOF
    tail -1 "$OUT/latency.csv"
  done
}

build_tools || exit 1
wait_cluster_healthy || exit 1
mkdir -p "$OUT"
case "${1:-all}" in
  throughput) throughput ;;
  size) size ;;
  consumer) consumer ;;
  latency) latency ;;
  all) throughput; size; consumer; latency ;;
  *) echo "usage: $0 [throughput|size|consumer|latency|all]"; exit 1 ;;
esac
