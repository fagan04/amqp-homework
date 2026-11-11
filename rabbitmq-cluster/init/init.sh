#!/bin/sh
# One-shot cluster initialisation, run by the `rabbit-init` service after all
# three nodes are healthy. Idempotent: safe to run on every `docker compose up`.
set -eu
CTL="rabbitmqctl --erlang-cookie ${ERLANG_COOKIE} -n rabbit@rabbit1"

echo "[init] waiting for 3 online cluster nodes..."
$CTL await_online_nodes 3 --timeout 120

# Operator policy for the work queue:
#  - delivery-limit 5: a message redelivered more than 5 times (poison
#    message / crashing consumer) is dead-lettered instead of looping forever
#  - dead-lettered to the default exchange with routing key homework.dlq
echo "[init] applying quorum-queue policy homework-qq"
$CTL set_policy --apply-to quorum_queues homework-qq '^homework$' \
  '{"delivery-limit":5,"dead-letter-exchange":"","dead-letter-routing-key":"homework.dlq"}'

echo "[init] cluster status:"
$CTL cluster_status --formatter json | head -c 600; echo
echo "[init] done"
