#!/usr/bin/env bash
# End-to-end run against a local Redpanda: dry run, cut, rollback.
# Every event is written to both orders and orders_v2. The output must hold
# each event exactly once, taken from the copy the switch assigns to its ts.
set -euo pipefail
cd "$(dirname "$0")"

T=1794781800000      # cut to orders_v2
R=$((T + 3600000))   # roll back to orders an hour later

rpk() { docker compose exec -T redpanda rpk "$@"; }
trap 'docker compose down -v >/dev/null 2>&1' EXIT

# produce FIRST LAST BASE writes ids FIRST..LAST, with ts = BASE + id, to both topics.
produce() {
  seq "$1" "$2" | jq -c --argjson base "$3" '{id: ., ts: ($base + .), copy: "old"}' | rpk topic produce orders >/dev/null
  seq "$1" "$2" | jq -c --argjson base "$3" '{id: ., ts: ($base + .), copy: "new"}' | rpk topic produce orders_v2 >/dev/null
}

# phase SWITCH FIRST LAST BASE restarts the processor with SWITCH, then
# produces ids FIRST..LAST and waits until the output holds LAST messages and
# the consumer group has committed everything. Restarting first makes sure
# the old switch never sees this phase's events.
phase() {
  echo "== CUTOVER_SWITCH=$1"
  CUTOVER_SWITCH="$1" docker compose up -d --quiet-pull cutover 2>/dev/null
  produce "$2" "$3" "$4"
  for _ in $(seq 60); do
    got=$(rpk topic describe billing_events -p | awk 'NR > 1 {n += $NF} END {print n + 0}')
    lag=$(rpk group describe billing 2>/dev/null | awk '$1 == "TOTAL-LAG" {print $2}')
    if [ "$got" -ge "$3" ] && [ "$lag" = 0 ]; then
      return
    fi
    sleep 1
  done
  echo "timed out: $got of $3 messages, lag ${lag:-unknown}" >&2
  exit 1
}

GOOS=linux CGO_ENABLED=0 go build -o bin/redpanda-connect-cutover ../cmd/redpanda-connect-cutover
docker compose up -d --quiet-pull --wait redpanda 2>/dev/null
rpk topic create orders orders_v2 billing_events >/dev/null

phase "FROM@0" 1 100 $((T - 1000))                  # all before T
phase "TO@$T" 101 200 $((T - 150))           # 101..149 before T, 150..200 from T on
phase "TO@$T,FROM@$R" 201 300 $((R - 250))   # 201..249 before R, 250..300 from R on

# The final switch holds the whole history, so it decides every id.
problems=$(rpk topic consume billing_events -o :end -f '%v\n' | jq -rs --argjson t "$T" --argjson r "$R" '
  (if (map(.id) | sort) != [range(1; 301)] then "ids are not 1..300 exactly once" else empty end),
  (.[] | select(.copy != (if .ts >= $t and .ts < $r then "new" else "old" end)) | "id \(.id) served from \(.copy)")')
if [ -n "$problems" ]; then
  echo "FAIL"; echo "$problems"; exit 1
fi
echo "PASS: 300 events, each served once from the right copy"
