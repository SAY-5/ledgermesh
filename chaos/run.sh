#!/usr/bin/env bash
# Chaos test: load the stack at a fixed order rate while killing one service at
# a time, then prove every order reached a terminal state with zero failures.
set -euo pipefail

cd "$(dirname "$0")/.."
DURATION="${CHAOS_DURATION:-60}"
RATE="${CHAOS_RATE:-20}"
KILLS="${CHAOS_KILLS:-3}"
RESTART_AFTER="${CHAOS_RESTART_AFTER:-5}"
DRAIN_TIMEOUT="${CHAOS_DRAIN_TIMEOUT:-180}"
DRAIN_CAP="${CHAOS_DRAIN_CAP:-600}"
KEEP_STACK="${CHAOS_KEEP_STACK:-0}"
PROJECT=ledgermesh
COMPOSE="docker compose -p $PROJECT -f deploy/docker-compose.yml"
OUT=chaos/out
mkdir -p "$OUT"
rm -f "$OUT"/orders.json "$OUT"/snapshots.jsonl "$OUT"/kills.jsonl "$OUT"/summary.txt

log() { printf '%s chaos: %s\n' "$(date +%H:%M:%S)" "$*"; }

wait_ready() {
  local name=$1 port=$2 deadline=$((SECONDS + 120))
  until curl -fsS "http://localhost:$port/actuator/health/readiness" 2>/dev/null | grep -q '"UP"'; do
    if (( SECONDS > deadline )); then log "$name not ready in time"; $COMPOSE logs --tail=50 "$name"; exit 1; fi
    sleep 1
  done
}

log "starting stack (compose project $PROJECT)"
$COMPOSE up -d --build --wait
for svc in order-service:8081 inventory-service:8082 payment-service:8083; do
  wait_ready "${svc%%:*}" "${svc##*:}"
done
log "stack ready"

python3 chaos/loadgen.py --rate "$RATE" --duration "$DURATION" --out "$OUT/orders.json" &
LOADGEN=$!
START=$SECONDS

# Kill schedule: spread the kills across the load window with random jitter.
victims=(inventory-service payment-service)
for (( i=0; i<KILLS; i++ )); do
  slot=$(( DURATION / (KILLS + 1) ))
  target=$(( slot * (i + 1) + RANDOM % (slot / 2 + 1) - slot / 4 ))
  (( target < 5 )) && target=5
  while (( SECONDS - START < target )); do sleep 1; done
  victim=${victims[$(( RANDOM % ${#victims[@]} ))]}
  container="$PROJECT-$victim-1"
  python3 chaos/report.py snapshot "$victim" "$OUT/snapshots.jsonl" || true
  log "killing $container at t+$(( SECONDS - START ))s"
  docker kill --signal=SIGKILL "$container" >/dev/null
  printf '{"service":"%s","at":%s}\n' "$victim" "$(python3 -c 'import time;print(time.time())')" >> "$OUT/kills.jsonl"
  sleep "$RESTART_AFTER"
  log "restarting $container"
  docker start "$container" >/dev/null
  case $victim in
    inventory-service) wait_ready "$victim" 8082 ;;
    payment-service) wait_ready "$victim" 8083 ;;
  esac
  log "$victim back at t+$(( SECONDS - START ))s"
done

wait $LOADGEN
log "load finished, waiting for the saga backlog to drain"
python3 chaos/report.py drain "$OUT/orders.json" "$DRAIN_TIMEOUT" "$DRAIN_CAP" || log "drain timed out"

echo
python3 chaos/report.py summary "$OUT/orders.json" "$OUT/snapshots.jsonl" "$OUT/kills.jsonl"
RESULT=$?
echo

if [[ "$KEEP_STACK" != "1" ]]; then
  log "stopping stack"
  $COMPOSE down -v --remove-orphans >/dev/null 2>&1 || true
fi
exit $RESULT
