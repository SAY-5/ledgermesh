#!/usr/bin/env bash
# Chaos test: load the stack at a fixed order rate while killing one service at
# a time, then prove every order reached a terminal state with zero failures.
set -euo pipefail

cd "$(dirname "$0")/.."

# Profiles set the kill schedule; every knob can still be overridden on its own.
PROFILE="${CHAOS_PROFILE:-steady}"
case "$PROFILE" in
  steady) profile_kills=3; profile_restart=5 ;;
  tight)  profile_kills=6; profile_restart=2 ;;
  *) echo "unknown CHAOS_PROFILE $PROFILE (steady, tight)" >&2; exit 2 ;;
esac

PY="${CHAOS_PYTHON:-python3}"
DURATION="${CHAOS_DURATION:-60}"
RATE="${CHAOS_RATE:-20}"
KILLS="${CHAOS_KILLS:-$profile_kills}"
RESTART_AFTER="${CHAOS_RESTART_AFTER:-$profile_restart}"
DRAIN_TIMEOUT="${CHAOS_DRAIN_TIMEOUT:-180}"
DRAIN_CAP="${CHAOS_DRAIN_CAP:-600}"
KEEP_STACK="${CHAOS_KEEP_STACK:-0}"
# The seed drives the kill schedule (moments and victims) and the load mix, so a run can be
# repeated. Without CHAOS_SEED a fresh one is drawn and printed in the summary header.
SEED="${CHAOS_SEED:-$(( $(date +%s) % 100000 ))}"
VICTIM_SPEC="${CHAOS_VICTIMS:-inventory-service payment-service order-service}"
RECORD="${CHAOS_RECORD:-0}"
ORDER_PORT="${LEDGERMESH_ORDER_PORT:-8081}"
INVENTORY_PORT="${LEDGERMESH_INVENTORY_PORT:-8082}"
PAYMENT_PORT="${LEDGERMESH_PAYMENT_PORT:-8083}"
export LEDGERMESH_ORDER_PORT="$ORDER_PORT" LEDGERMESH_INVENTORY_PORT="$INVENTORY_PORT" \
  LEDGERMESH_PAYMENT_PORT="$PAYMENT_PORT"
# report.py prints every knob in effect in the summary header
export CHAOS_PROFILE="$PROFILE" CHAOS_DURATION="$DURATION" CHAOS_RATE="$RATE" CHAOS_KILLS="$KILLS" \
  CHAOS_RESTART_AFTER="$RESTART_AFTER" CHAOS_VICTIMS="$VICTIM_SPEC" CHAOS_SEED="$SEED" \
  CHAOS_DRAIN_TIMEOUT="$DRAIN_TIMEOUT" CHAOS_DRAIN_CAP="$DRAIN_CAP"
PROJECT=ledgermesh
COMPOSE="docker compose -p $PROJECT -f deploy/docker-compose.yml"
OUT=chaos/out
if [[ "$KILLS" == 0 ]]; then LABEL="${CHAOS_LABEL:-baseline}"; else LABEL="${CHAOS_LABEL:-$PROFILE}"; fi
mkdir -p "$OUT"
rm -f "$OUT"/orders.json "$OUT"/snapshots.jsonl "$OUT"/kills.jsonl "$OUT"/summary.txt

declare -A PORT_OF=(
  [order-service]="$ORDER_PORT" [inventory-service]="$INVENTORY_PORT" [payment-service]="$PAYMENT_PORT"
)
read -r -a victims <<< "$VICTIM_SPEC"
for v in "${victims[@]}"; do
  [[ -n "${PORT_OF[$v]:-}" ]] || { echo "unknown victim $v in CHAOS_VICTIMS" >&2; exit 2; }
done
RANDOM=$SEED

log() { printf '%s chaos: %s\n' "$(date +%H:%M:%S)" "$*"; }

LOADGEN=""
cleanup() {
  local rc=$?
  if [[ -n "$LOADGEN" ]] && kill -0 "$LOADGEN" 2>/dev/null; then kill "$LOADGEN" 2>/dev/null || true; fi
  if [[ "$KEEP_STACK" != "1" ]]; then
    log "stopping stack"
    $COMPOSE down -v --remove-orphans >/dev/null 2>&1 || true
  fi
  exit "$rc"
}
# Teardown runs on every exit path, including a failed summary or a readiness timeout.
trap cleanup EXIT

wait_ready() {
  local name=$1 port=$2 deadline=$((SECONDS + 120))
  until curl -fsS "http://localhost:$port/actuator/health/readiness" 2>/dev/null | grep -q '"UP"'; do
    if (( SECONDS > deadline )); then log "$name not ready in time"; $COMPOSE logs --tail=50 "$name"; exit 1; fi
    sleep 1
  done
}

log "starting stack (compose project $PROJECT, profile $PROFILE, $KILLS kills, restart after ${RESTART_AFTER}s, seed $SEED, victims: ${victims[*]})"
$COMPOSE up -d --build --wait
for svc in "${!PORT_OF[@]}"; do
  wait_ready "$svc" "${PORT_OF[$svc]}"
done
log "stack ready"

$PY chaos/loadgen.py --base "http://localhost:$ORDER_PORT" --rate "$RATE" \
  --duration "$DURATION" --seed "$SEED" --out "$OUT/orders.json" &
LOADGEN=$!
START=$SECONDS

# Kill schedule: spread the kills across the load window with seeded jitter.
for (( i=0; i<KILLS; i++ )); do
  slot=$(( DURATION / (KILLS + 1) ))
  target=$(( slot * (i + 1) + RANDOM % (slot / 2 + 1) - slot / 4 ))
  (( target < 5 )) && target=5
  while (( SECONDS - START < target )); do sleep 1; done
  victim=${victims[$(( RANDOM % ${#victims[@]} ))]}
  container="$PROJECT-$victim-1"
  $PY chaos/report.py snapshot "$victim" "$OUT/snapshots.jsonl" || true
  log "killing $container at t+$(( SECONDS - START ))s"
  docker kill --signal=SIGKILL "$container" >/dev/null
  printf '{"service":"%s","at":%s}\n' "$victim" "$($PY -c 'import time;print(time.time())')" >> "$OUT/kills.jsonl"
  sleep "$RESTART_AFTER"
  log "restarting $container"
  docker start "$container" >/dev/null
  wait_ready "$victim" "${PORT_OF[$victim]}"
  log "$victim back at t+$(( SECONDS - START ))s"
done

wait $LOADGEN
LOADGEN=""
log "load finished, waiting for the saga backlog to drain"
$PY chaos/report.py drain "$OUT/orders.json" "$DRAIN_TIMEOUT" "$DRAIN_CAP" || log "drain timed out"

echo
if $PY chaos/report.py summary "$OUT/orders.json" "$OUT/snapshots.jsonl" "$OUT/kills.jsonl"; then
  RESULT=0
else
  RESULT=$?
fi
echo

# CHAOS_RECORD=1 keeps this run's summary and kill timeline under version control.
if [[ "$RECORD" == "1" ]]; then
  dest="chaos/evidence/$LABEL"
  mkdir -p "$dest"
  cp "$OUT/summary.txt" "$dest/summary.txt"
  if [[ -f "$OUT/kills.jsonl" ]]; then cp "$OUT/kills.jsonl" "$dest/kills.jsonl"; else rm -f "$dest/kills.jsonl"; fi
  log "recorded $dest/summary.txt"
fi
exit $RESULT
