#!/usr/bin/env bash
# B3: many tasks at once, more than the clusters can fit; fallback vs reauction.
# B3_CMD should keep the task running for a while (e.g. 20 s), otherwise TASK_CMD is used.
#   TASK_CMD="..." B3_CMD="..." ./bench/run_b3.sh
source "$(dirname "$0")/common.sh"

STRATEGIES="${STRATEGIES:-fallback reauction}"
TASKS="${TASKS:-6}"
CPU="${CPU:-2400}"
REPEAT="${REPEAT:-3}"
OUT="${RESULTS_DIR}/b3.csv"
DISPATCHER_BIN=/tmp/dispatcher-bin

start_dispatcher() {
  pkill -x dispatcher-bin || true
  sleep 1
  nohup "${DISPATCHER_BIN}" -strategy="$1" > /tmp/dispatcher.log 2>&1 < /dev/null &
  sleep 2
  if ! kill -0 $! 2>/dev/null; then
    echo "Error: dispatcher failed to start. Logs:" >&2
    cat /tmp/dispatcher.log >&2
    exit 1
  fi
}

(cd "${REPO_DIR}/dispatcher" && go build -o "${DISPATCHER_BIN}" .)

for strategy in ${STRATEGIES}; do
  start_dispatcher "${strategy}"
  for i in $(seq 1 "${REPEAT}"); do
    echo "==> B3 run ${i}/${REPEAT} (${strategy})"
    loadgen -tasks "${TASKS}" -burst -images "${REGISTRY}:light" -cmd "${B3_CMD:-${TASK_CMD}}" \
      -cpu "${CPU}" -mem 128 -label "${strategy}" -out "${OUT}"
    sleep 3
  done
done

start_dispatcher fallback
echo "==> B3 done, results in ${OUT} (dispatcher restarted with the default strategy)"
