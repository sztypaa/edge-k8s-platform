#!/usr/bin/env bash
# B2: does the objective function send tasks where the image already is?
# Run it once for every version of the agent and give each run its own label:
#   TASK_CMD="..." SSH_PASS=... ./bench/run_b2.sh new
source "$(dirname "$0")/common.sh"
setup_ssh

LABEL="${1:?usage: run_b2.sh <label>, e.g. new or old}"
TASKS="${TASKS:-30}"
RATE="${RATE:-0.5}"
REPEAT="${REPEAT:-3}"
OUT="${RESULTS_DIR}/b2.csv"
IMAGES="${REGISTRY}:fn-a,${REGISTRY}:fn-b,${REGISTRY}:fn-c,${REGISTRY}:fn-d"

for i in $(seq 1 "${REPEAT}"); do
  echo "==> B2 run ${i}/${REPEAT} (${LABEL})"
  remove_images ${IMAGES//,/ }
  loadgen -tasks "${TASKS}" -rate "${RATE}" -images "${IMAGES}" -zipf 0.75 -seed "${i}" \
    -cmd "${TASK_CMD}" -cpu 1500 -mem 128 -label "${LABEL}" -out "${OUT}"
done
echo "==> B2 done, results in ${OUT}"
