#!/usr/bin/env bash
# B1: cold start (image pulled) vs warm start (image already on the node).
# Before the warm run the image is pulled on every node, because the cluster that
# reports the image in its bid can lag behind by up to a minute.
#   TASK_CMD="..." SSH_PASS=... ./bench/run_b1.sh
source "$(dirname "$0")/common.sh"
setup_ssh

IMAGES="${IMAGES:-light size-50 size-100 size-250 size-500}"
REPEAT="${REPEAT:-5}"
OUT="${RESULTS_DIR}/b1.csv"

for i in $(seq 1 "${REPEAT}"); do
  for tag in ${IMAGES}; do
    image="${REGISTRY}:${tag}"
    echo "==> B1 run ${i}/${REPEAT}: ${image}"
    remove_images "${image}"
    loadgen -tasks 1 -images "${image}" -cmd "${TASK_CMD}" -cpu 500 -mem 128 -label cold -out "${OUT}"
    pull_images "${image}"
    loadgen -tasks 1 -images "${image}" -cmd "${TASK_CMD}" -cpu 500 -mem 128 -label warm -out "${OUT}"
  done
done
echo "==> B1 done, results in ${OUT}"
