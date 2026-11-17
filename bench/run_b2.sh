#!/usr/bin/env bash
# B2: does the objective function send tasks where the image already is?
# Before every series fn-a and fn-b are placed on the first Raspberry Pi and fn-c and fn-d
# on the second one, then the same sequence of tasks is submitted.
#   TASK_CMD="..." SSH_PASS=... ./bench/run_b2.sh
source "$(dirname "$0")/common.sh"
setup_ssh

TASKS="${TASKS:-30}"
RATE="${RATE:-0.2}"
REPEAT="${REPEAT:-3}"
OUT="${RESULTS_DIR}/b2.csv"
FN_A="${REGISTRY}:fn-a"
FN_B="${REGISTRY}:fn-b"
FN_C="${REGISTRY}:fn-c"
FN_D="${REGISTRY}:fn-d"

for i in $(seq 1 "${REPEAT}"); do
  echo "==> B2 run ${i}/${REPEAT}"
  remove_images "${FN_A}" "${FN_B}" "${FN_C}" "${FN_D}"
  pull_images_on "${WORKERS[0]}" "${FN_A}" "${FN_B}"
  pull_images_on "${WORKERS[1]}" "${FN_C}" "${FN_D}"
  # kubelet reports the list of images on the node with a delay of up to about a minute
  sleep 60
  loadgen -tasks "${TASKS}" -rate "${RATE}" -images "${FN_A},${FN_B},${FN_C},${FN_D}" -zipf 0.75 -seed "${i}" \
    -cmd "${TASK_CMD}" -cpu 1500 -mem 128 -label b2 -out "${OUT}"
done
echo "==> B2 done, results in ${OUT}"
