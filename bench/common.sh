#!/usr/bin/env bash
# Shared settings for run_b1.sh, run_b2.sh and run_b3.sh. Run them on the VM.
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "${BENCH_DIR}")"
export PATH="/usr/local/go/bin:${HOME}/go/bin:/snap/bin:${PATH}"

REGISTRY="${REGISTRY:-sztypaa/rpi-task}"
read -r -a WORKERS <<< "${WORKERS:-pi@192.168.1.100 pi@192.168.1.110}"
DISPATCHER="${DISPATCHER:-http://127.0.0.1:8080}"
RESULTS_DIR="${RESULTS_DIR:-${HOME}/bench-results}"
LOADGEN=/tmp/loadgen-bin

if [[ -z "${TASK_CMD:-}" ]]; then
  echo "Set TASK_CMD to the command run in the task container, e.g. TASK_CMD=\"/task --sleep 2s\"" >&2
  exit 1
fi

mkdir -p "${RESULTS_DIR}"
(cd "${REPO_DIR}" && go build -o "${LOADGEN}" ./bench/loadgen)

loadgen() {
  "${LOADGEN}" -dispatcher "${DISPATCHER}" "$@"
}

# Non-interactive ssh to the Raspberry Pi (same password for ssh and sudo, as in deploy_and_test.sh)
setup_ssh() {
  if [[ -z "${SSH_PASS:-}" ]]; then
    echo "Set SSH_PASS (password of user pi), it is needed to remove images from the Raspberry Pi" >&2
    exit 1
  fi
  ASKPASS_SCRIPT="$(mktemp)"
  chmod 700 "${ASKPASS_SCRIPT}"
  trap 'rm -f "${ASKPASS_SCRIPT}"' EXIT
  printf '#!/usr/bin/env bash\nprintf "%%s\\n" "${SSH_PASS}"\n' > "${ASKPASS_SCRIPT}"
  export SSH_PASS SSH_ASKPASS="${ASKPASS_SCRIPT}" SSH_ASKPASS_REQUIRE=force DISPLAY="${DISPLAY:-dummy:0}"
}

# Removes the given images from every Raspberry Pi, so the next task has to pull them (cold start).
remove_images() {
  for worker in "${WORKERS[@]}"; do
    printf '%s\n' "${SSH_PASS}" | ssh -o StrictHostKeyChecking=accept-new "${worker}" "
      IFS= read -r PW
      for img in $*; do
        if printf '%s\n' \"\${PW}\" | sudo -S -p '' k3s crictl inspecti \"docker.io/\${img}\" >/dev/null 2>&1; then
          printf '%s\n' \"\${PW}\" | sudo -S -p '' k3s crictl rmi \"docker.io/\${img}\" >/dev/null || echo \"  [WARN] \${img} not removed on \$(hostname)\"
        fi
      done
    "
  done
  # kubelet reports the list of images on the node with a delay
  sleep 15
}
