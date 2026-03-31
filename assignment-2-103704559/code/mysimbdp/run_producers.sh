#!/usr/bin/env bash
# ------------------------------------------------------------
# run_producers.sh  (run from mysimbdp/ root)
# - Starts tenantA and tenantB Python producers in the background
# - Saves logs to ./logs/
# - Saves process IDs (PIDs) so you can stop them later
# ------------------------------------------------------------

set -euo pipefail

# Resolve the repo root (directory this script lives in)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

TENANT_A_SCRIPT="${SCRIPT_DIR}/tenants/tenantA/tenantA_producer.py"
TENANT_B_SCRIPT="${SCRIPT_DIR}/tenants/tenantB/tenantB_producer.py"
LOG_DIR="${SCRIPT_DIR}/logs"

mkdir -p "${LOG_DIR}"

echo "==> Starting tenant producers (background)..."

# ── helper: start one producer safely ────────────────────────────────────────
start_producer() {
  local tenant="$1"
  local script="$2"
  local logfile="${LOG_DIR}/${tenant}_producer.log"
  local pidfile="${LOG_DIR}/${tenant}_producer.pid"

  if [[ ! -f "${script}" ]]; then
    echo "❌ Cannot start ${tenant} producer: '${script}' not found."
    return 1
  fi

  # Clean up stale PID if process is no longer running
  if [[ -f "${pidfile}" ]]; then
    old_pid="$(cat "${pidfile}")"
    if kill -0 "${old_pid}" 2>/dev/null; then
      echo "⚠️  ${tenant} producer already running (PID=${old_pid}). Skipping."
      return 0
    else
      rm -f "${pidfile}"
    fi
  fi

  nohup python3 "${script}" > "${logfile}" 2>&1 &
  echo $! > "${pidfile}"
  echo "✅ ${tenant} producer started (PID=$(cat "${pidfile}"))"
}

# ── start both producers ──────────────────────────────────────────────────────
start_producer "tenantA" "${TENANT_A_SCRIPT}"
start_producer "tenantB" "${TENANT_B_SCRIPT}"

echo ""
echo "Logs:"
echo "  tail -f ${LOG_DIR}/tenantA_producer.log"
echo "  tail -f ${LOG_DIR}/tenantB_producer.log"
echo ""
echo "To stop producers:"
echo "  kill \$(cat ${LOG_DIR}/tenantA_producer.pid) \$(cat ${LOG_DIR}/tenantB_producer.pid)"
echo ""
echo "Next — start workers via manager:"
echo "  curl -X POST http://localhost:8001/tenants/tenantA/workers/start \\"
echo "       -H 'Content-Type: application/json' -d '{}'"
echo "  curl -X POST http://localhost:8001/tenants/tenantB/workers/start \\"
echo "       -H 'Content-Type: application/json' -d '{}'"