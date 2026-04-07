#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
API_PORT="${API_PORT:-8000}"
FRONTEND_PORT="${FRONTEND_PORT:-5173}"
API_HOST="${API_HOST:-127.0.0.1}"
FRONTEND_HOST="${FRONTEND_HOST:-127.0.0.1}"

if [[ ! -d "${ROOT_DIR}/.venv" ]]; then
  echo "Missing .venv. Run the setup steps from README.md first."
  exit 1
fi

if [[ ! -d "${ROOT_DIR}/frontend/node_modules" ]]; then
  echo "Missing frontend/node_modules. Run 'cd frontend && npm install' first."
  exit 1
fi

API_PID=""
FRONTEND_PID=""

cleanup() {
  local exit_code=$?

  if [[ -n "${API_PID}" ]] && kill -0 "${API_PID}" 2>/dev/null; then
    kill "${API_PID}" 2>/dev/null || true
  fi

  if [[ -n "${FRONTEND_PID}" ]] && kill -0 "${FRONTEND_PID}" 2>/dev/null; then
    kill "${FRONTEND_PID}" 2>/dev/null || true
  fi

  wait 2>/dev/null || true
  exit "${exit_code}"
}

trap cleanup EXIT INT TERM

echo "Starting Loom API on http://${API_HOST}:${API_PORT}"
(
  cd "${ROOT_DIR}"
  source .venv/bin/activate
  exec uvicorn pipeline.api.app:app --reload --host "${API_HOST}" --port "${API_PORT}"
) &
API_PID=$!

echo "Starting Loom frontend on http://${FRONTEND_HOST}:${FRONTEND_PORT}"
(
  cd "${ROOT_DIR}/frontend"
  exec npm run dev -- --host "${FRONTEND_HOST}" --port "${FRONTEND_PORT}"
) &
FRONTEND_PID=$!

echo ""
echo "Loom is starting up."
echo "Frontend: http://${FRONTEND_HOST}:${FRONTEND_PORT}"
echo "API:      http://${API_HOST}:${API_PORT}"
echo "Health:   make health"
echo "Press Ctrl+C to stop both services."
echo ""

while true; do
  if ! kill -0 "${API_PID}" 2>/dev/null; then
    wait "${API_PID}"
    exit $?
  fi

  if ! kill -0 "${FRONTEND_PID}" 2>/dev/null; then
    wait "${FRONTEND_PID}"
    exit $?
  fi

  sleep 1
done
