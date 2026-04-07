#!/usr/bin/env bash

set -euo pipefail

API_URL="${LOOM_API_URL:-http://127.0.0.1:8000/api/health}"
FRONTEND_URL="${LOOM_FRONTEND_URL:-http://127.0.0.1:5173}"

if ! command -v curl >/dev/null 2>&1; then
  echo "curl is required for the health check."
  exit 1
fi

echo "Checking API: ${API_URL}"
API_RESPONSE="$(curl -fsS "${API_URL}")"
echo "API ok: ${API_RESPONSE}"

echo "Checking frontend: ${FRONTEND_URL}"
curl -fsS -I "${FRONTEND_URL}" >/dev/null
echo "Frontend ok"
