#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILE="${OPENAI_MCP_TUNNEL_PROFILE:-kb-realestate-local}"
TUNNEL_ID="${OPENAI_MCP_TUNNEL_ID:-}"
PYTHON_BIN="${MCP_PYTHON:-python3}"
MCP_COMMAND="${PYTHON_BIN} ${ROOT_DIR}/mcp_server.py"

if ! command -v tunnel-client >/dev/null 2>&1; then
  echo "tunnel-client is not installed. Install it from OpenAI Platform tunnel settings or the openai/tunnel-client release page." >&2
  exit 1
fi

if [[ -z "${CONTROL_PLANE_API_KEY:-}" ]]; then
  echo "CONTROL_PLANE_API_KEY is missing. Export a runtime API key with Tunnels Read + Use permission." >&2
  exit 1
fi

if [[ -z "${TUNNEL_ID}" ]]; then
  echo "OPENAI_MCP_TUNNEL_ID is missing. Export the tunnel_id from OpenAI Platform tunnel settings." >&2
  exit 1
fi

if [[ "${1:-}" == "--init" ]]; then
  tunnel-client init \
    --sample sample_mcp_stdio_local \
    --profile "${PROFILE}" \
    --tunnel-id "${TUNNEL_ID}" \
    --mcp-command "${MCP_COMMAND}"
fi

tunnel-client doctor --profile "${PROFILE}" --explain
exec tunnel-client run --profile "${PROFILE}"
