#!/usr/bin/env bash
set -euo pipefail

MODEL="${OLLAMA_MODEL:-llama3.2:1b}"
export OLLAMA_HOST="${OLLAMA_HOST:-0.0.0.0:11434}"
export OLLAMA_MODELS="${OLLAMA_MODELS:-/var/data/ollama/models}"

mkdir -p "$OLLAMA_MODELS"

ollama serve &
SERVER_PID="$!"

for _ in $(seq 1 60); do
  if ollama list >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

if ! ollama list | awk '{print $1}' | grep -Fxq "$MODEL"; then
  ollama pull "$MODEL"
fi

warm_model() {
  if command -v timeout >/dev/null 2>&1; then
    timeout "${OLLAMA_WARMUP_TIMEOUT_SECONDS:-180}" ollama run "$MODEL" "Reply with ready." >/tmp/ollama-warmup.log 2>&1 || true
  else
    ollama run "$MODEL" "Reply with ready." >/tmp/ollama-warmup.log 2>&1 || true
  fi
}

(
  while true; do
    warm_model
    sleep "${OLLAMA_WARMUP_INTERVAL_SECONDS:-900}"
  done
) &

wait "$SERVER_PID"
