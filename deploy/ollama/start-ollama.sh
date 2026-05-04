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

wait "$SERVER_PID"
