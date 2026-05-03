#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/Users/soumyasen/Agentic AI/AGQ/SenQuant/DataCollection"
HOST="127.0.0.1"
START_PORT="${SENQUANT_PORT:-8010}"
LOG_DIR="$APP_DIR/logs"
PID_DIR="$APP_DIR/.run"

cd "$APP_DIR"
mkdir -p "$LOG_DIR" "$PID_DIR"

if [[ ! -x "$APP_DIR/.venv/bin/python" ]]; then
  echo "Creating local Python virtual environment..."
  python3 -m venv "$APP_DIR/.venv"
fi

if ! "$APP_DIR/.venv/bin/python" -c "import pandas, yfinance" >/dev/null 2>&1; then
  echo "Installing required Python packages..."
  "$APP_DIR/.venv/bin/pip" install -r "$APP_DIR/requirements.txt"
fi

find_available_port() {
  if curl -fsS "http://$HOST:$START_PORT/api/summary" >/dev/null 2>&1; then
    echo "$START_PORT"
    return 0
  fi

  local port="$START_PORT"
  while [[ "$port" -le 8025 ]]; do
    if ! lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
      echo "$port"
      return 0
    fi
    port=$((port + 1))
  done
  echo "No available port found in 8010-8025." >&2
  return 1
}

PORT="$(find_available_port)"
URL="http://localhost:$PORT/"

if curl -fsS "http://$HOST:$PORT/api/summary" >/dev/null 2>&1; then
  echo "SenQuant Data Browser is already running at $URL"
else
  echo "Starting SenQuant Data Browser at $URL"
  nohup "$APP_DIR/.venv/bin/python" "$APP_DIR/app/server.py" --host "$HOST" --port "$PORT" \
    > "$LOG_DIR/senquant_app_$PORT.log" 2>&1 &
  echo "$!" > "$PID_DIR/senquant_app_$PORT.pid"

  for _ in {1..60}; do
    if curl -fsS "http://$HOST:$PORT/api/summary" >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done

  if ! curl -fsS "http://$HOST:$PORT/api/summary" >/dev/null 2>&1; then
    echo "Server did not become ready. Check: $LOG_DIR/senquant_app_$PORT.log" >&2
    exit 1
  fi
fi

echo "Opening $URL"
open "$URL"
