#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -x ".venv/bin/python" ]]; then
  PYTHON=".venv/bin/python"
else
  PYTHON="python3"
fi

if [[ "${SENQUANT_FORCE_DAILY_REFRESH:-}" != "1" ]]; then
  weekday="$(date +%u)"
  hour="$(date +%H)"
  minute="$(date +%M)"
  if (( weekday > 5 )); then
    echo "Weekend; skipping daily refresh."
    exit 0
  fi
  if (( 10#$hour < 15 || (10#$hour == 15 && 10#$minute < 30) )); then
    echo "Before 3:30 PM local time; skipping daily refresh."
    exit 0
  fi
fi

"$PYTHON" -m datacollection.cli daily-refresh --skip-if-completed "$@"
