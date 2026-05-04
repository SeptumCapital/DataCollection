#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PLIST="$HOME/Library/LaunchAgents/com.senquant.daily-refresh.plist"
LOG_DIR="$ROOT_DIR/logs"
mkdir -p "$LOG_DIR" "$HOME/Library/LaunchAgents"

cat > "$PLIST" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>com.senquant.daily-refresh</string>
  <key>ProgramArguments</key>
  <array>
    <string>/bin/zsh</string>
    <string>-lc</string>
    <string>cd "$ROOT_DIR" &amp;&amp; scripts/daily_refresh.sh</string>
  </array>
  <key>StartInterval</key>
  <integer>3600</integer>
  <key>StandardOutPath</key>
  <string>$LOG_DIR/daily-refresh.out.log</string>
  <key>StandardErrorPath</key>
  <string>$LOG_DIR/daily-refresh.err.log</string>
  <key>WorkingDirectory</key>
  <string>$ROOT_DIR</string>
</dict>
</plist>
PLIST

launchctl bootout "gui/$(id -u)" "$PLIST" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$(id -u)" "$PLIST"
launchctl enable "gui/$(id -u)/com.senquant.daily-refresh"

cat <<MSG
Installed local daily refresh LaunchAgent:
$PLIST

Schedule: hourly. The script skips until today's market-day refresh is needed
and then skips again after one successful refresh for that market date.
Logs:
$LOG_DIR/daily-refresh.out.log
$LOG_DIR/daily-refresh.err.log
MSG
