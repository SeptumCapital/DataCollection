#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -f ".env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source ".env"
  set +a
fi

DATA_DIR="${DATA_COLLECTION_ROOT:-./data}"
RENDER_DATA_ROOT="${RENDER_DATA_ROOT:-/var/data/senquant}"

if [[ -z "${RENDER_SSH_TARGET:-}" ]]; then
  cat >&2 <<'MSG'
RENDER_SSH_TARGET is not set.

Add it to .env, for example:
RENDER_SSH_TARGET="srv-xxxxx@ssh.virginia.render.com"

Then rerun:
scripts/sync_render_data.sh
MSG
  exit 1
fi

if [[ ! -d "$DATA_DIR" ]]; then
  echo "Data directory not found: $DATA_DIR" >&2
  exit 1
fi

archive="$(mktemp -t senquant-data.XXXXXX.tgz)"
trap 'rm -f "$archive"' EXIT

echo "Packing $DATA_DIR ..."
tar -czf "$archive" -C "$(dirname "$DATA_DIR")" "$(basename "$DATA_DIR")"

echo "Uploading data archive to $RENDER_SSH_TARGET:$RENDER_DATA_ROOT/ ..."
scp -s "$archive" "$RENDER_SSH_TARGET:$RENDER_DATA_ROOT/senquant-data.tgz"

cat <<MSG
Upload complete.

In Render Shell, run:
cd $RENDER_DATA_ROOT
tar -xzf senquant-data.tgz
curl https://septumcapital.com/health
MSG

