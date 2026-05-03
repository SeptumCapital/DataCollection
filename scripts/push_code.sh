#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

branch="$(git branch --show-current)"
if [[ "$branch" != "main" ]]; then
  echo "Not on main; skipping automatic push for branch: $branch" >&2
  exit 0
fi

git push origin main

