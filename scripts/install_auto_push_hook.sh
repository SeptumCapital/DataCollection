#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HOOK_PATH="$ROOT_DIR/.git/hooks/post-commit"

if [[ ! -d "$ROOT_DIR/.git" ]]; then
  echo "This must be run from a Git checkout." >&2
  exit 1
fi

cat > "$HOOK_PATH" <<'HOOK'
#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(git rev-parse --show-toplevel)"
cd "$ROOT_DIR"

branch="$(git branch --show-current)"
if [[ "$branch" != "main" ]]; then
  exit 0
fi

git push origin main
HOOK

chmod +x "$HOOK_PATH"
echo "Installed post-commit auto-push hook at $HOOK_PATH"

