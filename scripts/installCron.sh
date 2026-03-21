#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
TEMPLATE="$ROOT_DIR/ops/cron.gpu-market-dashboard"

if [[ ! -f "$TEMPLATE" ]]; then
  echo "Cron template not found: $TEMPLATE" >&2
  exit 1
fi

( crontab -l 2>/dev/null; echo ""; cat "$TEMPLATE" ) | crontab -
echo "Installed cron from $TEMPLATE"
