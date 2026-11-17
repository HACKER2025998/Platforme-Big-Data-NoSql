#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python3 scripts/generate-measures.py --seed --count 2 --once --interval 0.1 "$@"
