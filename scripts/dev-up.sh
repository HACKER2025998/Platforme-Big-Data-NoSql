#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python3 scripts/init-env.py
scripts/compose.sh up -d --build
scripts/migrate-mongo.sh
for i in $(seq 1 120); do
  if curl -fsS http://localhost:8080/actuator/health/readiness >/dev/null; then echo 'Plateforme prête : http://localhost:8088'; exit 0; fi
  sleep 3
done
echo 'Délai de préparation dépassé ; consulter scripts/compose.sh logs' >&2
exit 1
