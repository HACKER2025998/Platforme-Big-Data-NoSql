#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
scripts/compose.sh exec -T mongodb sh -ec 'exec mongosh --quiet --username "$MONGO_INITDB_ROOT_USERNAME" --password "$MONGO_INITDB_ROOT_PASSWORD" --authenticationDatabase admin /docker-entrypoint-initdb.d/001-init.js'
