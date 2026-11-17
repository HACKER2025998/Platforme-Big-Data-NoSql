#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p /tmp/temperature-reports
npx --yes newman@6.2.1 run postman/temperature.postman_collection.json -e postman/local.postman_environment.json --delay-request 1000 --timeout 180000 --bail --reporters cli,json --reporter-json-export /tmp/temperature-reports/postman.json
