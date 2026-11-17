#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
scripts/compose.sh exec -T mongodb sh -ec 'mongosh --quiet --username "$MONGO_APP_USERNAME" --password "$MONGO_APP_PASSWORD" --authenticationDatabase temperature temperature --eval "if(db.runCommand({ping:1}).ok!==1)quit(1)"'
echo 'MongoDB : OK'
scripts/compose.sh exec -T cassandra sh -ec 'cqlsh -u temperature_app -p "$CASSANDRA_APP_PASSWORD" -e "SELECT capteur_id FROM temperature.derniere_mesure LIMIT 1" >/dev/null'
echo 'Cassandra : OK'
scripts/compose.sh exec -T redis sh -ec 'REDISCLI_AUTH="$REDIS_PASSWORD" redis-cli ping | grep -q PONG'
echo 'Redis : OK'
scripts/compose.sh exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic mesures-temperature >/dev/null
echo 'Kafka : OK'
curl -fsS --max-time 12 http://localhost:8080/actuator/health/readiness >/dev/null
echo 'Readiness Java : OK'
