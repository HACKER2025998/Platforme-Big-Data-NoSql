#!/usr/bin/env bash
# Restauration strictement réservée à une pile distincte, jamais aux volumes de travail.
set -euo pipefail
cd "$(dirname "$0")/.."
backup_dir="$(realpath "${1:?Usage : restore-dev.sh DOSSIER temperature-restore-NOM}")"
restore_project="${2:?Projet de restauration requis}"
[[ "$restore_project" =~ ^temperature-restore-[a-z0-9-]+$ ]] || { echo 'Le projet doit commencer par temperature-restore-' >&2; exit 2; }
for file in mongo.archive.gz mesures_par_capteur_jour.csv derniere_mesure.csv; do test -s "$backup_dir/$file"; done
# Refuser une pile existante : restauration uniquement sur volumes neufs.
[[ -z $(docker volume ls -q --filter "label=com.docker.compose.project=$restore_project") ]] || { echo 'Le projet cible possède déjà des volumes ; choisir un nom neuf' >&2; exit 2; }
override_file="$(mktemp /tmp/temperature-restore.XXXXXX.yaml)"
trap 'rm -f "$override_file"' EXIT
cat > "$override_file" <<'YAML'
services:
  mongodb:
    ports: !reset []
  cassandra:
    ports: !reset []
  redis:
    ports: !reset []
  kafka:
    ports: !reset []
  backend:
    image: temperature-backend
    ports: !override ["127.0.0.1:8092:8080"]
  web:
    image: temperature-web
    ports: !override ["127.0.0.1:8098:80"]
YAML
restore_compose() { docker compose --env-file .env -p "$restore_project" -f infra/compose.dev.yaml -f "$override_file" "$@"; }
restore_compose up -d --no-build mongodb cassandra cassandra-init redis kafka kafka-init
for attempt in $(seq 1 150); do
  if restore_compose exec -T mongodb sh -ec 'mongosh --quiet --username "$MONGO_APP_USERNAME" --password "$MONGO_APP_PASSWORD" --authenticationDatabase temperature temperature --eval "if(db.runCommand({ping:1}).ok!==1)quit(1)"' >/dev/null 2>&1; then break; fi
  [[ "$attempt" != 150 ]] || exit 1
  sleep 2
done
# Attendre les deux initialisations avant les imports.
for init in cassandra-init kafka-init; do
  init_id="$(restore_compose ps -a -q "$init")"
  exit_code="$(docker wait "$init_id")"
  [[ "$exit_code" == 0 ]] || { echo "Initialisation $init en échec" >&2; exit 1; }
done
restore_compose exec -T mongodb sh -ec 'exec mongorestore --quiet --username "$MONGO_APP_USERNAME" --password "$MONGO_APP_PASSWORD" --authenticationDatabase temperature --archive --gzip --nsInclude "temperature.*"' < "$backup_dir/mongo.archive.gz"
for table in mesures_par_capteur_jour derniere_mesure; do
  restore_compose cp "$backup_dir/$table.csv" "cassandra:/tmp/temperature-$table.csv" >/dev/null
  restore_compose exec -T -e BACKUP_TABLE="$table" cassandra sh -ec 'cqlsh -u temperature_app -p "$CASSANDRA_APP_PASSWORD" -e "COPY temperature.$BACKUP_TABLE FROM '\''/tmp/temperature-$BACKUP_TABLE.csv'\'' WITH HEADER = TRUE"' >/dev/null
done
restore_compose up -d --no-build --no-deps backend web
for attempt in $(seq 1 120); do
  if curl -fsS --max-time 12 http://localhost:8092/actuator/health/readiness >/dev/null; then
    echo 'Restauration prête : API http://localhost:8092 ; interface http://localhost:8098'
    echo "Arrêt sans effacer les données : docker compose -p $restore_project -f infra/compose.dev.yaml --env-file .env down"
    exit 0
  fi
  sleep 2
done
echo 'Délai de restauration dépassé' >&2
exit 1
