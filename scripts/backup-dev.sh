#!/usr/bin/env bash
# Sauvegarde logique d'une petite pile Docker de laboratoire, sans secret dans l'archive.
set -euo pipefail
cd "$(dirname "$0")/.."
backup_dir="${1:-/tmp/temperature-backup-$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "$backup_dir"
backup_dir="$(realpath "$backup_dir")"
chmod 700 "$backup_dir"
[[ -z $(find "$backup_dir" -mindepth 1 -maxdepth 1 -print -quit) ]] || { echo 'Le dossier de sauvegarde doit être vide' >&2; exit 2; }
scripts/compose.sh stop backend
trap 'scripts/compose.sh start backend >/dev/null' EXIT
scripts/compose.sh exec -T mongodb sh -ec 'exec mongodump --quiet --username "$MONGO_APP_USERNAME" --password "$MONGO_APP_PASSWORD" --authenticationDatabase temperature --db temperature --archive --gzip' > "$backup_dir/mongo.archive.gz"
for table in mesures_par_capteur_jour derniere_mesure; do
  scripts/compose.sh exec -T -e BACKUP_TABLE="$table" cassandra sh -ec 'cqlsh -u temperature_app -p "$CASSANDRA_APP_PASSWORD" -e "COPY temperature.$BACKUP_TABLE TO '\''/tmp/temperature-$BACKUP_TABLE.csv'\'' WITH HEADER = TRUE"' >/dev/null
  scripts/compose.sh cp "cassandra:/tmp/temperature-$table.csv" "$backup_dir/$table.csv" >/dev/null
done
cp infra/cassandra/001-schema.cql infra/versions.env "$backup_dir/"
git rev-parse HEAD > "$backup_dir/source-commit.txt"
chmod 600 "$backup_dir/"*
printf 'Sauvegarde logique créée : %s\n' "$backup_dir"
