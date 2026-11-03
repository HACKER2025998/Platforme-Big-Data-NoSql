#!/usr/bin/env bash
set -euo pipefail
# Les secrets générés sont hexadécimaux ; interdire toute interpolation CQL ambiguë.
for value in "$CASSANDRA_APP_PASSWORD" "$CASSANDRA_ADMIN_PASSWORD"; do
  [[ "$value" =~ ^[a-zA-Z0-9_-]{16,128}$ ]] || { echo 'Secret Cassandra : 16 à 128 caractères alphanumériques, tiret ou underscore' >&2; exit 1; }
done
admin=''
for i in $(seq 1 120); do
  if cqlsh cassandra -u cassandra -p "$CASSANDRA_ADMIN_PASSWORD" -e 'SELECT cluster_name FROM system.local' >/dev/null 2>&1; then admin="$CASSANDRA_ADMIN_PASSWORD"; break; fi
  if cqlsh cassandra -u cassandra -p cassandra -e 'SELECT cluster_name FROM system.local' >/dev/null 2>&1; then admin=cassandra; break; fi
  sleep 3
done
[[ -n "$admin" ]] || { echo 'Cassandra ne répond pas ou identifiants admin invalides' >&2; exit 1; }
cqlsh cassandra -u cassandra -p "$admin" -f /schema.cql
# Ne pas imprimer les commandes contenant les mots de passe.
{
  printf "CREATE ROLE IF NOT EXISTS temperature_app WITH PASSWORD = '%s' AND LOGIN = true;\n" "$CASSANDRA_APP_PASSWORD"
  printf 'GRANT SELECT ON KEYSPACE temperature TO temperature_app;\nGRANT MODIFY ON KEYSPACE temperature TO temperature_app;\n'
  if [[ "$admin" == cassandra ]]; then printf "ALTER ROLE cassandra WITH PASSWORD = '%s';\n" "$CASSANDRA_ADMIN_PASSWORD"; fi
} | cqlsh cassandra -u cassandra -p "$admin"
echo 'Schéma et comptes Cassandra prêts.'
