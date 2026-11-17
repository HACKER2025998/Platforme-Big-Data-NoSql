#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ ! ${1:-} =~ ^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$ ]]; then echo 'Usage : scripts/replay-ingestion.sh UUID' >&2; exit 2; fi
scripts/compose.sh exec -T -e REPLAY_EVENT_ID="$1" mongodb sh -ec 'exec mongosh --quiet --username "$MONGO_APP_USERNAME" --password "$MONGO_APP_PASSWORD" --authenticationDatabase temperature temperature --eval '\''const r=db.ingestions.updateOne({_id:process.env.REPLAY_EVENT_ID,processingState:"FAILED"},{$set:{publicationState:"PENDING",processingState:"PENDING",nextAttemptAt:0},$unset:{lastErrorCode:""}}); if(r.modifiedCount!==1){print("Aucune ingestion FAILED correspondante");quit(1)};print("Ingestion remise en attente")'\'''
