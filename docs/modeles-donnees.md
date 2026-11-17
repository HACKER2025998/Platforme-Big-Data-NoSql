# Modèles de données et invariants

## MongoDB

`capteurs` : `_id` fourni par le client, `libelle`, `salle`, `seuilTemperature`, `actif`, `version`, `createdAt`, `updatedAt`. La modification compare la version puis l’incrémente atomiquement. Pas de suppression métier.

`ingestions` : `_id=eventId`, événement immuable, empreinte canonique des champs client, `publicationState`, `processingState`, `tentatives`, `nextAttemptAt`, dates de progression et `lastErrorCode` éventuel. Un seul insert durable constitue le reçu et l’outbox ; aucun TTL du registre. Publication et traitement ne s’écrasent pas mutuellement.

L’événement contient `schemaVersion=1`, `eventId`, `capteurId`, `date`, `temperature`, `seuilApplique`, `acceptedAt`. Un changement de seuil ne change pas les reprises. L’empreinte normalise UUID, UTC milliseconde et représentation décimale ; 45.2 et 45.20 représentent la même valeur.

`alertes` : `_id=eventId`, `capteurId`, `date`, `temperature`, `seuilApplique`, `statut` OPEN/ACKNOWLEDGED et `dateAcquittement` éventuelle. `setOnInsert` conserve le premier document et son acquittement. L’acquittement compare OPEN et préserve la première date.

Le script `infra/mongo/init.js` crée des validateurs et les index suivants, de façon réexécutable :

| Collection | Index supplémentaire |
| --- | --- |
| capteurs | salle, actif, _id |
| ingestions | publicationState, nextAttemptAt, _id |
| alertes | capteurId, statut, date DESC, _id DESC |
| alertes | statut, date DESC, _id DESC |

L’unicité est fournie par les index `_id` natifs. Les insertions sont confirmées avec journalisation et majorité ; une instance unique ne fournit aucune redondance matérielle.

## Cassandra

Keyspace `temperature`, NetworkTopologyStrategy, datacenter `datacenter1`, RF1.

```sql
CREATE TABLE mesures_par_capteur_jour (
 capteur_id text, jour date, date_mesure timestamp,
 event_id text, temperature decimal,
 PRIMARY KEY ((capteur_id, jour), date_mesure, event_id)
) WITH CLUSTERING ORDER BY (date_mesure DESC, event_id DESC);

CREATE TABLE derniere_mesure (
 capteur_id text PRIMARY KEY, sort_key text,
 date_mesure timestamp, event_id text, temperature decimal
);
```

Les lectures interrogent les partitions connues par capteur et jour UTC, avec bornes de date. Aucune requête `ALLOW FILTERING`. La pagination utilise un curseur signé lié aux filtres ; les lignes précédentes peuvent être relues et ignorées lors de la reprise d’une page. Cette stratégie simple n’est pas optimisée pour des partitions très volumineuses. Une nouvelle arrivée peut modifier une page ; il n’y a pas de snapshot global.

`sort_key` concatène epoch milliseconde sur 16 chiffres, `:` et UUID canonique. Les dates commencent en 2000 et restent à epoch positif. INSERT IF NOT EXISTS puis UPDATE IF sort_key inférieur assurent la monotonie par CAS. À date identique, le plus grand UUID lexical détermine la dernière valeur.

Compte `temperature_app` : SELECT/MODIFY sur ce keyspace ; pas de modification de schéma. Compte administrateur distinct pour l’initialisation ; son mot de passe par défaut est remplacé.

## Redis

Hash `derniere:{capteurId}` : champs `sortKey` et `value` JSON. Lua compare puis met à jour atomiquement, avec TTL de 24 h. Une mesure ancienne ne remplace pas une récente. Cache absent ou indisponible : lecture de `derniere_mesure` Cassandra et tentative de réchauffement. Redis n’intervient pas dans le calcul durable des statistiques.

## Kafka

Topics `mesures-temperature` et `mesures-temperature-dlq`, trois partitions et facteur de réplication 1, rétention de laboratoire 24 h. La clé est `capteurId`, le groupe `traitement-temperature`. Concurrence de consommation 1 et auto-commit désactivé. La DLQ conserve le payload brut et les en-têtes de topic, partition, offset d’origine et type d’erreur.

## Limites contractuelles

Température et seuil : −50 à 100 °C. Date : 2000 à maintenant + cinq minutes, normalisation milliseconde. Seuil strictement dépassé pour produire une alerte. Période de lecture : sept jours maximum ; historique 500 lignes/page ; statistiques 10 000 mesures maximum. Moyenne HALF_UP à quatre décimales ; cas vide : count 0 et agrégats null.

Le schéma et les comportements sont vérifiés par la recette HTTP et les essais de panne. La maquette ne garantit pas de transaction distribuée : des projections partielles sont réparées par les reprises idempotentes.
