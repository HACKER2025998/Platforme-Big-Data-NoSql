# Plateforme de températures — Backend

Backend Java Spring Boot pour la collecte et la consultation de mesures de température.
Stack : Java 21, Spring Boot 4.0.9, Kafka, MongoDB, Cassandra, Redis, Docker Compose.

## Structure du dépôt

| Dossier | Contenu |
| --- | --- |
| `backend/` | Application Spring Boot (Maven Wrapper) |
| `infra/` | Docker Compose local, versions des images, initialisation des bases |
| `docs/` | Règles métier, contrat OpenAPI, versions, guides |
| `postman/` | Collection et environnement Postman (sans secrets) |
| `scripts/` | Scripts de démarrage, seed et recette |

## Démarrage rapide (cible du projet)

```bash
cp .env.example .env        # renseigner les secrets locaux
./scripts/dev-up.sh         # démarre Kafka, MongoDB, Cassandra, Redis
cd backend
./mvnw spring-boot:run -Dspring-boot.run.profiles=local
```

Tests : `cd backend && ./mvnw verify`
Sous Windows : utiliser `mvnw.cmd`.

> Ces commandes sont la cible ; les scripts sont ajoutés par les tâches suivantes.

## Règles de contribution

1. **Une issue par tâche** : titre `[Txxx] Titre de la tâche`.
2. **Une branche par tâche**, depuis `main` : `feat/Txxx-description-courte`
   (ex. `feat/T021-document-capteur`).
3. **Une PR par tâche**, avec le modèle `.github/pull_request_template.md`.
4. **Revue obligatoire** par un autre membre avant fusion sur `main`.
5. **Fusion sur `main`** seulement si les tests utiles sont verts. Pas de push direct sur `main`.
6. **Aucun secret dans Git** : `.env` est ignoré, seul `.env.example` (valeurs fictives) est versionné.
7. **Fichiers partagés** : A pilote `pom.xml` et `contract/` ; B pilote Compose et configs communes ;
   C pilote Kafka ; D pilote OpenAPI et Postman. Les autres passent par une petite PR ou se coordonnent.

## Équipe

| Membre | Responsabilité |
| --- | --- |
| A | Socle Java, capteurs, admission, qualité, intégration |
| B | Docker local, configurations, Cassandra, Redis |
| C | Kafka, outbox, consommation, alertes, reprise |
| D | Contrat, APIs de lecture, Postman, recette, documentation |
