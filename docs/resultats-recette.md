# Résultats de validation

Recette exécutée le **3 novembre 2026**, sur Linux, JDK 21 et Docker Engine 29.9.2. Les contrôles suivants ont été exécutés ; ils ne constituent pas une mesure de performance ou de haute disponibilité.

| Vérification | Résultat constaté |
| --- | --- |
| Java `mvn verify` | 25 tests, zéro échec, zéro test ignoré |
| React `npm run build` | Construction Vite réussie |
| Contrat OpenAPI | Validation Swagger Parser réussie |
| Recette HTTP `scripts/recette.py` | 29 contrôles réussis avec les vrais services |
| Recette panne `scripts/recette-pannes.py` | 13 contrôles réussis avec les vrais services |
| Newman / collection Postman | 20 assertions, zéro échec |
| Navigateur Chromium | Création, modification, mesure, suivi PROCESSED, acquittement, CSV et mobile réussis ; aucune exception JavaScript |
| Authentification Cassandra | Connexion applicative réussie, mauvais mot de passe refusé |
| Ansible | Syntaxe du playbook validée avec community.docker 4.8.1, sans connexion aux VM |
| Clone / volumes neufs | Pile séparée démarrée, recette HTTP réelle exécutée |
| Images Java et React/Nginx | Construction multiétape réussie ; Java sous UID 10001, interface HTTP 200 et probes masquées par Nginx (404) |
| Migration MongoDB | Deux exécutions successives réussies, sans suppression |

## Cas vérifiés

Les contrôles HTTP couvrent les capteurs, conflits de version, PATCH invalide, état sans mesure, admission durable, seuil strict, seuil figé après modification du capteur, replay identique après désactivation, rejet d’un eventId contradictoire, pagination multijour avec dates égales, signature des curseurs, agrégats exacts et entrées hors limites.

Les essais de panne ont arrêté temporairement Redis, Kafka et MongoDB sur la pile locale de recette puis les ont remis en route. Ils vérifient le repli Cassandra, le traitement sans cache, readiness dégradée pour Kafka, liveness indépendante, l’outbox après reprise, le refus d’admission sans Mongo, la conservation du reçu au redémarrage, le rejeu Kafka sans double alerte et sans perte de l’acquittement, la conservation d’un JSON illisible en DLQ et la poursuite de consommation.

Les captures du README proviennent du navigateur sur la vraie API et le jeu de démonstration HTTP. Elles n’attestent pas à elles seules de la réussite des tests.

## Portée et vérifications distinctes

Les tests d’intégration sont exécutés par scripts HTTP/Compose, plutôt que par les classes Testcontainers détaillées dans le backlog. Ils utilisent les vrais services. La matrice exhaustive de pannes à chaque instruction, l’échec de publication DLQ et les campagnes de charge ne sont pas couverts par cette première recette.

Les workflows GitHub joignent leurs propres rapports au commit testé. Leur état est consultable dans l’onglet Actions ; les résultats locaux ne signifient pas qu’une exécution distante a réussi.

Le déploiement Ansible sur deux VM, les sauvegardes/restaurations hors VM et les mesures comparatives installation manuelle/automatisée n’ont pas été exécutés faute d’infrastructure cible fournie. Aucun temps de déploiement ou taux de disponibilité n’est inventé.
