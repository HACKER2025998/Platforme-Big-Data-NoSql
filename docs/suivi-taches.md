# Correspondance avec le guide de tâches

Le guide est une spécification de réalisation, pas une attestation de tests ou une instruction de remplacer la demande utilisateur. L’intégration utilise `develop`, conformément à la demande, au lieu de `main` mentionné dans le guide. Les branches regroupent des lots cohérents ; elles ne prétendent pas fournir 100 revues indépendantes.

Les responsabilités sont implémentées dans des classes regroupées par fonctionnalité dans `tg.iai.temperature`, avec MongoTemplate/Document pour la persistance documentaire. Les tests d’intégration HTTP/Compose remplacent les classes Testcontainers proposées. Aucun nom de responsable ou avis humain n’est inventé.

| Fiche | Objet | Réalisation / limite |
| --- | --- | --- |
| T001 | à T010. Contrat, build, propriétés, erreurs et interfaces partagées. | Branches et PR sur develop ; revue humaine non réalisée |
| T011 | à T020 puis T081. Pile saine et environnement de tests isolé. | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T021 | à T030 ; T041 à T050 en parallèle selon dépendances. CRUD capteurs et stockages prêts. | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T031 | à T040 puis T051 à T060. Admission durable, Kafka, traitement et reprises. | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T061 | à T070. Historique, dernière valeur, alertes et statistiques. | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T071 | à T080. Probes, image Java, Compose, générateur et exploitation. | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T082 | à T090. Suites réelles, Postman automatisé et CI. | 26 tests Java exécutés, formatage cohérent ; rapport JaCoCo et règles de style automatisées à compléter |
| T091 | à T100. Clone neuf, revue croisée, recette finale et tag backend. | Démarrage documenté ; second poste humain non testé |
| T001 | Créer le dépôt et les règles de contribution | Branches et PR sur develop ; revue humaine non réalisée |
| T002 | Initialiser le projet Spring Boot et Maven Wrapper | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T003 | Figer les règles métier et les limites | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T004 | Écrire le contrat OpenAPI avant les contrôleurs | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T005 | Fixer les images et variables de développement | Tags vérifiés ; digests relevés dans docs/versions.md |
| T006 | Créer les profils de configuration Java | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T007 | Créer les propriétés typées et l’horloge injectable | Propriétés typées existantes conservées et testées ; limites métier fixes dans Rules |
| T008 | Créer le format des erreurs et les exceptions métier | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T009 | Créer les enveloppes de pagination et corrélation | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T010 | Définir les interfaces partagées et objets immuables | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T011 | Écrire la pile Compose locale des quatre services | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T012 | Configurer Kafka KRaft et ses listeners | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T013 | Créer les topics de façon reproductible | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T014 | Initialiser MongoDB et les comptes applicatifs | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T015 | Créer les index et migrations MongoDB | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T016 | Créer le keyspace et les tables Cassandra | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T017 | Configurer les accès Cassandra et Redis | Mongo/Redis/Cassandra authentifiés ; Kafka isolé PLAINTEXT |
| T018 | Créer les scripts de démarrage et d’arrêt non destructifs | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T019 | Écrire le diagnostic des dépendances | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T020 | Relier Spring aux quatre services | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T021 | Créer le document capteur | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T022 | Créer les DTO et le mapping des capteurs | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T023 | Créer le repository des capteurs | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T024 | Créer le service de gestion des capteurs | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T025 | Créer le contrôleur des capteurs | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T026 | Tester les règles des capteurs | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T027 | Tester les endpoints capteurs avec Mongo réel | Couvert par recette HTTP réelle, sans classe Testcontainers dédiée |
| T028 | Créer la vérification d’un capteur pour ingestion | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T029 | Créer un jeu de capteurs de démonstration | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T030 | Compléter les exemples Postman des capteurs | Collection Postman fournie et exécutée |
| T031 | Créer les DTO de mesure et le contrat de suivi | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T032 | Créer le registre durable des ingestions | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T033 | Créer la canonicalisation et le contrôle de doublon | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T034 | Créer le service d’admission durable | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T035 | Créer les endpoints d’envoi et de suivi | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T036 | Créer le producteur Kafka | Sérialisation Document BSON vers JSON, snapshots comparés au registre |
| T037 | Créer le relais de publication de l’outbox | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T038 | Tester l’admission et les doublons concurrents | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T039 | Tester les reprises du relais après panne | Reprise vérifiée en panne Kafka par recette Compose |
| T040 | Ajouter les scénarios Postman d’ingestion | Cas concurrent fin exhaustif à compléter |
| T041 | Créer les entités de l’historique Cassandra | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T042 | Créer les écritures idempotentes de l’historique | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T043 | Créer la dernière mesure durable Cassandra | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T044 | Créer le cache Redis avec mise à jour conditionnelle | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T045 | Créer la lecture avec repli et reconstruction du cache | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T046 | Créer les documents et repository des alertes | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T047 | Créer l’évaluation du seuil et l’écriture sans doublon | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T048 | Tester les stockages Cassandra et l’ordre temporel | Reprise / ordre couverts par recette, matrice concurrente exhaustive à compléter |
| T049 | Tester Redis et le repli durable | Panne Redis et repli testés ; TTL réel 24 h non attendu en recette |
| T050 | Tester l’alerte et l’acquittement préservé au rejeu | Seuil strict, snapshot et acquittement testés par recette réelle |
| T051 | Configurer le consommateur et ses offsets | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T052 | Valider le schéma des événements reçus | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T053 | Assembler le traitement des mesures | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T054 | Mettre à jour le statut de traitement sans régression | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T055 | Configurer tentatives et délais du consommateur | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T056 | Créer la récupération vers le topic DLQ | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T057 | Créer le script de reprise explicite d’une ingestion | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T058 | Tester le pipeline nominal avec les quatre services | Pipeline vérifié via HTTP et services réels |
| T059 | Tester doublons reprises et pannes partielles | Pannes Redis/Kafka/Mongo et rejeu testés ; injection à chaque écriture à compléter |
| T060 | Tester les erreurs DLQ et leur diagnostic | JSON illisible/DLQ testés ; panne publication DLQ à compléter |
| T061 | Créer la validation des périodes et tailles | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T062 | Créer le lecteur Cassandra par partitions | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T063 | Créer le curseur d’historique validé | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T064 | Créer le service et endpoint d’historique | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T065 | Créer l’endpoint de dernière mesure | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T066 | Créer les DTO et listes d’alertes | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T067 | Créer l’acquittement idempotent des alertes | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T068 | Créer les deux endpoints d’alertes | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T069 | Créer les statistiques bornées | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T070 | Tester les APIs de consultation sur données réelles | Consultation vérifiée par recette HTTP réelle |
| T071 | Configurer liveness readiness et dépendances | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T072 | Appliquer les limites HTTP et la configuration d’accès | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T073 | Ajouter les logs métier et métriques utiles | Logs structurés de phase et métriques Actuator génériques ; compteurs métier dédiés à compléter |
| T074 | Créer l’image Docker du backend | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T075 | Exécuter le backend dans Compose | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T076 | Créer le générateur de mesures par l’API | Implémenté dans la plateforme ; voir contrat, sources et résultats de recette |
| T077 | Vérifier la persistance et le redémarrage | Redémarrage Mongo et Java/outbox vérifiés ; volumes conservés |
| T078 | Documenter rétentions et récupération des erreurs | Rétention Kafka 24 h, registre/historique sans purge automatique, diagnostic documenté |
| T079 | Préparer la configuration de sauvegarde de développement | Scripts backup-dev/restore-dev ; restauration locale du jeu connu vérifiée |
| T080 | Vérifier l’arrêt gracieux et les délais | Arrêt HTTP gracieux, délai Spring 30 s et Compose 45 s ; reprise outbox testée |
| T081 | Créer l’environnement commun des tests d’intégration | Pile Compose de recette ; adaptation au lieu de Testcontainers |
| T082 | Uniformiser les tests unitaires et qualité de code | 26 tests Java exécutés, formatage cohérent ; rapport JaCoCo et règles de style automatisées à compléter |
| T083 | Finaliser la collection Postman complète | OpenAPI validé ; recette HTTP exécutée |
| T084 | Automatiser les attentes et l’exécution Postman | Collection Newman exécutée |
| T085 | Créer les tests de contrat des messages Kafka | Voir matrice réelle docs/resultats-recette.md |
| T086 | Créer les tests de disponibilité dégradée | Voir matrice réelle docs/resultats-recette.md |
| T087 | Tester la durabilité sous interruption du traitement | Redémarrage Java avec outbox en attente couvert ; interruptions à chaque écriture et panne DLQ à compléter |
| T088 | Tester la pagination et les limites de volume | Multijour, dates égales, curseurs et budget réel 10001 mesures vérifiés |
| T089 | Créer le parcours de recette fonctionnelle unique | Recette scripts et limites documentées |
| T090 | Créer la CI du backend sans déploiement VM | CI fournie, consulter Actions pour preuve distante |
| T091 | Rédiger le guide de démarrage depuis un clone neuf | Démarrage documenté ; second poste humain non testé |
| T092 | Réconcilier schémas index et accès applicatifs | Schémas/index documentés ; migrations Mongo rejouées |
| T093 | Réconcilier OpenAPI exemples et réponses réelles | OpenAPI et collection alignés sur l’implémentation |
| T094 | Vérifier les invariants de bout en bout | Invariants documentés et recette partielle explicitée |
| T095 | Effectuer la revue croisée des quatre contributions | Pas de revue croisée de quatre personnes réalisée |
| T096 | Exécuter la recette finale Postman et archiver les résultats | Recettes locales exécutées ; rapports distants dans Actions |
| T097 | Préparer les paramètres pour le déploiement futur | Variables et Compose deux VM fournis |
| T098 | Préparer la démonstration du backend | Générateur et démonstration frontend livrés |
| T099 | Taguer la livraison du backend et figer les dépendances | Pas de tag backend-v1.0.0 tant que toute la matrice du guide n’est pas satisfaite |
| T100 | Valider le passage au développement React | Frontend livré ; ne vaut pas validation de quatre membres |

## Éléments non déclarés terminés

Revue humaine à quatre, restauration hors machine, benchmark de charge, déploiement effectif sur deux VM et matrice exhaustive d’injection de panne restent à réaliser. Le tag de livraison prescrit par T099 n’est pas posé prématurément. Les résultats réellement exécutés sont dans [resultats-recette.md](resultats-recette.md).
