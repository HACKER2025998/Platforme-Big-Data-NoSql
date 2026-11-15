# Kafka local (KRaft, un seul nœud)

| Usage | Adresse |
|---|---|
| Depuis le poste (Spring Boot) | `localhost:9092` |
| Depuis le réseau Compose | `kafka:29092` |
| Contrôleur (interne) | `kafka:9093` |

Démarrage : `docker compose -f infra/compose.dev.yaml up -d kafka`
