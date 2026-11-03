# Développement Java local

La méthode recommandée du README exécute toute la pile dans Docker. Pour utiliser un IDE Java :

```bash
python3 scripts/init-env.py
./scripts/compose.sh up -d mongodb cassandra cassandra-init redis kafka kafka-init
# Attendre la fin réussie des services *-init et la santé des quatre services.
./scripts/migrate-mongo.sh
cd backend
./mvnw verify package
cd ..
JAVA_HOME=/chemin/vers/jdk21 python3 scripts/run-backend.py
```

Le lanceur lit `.env` sans imprimer ses secrets ni les passer dans les arguments Java. Les connexions locales utilisent les ports loopback. Si un backend Docker est déjà lancé sur 8080, l’arrêter avant de lancer le backend local.

Dans un autre terminal :

```bash
./scripts/seed-capteurs.sh
python3 scripts/recette.py
./scripts/run-postman.sh
cd frontend
npm ci
npm run dev
```

Le serveur Vite utilise le proxy `/api` vers localhost:8080. Les conteneurs utilisent les noms des services Compose. En déploiement deux VM, Java utilise les adresses privées configurées par Ansible.
