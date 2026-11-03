# Versions et images vérifiées

Images téléchargées et utilisées le 3 novembre 2026 ; digests issus des résolutions Docker locales. Le code épingle les mêmes références.

| Image | Digest SHA256 |
| --- | --- |
| `mongo:8.0.32` | `d0d926f94df099bff534b7ee5b5986458131a22489dfff8664509af0c1e2ca9c` |
| `cassandra:5.0.9` | `ae4cc2e4b62a5b2a2dd1645ba827cb50c589cc7531bcffe8b8b60ac3b1adc79d` |
| `redis:7.4.12-alpine` | `8da19c7399dca900c6822a1cf30f9d2285585e2c4b457ff4095b0f4500054d3f` |
| `apache/kafka:4.0.2` | `836cafdad9f4825880d7cf1d5a21202915ae2527bd0ef1c3600c526ed7814d1f` |
| `python:3.13-slim` | `70729b46c69b4f1e97c4822c1af3df53a1476cf5ddc6c087c0c10bc3a5678c2f` |
| `maven:3.9.11-eclipse-temurin-21` | `6fdc855a6ed81d288ca7ca37ac6ff5e9308b612485c0801d70b25a858c83d237` |
| `eclipse-temurin:21-jre` | `cff19e6215689161eb6162c11b86b0c60ddf802164f2eaf48d570f8fb79a36c5` |
| `node:24.11.0-alpine` | `f36fed0b2129a8492535e2853c64fbdbd2d29dc1219ee3217023ca48aebd3787` |
| `nginx:1.28-alpine` | `a8b39bd9cf0f83869a2162827a0caf6137ddf759d50a171451b335cecc87d236` |

Java : 21. Spring Boot : 4.0.9 et dépendances gérées par son BOM. Maven Wrapper : 3.9.11. React : 19.2.0. Vite : 7.2.2. Newman : 6.2.1. Ansible collection community.docker : 4.8.1. Dépendances JavaScript exactes dans `frontend/package-lock.json`.

Les digests applicatifs de publication sont produits par GHCR après validation ; les images applicatives locales ne constituent pas une publication GHCR.

## Compatibilité du noyau MongoDB

Le runner GitHub a rencontré l’erreur fatale MongoDB 12257600 sur son noyau récent. Le [ticket MongoDB officiel SERVER-136848](https://jira.mongodb.org/browse/SERVER-136848) décrit le contrôle de version des noyaux Ubuntu ; les [notes MongoDB](https://www.mongodb.com/docs/v8.0/release-notes/8.0/) décrivent l’incompatibilité TCMalloc/rseq.

Les Compose définissent `GLIBC_TUNABLES=glibc.pthread.rseq=1` pour MongoDB. Ce réglage laisse glibc enregistrer rseq et empêche TCMalloc d’activer ses caches par CPU ; l’allocateur utilise ses caches par thread. Cette déduction suit le fonctionnement décrit par la [documentation officielle TCMalloc de MongoDB](https://www.mongodb.com/docs/manual/administration/tcmalloc-performance/) et le [suivi de l’image officielle](https://github.com/docker-library/mongo/issues/765). Le réglage doit être réévalué lors d’une mise à jour de MongoDB ou du noyau ; aucune performance de production n’est revendiquée.
