# Règles métier et limites (T003)

Document de référence validé par les quatre membres. Toute modification passe par une PR relue.

## 1. Capteur

| Champ | Règle |
| --- | --- |
| `id` | 1 à 64 caractères, uniquement `A-Z`, `0-9`, `-`, `_` (regex `^[A-Z0-9_-]{1,64}$`). Majuscules obligatoires, pas de normalisation silencieuse. |
| `libelle` | Chaîne non vide, 120 caractères maximum. |
| `salle` | Chaîne non vide, 120 caractères maximum. |
| `seuilTemperature` | Décimal fini, entre -50 et 100 inclus. |
| `actif` | Booléen. |

- Création avec un `id` existant : **409**.
- Modification avec une version obsolète : **409**.
- Aucune suppression physique dans cette version : un capteur se désactive (`actif=false`).

## 2. Mesure

| Champ | Règle |
| --- | --- |
| `eventId` | UUID, stocké et comparé en forme canonique (minuscules). |
| `capteurId` | Même format qu'un identifiant de capteur. |
| `date` | UTC, normalisée à la milliseconde. |
| `temperature` | Décimal fini (ni NaN ni infini), entre -50 et 100 **inclus**. |

### Dates
- Les mesures historiques sont acceptées.
- Une date postérieure à `maintenant + 5 minutes` est refusée (400). Exactement `maintenant + 5 min` est accepté.
- Une date antérieure au `2000-01-01T00:00:00Z` est refusée (400), pour préserver l'ordre lexical de `sortKey` (epoch positif sur largeur fixe).
- Toutes les dates sont en UTC ; le `jour` de partition Cassandra est la date UTC.

## 3. Capteur actif et inactif

- **Actif** : accepte de nouvelles mesures.
- **Inactif** : refuse de nouvelles mesures (**409**). Il reste lisible (fiche, historique, alertes déjà créées).
- **Inconnu** : nouvelle mesure refusée (**404**).

## 4. Alerte

- Une alerte est créée **seulement si** `temperature > seuilApplique` (strictement supérieur).
- `temperature == seuilApplique` : **pas d'alerte**.
- `seuilApplique` et `acceptedAt` sont figés à la **première admission** de l'événement. Un changement ultérieur du seuil du capteur ne change pas le résultat d'un rejeu.
- Une alerte a pour identifiant l'`eventId` : un rejeu ne crée jamais une deuxième alerte.
- Un acquittement déjà fait est conservé après répétition de l'acquittement et après rejeu Kafka.

## 5. Doublons et rejeux

- Même `eventId`, **contenu identique** (`capteurId`, `date` normalisée, `temperature`) : **202** avec le même reçu, sans nouvelle écriture.
- Même `eventId`, **contenu différent** : **409**, l'événement existant n'est pas modifié.
- `eventId` différents avec mêmes `capteurId`, `date` et `temperature` : deux mesures distinctes (la clé d'historique contient l'`eventId`).

## 6. Exemples limites à valider par les quatre membres

| # | Cas | Résultat attendu |
| --- | --- | --- |
| 1 | Température = -50 | Acceptée |
| 2 | Température = -50.1 | 400 |
| 3 | Température = 100 | Acceptée |
| 4 | Température = 100.1 | 400 |
| 5 | Température = seuil (ex. 40 pour seuil 40) | Acceptée, **pas d'alerte** |
| 6 | Température = 40.1 pour seuil 40 | Acceptée, **alerte** |
| 7 | Seuil du capteur = 100.1 | 400 |
| 8 | Capteur inactif, nouvelle mesure | **409** |
| 9 | Capteur inconnu, nouvelle mesure | **404** |
| 10 | Date = maintenant + 5 min | Acceptée |
| 11 | Date = maintenant + 5 min + 1 ms | 400 |
| 12 | Date = 1999-12-31T23:59:59.999Z | 400 |
| 13 | Date historique (ex. 2026-01-01T00:00:00Z) | Acceptée |
| 14 | Rejeu identique d'un `eventId` | 202, même reçu, une seule alerte |
| 15 | Rejeu du même `eventId` avec une autre température | 409 |
| 16 | `id` capteur `salle-01` (minuscules) | 400 |
| 17 | `id` capteur de 65 caractères | 400 |
| 18 | Création d'un capteur déjà existant | 409 |
