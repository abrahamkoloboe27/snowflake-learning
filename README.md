# ShopFlow — Pipeline analytique e-commerce end-to-end

**Projet fil rouge Snowflake**
**Durée : 5 jours**
**Prérequis : Badges Snowflake 1, 2 et 3 (Data Warehousing, Data Lake, Data Engineering)**

---

## 1. Contexte

**ShopFlow** est une jeune boutique e-commerce française spécialisée dans la vente de produits high-tech. L'entreprise génère chaque jour :

- des commandes clients (fichiers CSV exportés du back-office)
- un référentiel clients (fichier Parquet compressé mis à jour par le CRM)
- un catalogue produits (fichier JSON exporté du PIM)
- un flux d'événements web (fichier JSON semi-structuré : pages vues, clics, ajouts panier)

Jusqu'ici, ces données dorment dans un bucket. La direction souhaite les exploiter pour piloter le business et vous confie la mise en place de la **plateforme data Snowflake**.

## 2. Objectifs pédagogiques

À l'issue du projet, vous devez être capable de :

1. Concevoir une **architecture multicouche** (RAW → STAGING → MARTS) sur Snowflake
2. Dimensionner et gérer plusieurs **virtual warehouses** selon les workloads
3. Ingérer des données **structurées et semi-structurées** depuis un stage
4. Interroger des données via des **external tables**
5. Construire un pipeline **automatisé** avec Streams, Tasks et Dynamic Tables
6. Utiliser **Time Travel** pour restaurer une donnée
7. Restituer les résultats dans un **dashboard Snowsight**

## 3. Architecture cible

```
                        ┌──────────────────────────────┐
                        │   Stage (fichiers sources)   │
                        │  orders.csv                  │
                        │  customers.parquet           │
                        │  products.json               │
                        │  web_events.json             │
                        └──────────────┬───────────────┘
                                       │  COPY INTO / EXTERNAL TABLE
                                       ▼
                        ┌──────────────────────────────┐
                        │  SCHEMA : RAW                │
                        │  Tables brutes, VARIANT      │
                        │  Zone de landing             │
                        └──────────────┬───────────────┘
                                       │  STREAM (CDC)
                                       ▼
                        ┌──────────────────────────────┐
                        │  SCHEMA : STAGING            │
                        │  Nettoyage, typage,          │
                        │  parsing VARIANT → colonnes  │
                        └──────────────┬───────────────┘
                                       │  TASK planifiée
                                       ▼
                        ┌──────────────────────────────┐
                        │  SCHEMA : MARTS              │
                        │  Dynamic Tables agrégées :   │
                        │  - CA / jour                 │
                        │  - Top produits              │
                        │  - Cohortes clients          │
                        └──────────────┬───────────────┘
                                       │
                                       ▼
                        ┌──────────────────────────────┐
                        │  Dashboard Snowsight         │
                        └──────────────────────────────┘
```

## 4. Dataset fourni

Le dossier `data/` est organisé en deux lots correspondant à deux moments du projet :

```
data/
├── j1/    ← lot initial (ingéré aux Jours 1 et 2)
│   ├── customers.parquet
│   ├── products.json
│   ├── orders.csv
│   ├── order_items.csv
│   └── web_events.json
└── j2/    ← second lot (ingéré au Jour 3 pour déclencher Streams/Tasks)
    ├── orders_j2.csv
    ├── order_items_j2.csv
    └── web_events_j2.json
```

### Lot J1 — chargement initial (Jours 1 et 2)

| Fichier | Format | Volume | Contenu |
|---|---|---|---|
| `customers.parquet` | Parquet snappy | ~10 000 lignes | customer_id, email, first_name, last_name, city, signup_date |
| `products.json` | JSON (array) | ~500 lignes | product_id, name, category, brand, price, attributes (imbriqué) |
| `orders.csv` | CSV (headers, `,`) | ~50 000 lignes | order_id, customer_id, order_date, status, total_amount |
| `order_items.csv` | CSV | ~150 000 lignes | order_id, product_id, quantity, unit_price |
| `web_events.json` | JSON line-delimited | ~200 000 lignes | event_id, user_id, event_type, product_id, timestamp, session_id, device |

### Lot J2 — arrivage du "lendemain" (Jour 3)

Ces fichiers représentent les **nouvelles données** de la journée suivante et servent à démontrer que votre pipeline automatisé (Streams + Tasks) capte bien les changements. Vous les uploaderez au **Jour 3 uniquement**, une fois vos Streams et Tasks en place.

## 5. Étapes détaillées (5 jours)

### Jour 1 — Fondations & ingestion structurée

**Objectif :** poser l'infrastructure Snowflake et ingérer les données structurées.

1. Créer une base `SHOPFLOW_DB` avec trois schemas : `RAW`, `STAGING`, `MARTS`
2. Créer deux virtual warehouses :
   - `WH_INGEST` (XSMALL, auto-suspend 60s) pour les COPY
   - `WH_TRANSFORM` (SMALL, auto-suspend 60s) pour les Tasks
3. Créer un rôle applicatif `SHOPFLOW_ENGINEER` et donner les grants nécessaires
4. Créer un **stage interne** `RAW.STAGE_LANDING`
5. Uploader `orders.csv`, `order_items.csv`, `customers.parquet` via `PUT` (SnowSQL) ou l'UI Snowsight
6. Définir deux **file formats** : `FF_CSV_ORDERS`, `FF_PARQUET`
7. Créer les tables cibles dans `RAW` et exécuter les `COPY INTO`
8. Vérifier les compteurs (`SELECT COUNT(*)`) et les erreurs (`VALIDATION_MODE`)

**Livrable J1 :** script `01_setup_and_ingest.sql`

---

### Jour 2 — Data Lake : semi-structuré & external table

**Objectif :** ingérer JSON et interroger un fichier sans le charger.

1. Uploader `products.json` et `web_events.json` sur le stage
2. Créer un file format `FF_JSON`
3. Créer `RAW.PRODUCTS_RAW (data VARIANT)` et charger `products.json` avec `COPY INTO`
4. Requêter les champs imbriqués avec la notation `data:field::type`, `LATERAL FLATTEN`
5. Créer une **external table** `RAW.WEB_EVENTS_EXT` pointant sur `web_events.json` sur le stage (sans copie physique)
6. Comparer les performances : requête sur `WEB_EVENTS_EXT` vs table matérialisée
7. Créer `RAW.WEB_EVENTS_RAW` et faire un `INSERT ... SELECT` depuis l'external table

**Livrable J2 :** script `02_semi_structured.sql`

---

### Jour 3 — Pipeline automatisé : Streams & Tasks

**Objectif :** détecter les nouvelles données et les transformer automatiquement.

1. Créer un **Stream** sur chaque table RAW : `STR_ORDERS`, `STR_ORDER_ITEMS`, `STR_WEB_EVENTS`
2. Créer les tables cibles dans `STAGING` : `STG_ORDERS`, `STG_ORDER_ITEMS`, `STG_WEB_EVENTS`, `STG_PRODUCTS`, `STG_CUSTOMERS`
3. Écrire les requêtes de transformation (nettoyage nulls, typage dates, parsing VARIANT, jointure enrichissement)
4. Créer trois **Tasks** planifiées (toutes les 5 minutes) qui consomment les Streams :
   - `TSK_LOAD_STG_ORDERS`
   - `TSK_LOAD_STG_WEB_EVENTS`
   - `TSK_LOAD_STG_PRODUCTS`
5. Chaîner les tasks avec `AFTER` (une task root, des tasks dépendantes)
6. **Test dynamique :** uploader le lot J2 (`orders_j2.csv`, `web_events_j2.json`), charger dans RAW, observer les streams se remplir, attendre la task, vérifier STAGING

**Livrable J3 :** script `03_streams_tasks.sql`

---

### Jour 4 — Marts analytiques & Time Travel

**Objectif :** produire les indicateurs métier et démontrer la robustesse.

1. Créer trois **Dynamic Tables** dans `MARTS` (target_lag = 5 minutes) :
   - `DT_DAILY_REVENUE` : CA et nombre de commandes par jour
   - `DT_TOP_PRODUCTS` : top 20 produits par CA sur les 30 derniers jours
   - `DT_CUSTOMER_COHORTS` : rétention par mois d'inscription
2. Interroger les dynamic tables, vérifier qu'elles se rafraîchissent automatiquement
3. **Exercice Time Travel :**
   - Supprimer accidentellement 1 000 lignes de `RAW.ORDERS_RAW` (`DELETE ... WHERE ...`)
   - Restaurer via `SELECT ... AT (OFFSET => -60)` puis `INSERT`
   - OU utiliser `UNDROP` sur une table supprimée
4. Documenter la procédure de recovery dans le README

**Livrable J4 :** script `04_marts_and_timetravel.sql`

---

### Jour 5 — Dashboard, documentation & soutenance

**Objectif :** restituer et packager.

1. Créer un **dashboard Snowsight** avec 4 tuiles minimum :
   - Ligne : CA par jour (14 derniers jours)
   - Barres : Top 10 produits
   - Table : dernières commandes traitées
   - KPI : nombre de commandes du jour, panier moyen
2. Rédiger le **README.md** de votre dépôt Git avec :
   - Contexte et architecture (schéma)
   - Prérequis Snowflake (account, warehouse, role)
   - Instructions de reproduction (ordre d'exécution des scripts)
   - Screenshots du dashboard
   - Points de vigilance rencontrés
3. Structurer votre dépôt Git :
   ```
   shopflow/
   ├── README.md
   ├── data/                 # dataset synthétique
   ├── sql/
   │   ├── 01_setup_and_ingest.sql
   │   ├── 02_semi_structured.sql
   │   ├── 03_streams_tasks.sql
   │   └── 04_marts_and_timetravel.sql
   ├── screenshots/
   └── slides/               # support de soutenance
   ```
4. Préparer une **présentation de 10 minutes** : contexte, archi, démo live, difficultés, apprentissages

**Livrable J5 :** dépôt Git complet + slides + démo dashboard

## 6. Livrables attendus

À la fin du projet, vous remettez :

- Un **dépôt Git** public ou partagé (GitHub / GitLab) contenant les scripts SQL, le dataset, les screenshots et le README
- Un **dashboard Snowsight** fonctionnel dans votre compte (partage d'URL ou capture)
- Une **présentation orale de 10 minutes** avec démo live du pipeline (upload du lot J2 → observation des streams → rafraîchissement du dashboard)

## 7. Contraintes techniques

- Utiliser exclusivement les fonctionnalités vues dans les badges 1, 2 et 3
- Nommer les objets en `SNAKE_CASE` avec préfixes explicites (`RAW_`, `STG_`, `DT_`, `TSK_`, `STR_`)
- Toutes les warehouses doivent avoir `AUTO_SUSPEND` ≤ 60s (contrôle des coûts)
- Aucun mot de passe ou credential en clair dans les scripts commités
- **Suspendre manuellement vos warehouses en fin de journée** pour éviter la surconsommation de crédits

## 8. Bonus (optionnels)

Si vous avancez plus vite que prévu, essayez :

- Ajouter un **masking policy** sur `STG_CUSTOMERS.EMAIL` (RGPD)
- Créer une **row access policy** limitant l'accès aux commandes par région
- Écrire un **test de qualité** (procédure stored + task d'alerte)
- Connecter Snowflake à un outil externe (Power BI, Metabase, Tableau)

## 9. Ressources utiles

- Snowflake Docs — [Streams](https://docs.snowflake.com/en/user-guide/streams-intro)
- Snowflake Docs — [Tasks](https://docs.snowflake.com/en/user-guide/tasks-intro)
- Snowflake Docs — [Dynamic Tables](https://docs.snowflake.com/en/user-guide/dynamic-tables-about)
- Snowflake Docs — [Time Travel](https://docs.snowflake.com/en/user-guide/data-time-travel)
- Snowflake Quickstart — [Getting Started with Streams and Tasks](https://quickstarts.snowflake.com/guide/getting_started_with_streams_and_tasks/)
