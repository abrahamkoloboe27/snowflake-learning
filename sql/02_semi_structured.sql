-- ShopFlow Day 2: Semi-structured data (JSON) & external table pattern
-- Co-authored with CoCo
-- ============================================================================
-- ShopFlow — Jour 2 : Data Lake, semi-structuré & external table
-- Fichier : 02_semi_structured.sql
-- Prérequis : 01_setup_and_ingest.sql exécuté (DB, schemas, WH, rôle, stage)
--
-- SCRIPT REJOUABLE : tables recréées via CREATE OR REPLACE (pas de doublons).
--
-- STRUCTURE RÉELLE DES FICHIERS (constatée sur échantillons) :
--   products.json   → ARRAY JSON  → nécessite STRIP_OUTER_ARRAY = TRUE
--                     champs : product_id, name, category, brand, price,
--                     attributes {color, warranty_months, weight_g},
--                     tags [array, parfois vide]  ← absent de l'énoncé !
--   web_events.json → JSON line-delimited (NDJSON)
--                     champs : event_id, user_id (nullable), session_id,
--                     event_type, product_id (nullable), timestamp ISO,
--                     device, context {user_agent, ip, referrer (nullable)}
--
-- POINT DE VIGILANCE (README) : les external tables Snowflake ne supportent
-- QUE les stages externes (S3/GCS/Azure). Notre stage étant interne, on
-- démontre le pattern data-lake équivalent : requête directe sur le fichier
-- en stage, sans chargement. La DDL external table est fournie (commentée)
-- pour le cas d'un bucket cloud.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 0. Contexte d'exécution
-- ----------------------------------------------------------------------------
USE ROLE SHOPFLOW_ENGINEER;
USE WAREHOUSE WH_INGEST;
USE DATABASE SHOPFLOW_DB;
USE SCHEMA RAW;

-- >>> UPLOAD PRÉALABLE <<<
-- Déposer products.json et web_events.json dans le dossier j1/ du stage :
--   Snowsight > Data > SHOPFLOW_DB > RAW > Stages > STAGE_LANDING
--   > + Files > chemin /j1
LIST @RAW.STAGE_LANDING/j1/;

-- ----------------------------------------------------------------------------
-- 1. File format JSON
--    STRIP_OUTER_ARRAY = TRUE : indispensable pour products.json (array →
--    1 ligne par élément). Sans effet sur le NDJSON de web_events.json,
--    donc un seul format suffit pour les deux fichiers.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT RAW.FF_JSON
  TYPE              = 'JSON'
  STRIP_OUTER_ARRAY = TRUE
  COMMENT = 'JSON : gère array (products) et line-delimited (web_events)';

-- ----------------------------------------------------------------------------
-- 2. Aperçus AVANT chargement (la leçon du Jour 1 !)
-- ----------------------------------------------------------------------------
SELECT t.$1 AS product_doc
FROM @RAW.STAGE_LANDING/j1/products.json (FILE_FORMAT => 'RAW.FF_JSON') t
LIMIT 5;

SELECT t.$1 AS event_doc
FROM @RAW.STAGE_LANDING/j1/web_events.json (FILE_FORMAT => 'RAW.FF_JSON') t
LIMIT 5;

-- ----------------------------------------------------------------------------
-- 3. PRODUCTS : chargement en VARIANT (zone RAW = données brutes)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE RAW.PRODUCTS_RAW (
  DATA          VARIANT,
  _LOADED_AT    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  _SOURCE_FILE  VARCHAR
);

-- NB : pas de VALIDATION_MODE ici (non supporté avec COPY transformé,
-- et un COPY JSON simple exige une table à colonne VARIANT unique).
-- L'aperçu de la section 2 sert de contrôle préalable.
COPY INTO RAW.PRODUCTS_RAW (DATA, _SOURCE_FILE)
FROM (
  SELECT t.$1, 'j1/products.json'
  FROM @RAW.STAGE_LANDING/j1/products.json t
)
FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_JSON')
ON_ERROR = 'ABORT_STATEMENT';

-- Volume attendu : ~500 lignes (1 par produit, grâce à STRIP_OUTER_ARRAY)
SELECT COUNT(*) AS NB_PRODUCTS FROM RAW.PRODUCTS_RAW;

-- ----------------------------------------------------------------------------
-- 4. Requêtes semi-structurées : notation data:champ::type
-- ----------------------------------------------------------------------------
-- 4.1 Champs simples + objet imbriqué "attributes"
SELECT
  DATA:product_id::VARCHAR              AS product_id,
  DATA:name::VARCHAR                    AS name,
  DATA:category::VARCHAR                AS category,
  DATA:brand::VARCHAR                   AS brand,
  DATA:price::NUMBER(10,2)              AS price,
  DATA:attributes:color::VARCHAR        AS color,
  DATA:attributes:warranty_months::NUMBER AS warranty_months,
  DATA:attributes:weight_g::NUMBER      AS weight_g
FROM RAW.PRODUCTS_RAW
LIMIT 10;

-- 4.2 Agrégat métier directement sur le VARIANT
SELECT
  DATA:category::VARCHAR       AS category,
  COUNT(*)                     AS nb_products,
  ROUND(AVG(DATA:price::NUMBER(10,2)), 2) AS avg_price
FROM RAW.PRODUCTS_RAW
GROUP BY 1
ORDER BY avg_price DESC;

-- 4.3 LATERAL FLATTEN : exploser l'array "tags"
--     OUTER => TRUE conserve les produits SANS tag (tags = []) — sans cette
--     option, ils disparaîtraient du résultat.
SELECT
  DATA:product_id::VARCHAR AS product_id,
  DATA:name::VARCHAR       AS name,
  f.value::VARCHAR         AS tag
FROM RAW.PRODUCTS_RAW,
     LATERAL FLATTEN(input => DATA:tags, OUTER => TRUE) f
LIMIT 20;

-- 4.4 Répartition des tags (ici sans OUTER : on ne compte que les tags réels)
SELECT f.value::VARCHAR AS tag, COUNT(*) AS nb_products
FROM RAW.PRODUCTS_RAW,
     LATERAL FLATTEN(input => DATA:tags) f
GROUP BY 1
ORDER BY nb_products DESC;

-- ----------------------------------------------------------------------------
-- 5. WEB EVENTS — pattern "data lake" : interroger SANS charger
-- ----------------------------------------------------------------------------
-- 5.A  LIMITATION SNOWFLAKE : CREATE EXTERNAL TABLE exige un stage EXTERNE
--      (S3 / GCS / Azure). Sur notre stage interne, la commande échouerait.
--      DDL de référence si un bucket cloud est disponible :
--
-- CREATE STAGE RAW.STAGE_EXT_EVENTS
--   URL = 's3://mon-bucket/shopflow/'
--   CREDENTIALS = (AWS_KEY_ID = '...' AWS_SECRET_KEY = '...')  -- ou STORAGE INTEGRATION
--   FILE_FORMAT = RAW.FF_JSON;
--
-- CREATE OR REPLACE EXTERNAL TABLE RAW.WEB_EVENTS_EXT
--   LOCATION = @RAW.STAGE_EXT_EVENTS
--   FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_JSON')
--   PATTERN = '.*web_events.*[.]json'
--   AUTO_REFRESH = FALSE;
-- ALTER EXTERNAL TABLE RAW.WEB_EVENTS_EXT REFRESH;
-- SELECT VALUE:event_type::VARCHAR, COUNT(*) FROM RAW.WEB_EVENTS_EXT GROUP BY 1;

-- 5.B  ÉQUIVALENT SUR STAGE INTERNE : requête directe sur le fichier.
--      Même principe qu'une external table : le fichier reste sur le stage,
--      aucune copie physique en table, parsing à la volée.
SELECT
  t.$1:event_id::VARCHAR            AS event_id,
  t.$1:user_id::VARCHAR             AS user_id,      -- parfois NULL
  t.$1:session_id::VARCHAR          AS session_id,
  t.$1:event_type::VARCHAR          AS event_type,
  t.$1:product_id::VARCHAR          AS product_id,   -- parfois NULL
  t.$1:timestamp::TIMESTAMP_NTZ     AS event_ts,
  t.$1:device::VARCHAR              AS device,
  t.$1:context:referrer::VARCHAR    AS referrer,     -- objet imbriqué
  t.$1:context:ip::VARCHAR          AS ip
FROM @RAW.STAGE_LANDING/j1/web_events.json (FILE_FORMAT => 'RAW.FF_JSON') t
LIMIT 10;

-- 5.C  BENCHMARK 1/2 — agrégation directement sur le fichier en stage
--      (noter la durée dans Query History : scan JSON complet, aucun pruning)
SELECT
  t.$1:event_type::VARCHAR AS event_type,
  COUNT(*)                 AS nb_events
FROM @RAW.STAGE_LANDING/j1/web_events.json (FILE_FORMAT => 'RAW.FF_JSON') t
GROUP BY 1
ORDER BY nb_events DESC;

-- ----------------------------------------------------------------------------
-- 6. Matérialisation : WEB_EVENTS_RAW (INSERT ... SELECT depuis le stage)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE RAW.WEB_EVENTS_RAW (
  DATA          VARIANT,
  _LOADED_AT    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  _SOURCE_FILE  VARCHAR
);

-- Équivalent du "INSERT ... SELECT depuis l'external table" de l'énoncé
-- (avec une vraie external table : SELECT VALUE FROM RAW.WEB_EVENTS_EXT)
INSERT INTO RAW.WEB_EVENTS_RAW (DATA, _SOURCE_FILE)
SELECT t.$1, 'j1/web_events.json'
FROM @RAW.STAGE_LANDING/j1/web_events.json (FILE_FORMAT => 'RAW.FF_JSON') t;

-- Volume attendu : ~200 000 lignes
SELECT COUNT(*) AS NB_EVENTS FROM RAW.WEB_EVENTS_RAW;

-- 6.B  BENCHMARK 2/2 — même agrégation sur la table matérialisée.
--      Comparer la durée avec 5.C dans Query History (Monitoring > Query
--      History) : stockage columnar + micro-partitions = nettement plus
--      rapide que le parsing du fichier à chaque requête.
SELECT
  DATA:event_type::VARCHAR AS event_type,
  COUNT(*)                 AS nb_events
FROM RAW.WEB_EVENTS_RAW
GROUP BY 1
ORDER BY nb_events DESC;

-- ----------------------------------------------------------------------------
-- 7. Vérifications & exploration qualité (prépare le nettoyage du Jour 3)
-- ----------------------------------------------------------------------------
-- Nulls à traiter en STAGING : user_id, product_id, referrer
SELECT
  COUNT(*)                                              AS total,
  COUNT_IF(DATA:user_id IS NULL OR DATA:user_id = 'null')     AS user_id_null,
  COUNT_IF(DATA:product_id IS NULL OR DATA:product_id = 'null') AS product_id_null,
  COUNT_IF(DATA:context:referrer IS NULL OR DATA:context:referrer = 'null') AS referrer_null
FROM RAW.WEB_EVENTS_RAW;

-- Répartition par device et par type d'événement
SELECT DATA:device::VARCHAR AS device, COUNT(*) AS nb
FROM RAW.WEB_EVENTS_RAW GROUP BY 1 ORDER BY nb DESC;

-- Bornes temporelles des événements
SELECT MIN(DATA:timestamp::TIMESTAMP_NTZ) AS first_event,
       MAX(DATA:timestamp::TIMESTAMP_NTZ) AS last_event
FROM RAW.WEB_EVENTS_RAW;

-- ----------------------------------------------------------------------------
-- 8. Fin de journée : suspendre les warehouses
-- ----------------------------------------------------------------------------
ALTER WAREHOUSE WH_INGEST    SUSPEND;
ALTER WAREHOUSE WH_TRANSFORM SUSPEND;