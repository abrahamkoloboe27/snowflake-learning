-- ============================================================================
-- ShopFlow — Jour 1 : Fondations & ingestion structurée
-- Fichier : 01_setup_and_ingest.sql
-- Prérequis : rôle ACCOUNTADMIN (ou SYSADMIN + SECURITYADMIN) pour le setup
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 0. Contexte d'exécution
-- ----------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

-- ----------------------------------------------------------------------------
-- 1. Base de données et schemas (architecture multicouche)
-- ----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS SHOPFLOW_DB
  COMMENT = 'Plateforme analytique e-commerce ShopFlow';

CREATE SCHEMA IF NOT EXISTS SHOPFLOW_DB.RAW
  COMMENT = 'Zone de landing : données brutes, non transformées';

CREATE SCHEMA IF NOT EXISTS SHOPFLOW_DB.STAGING
  COMMENT = 'Zone de nettoyage : typage, parsing, enrichissement';

CREATE SCHEMA IF NOT EXISTS SHOPFLOW_DB.MARTS
  COMMENT = 'Zone analytique : agrégats métier (dynamic tables)';

-- ----------------------------------------------------------------------------
-- 2. Virtual warehouses (contrainte : AUTO_SUSPEND <= 60s)
-- ----------------------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS WH_INGEST
  WAREHOUSE_SIZE      = 'XSMALL'
  AUTO_SUSPEND        = 60
  AUTO_RESUME         = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Warehouse dédié aux COPY INTO (ingestion)';

CREATE WAREHOUSE IF NOT EXISTS WH_TRANSFORM
  WAREHOUSE_SIZE      = 'SMALL'
  AUTO_SUSPEND        = 60
  AUTO_RESUME         = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Warehouse dédié aux Tasks de transformation';

-- ----------------------------------------------------------------------------
-- 3. Rôle applicatif SHOPFLOW_ENGINEER + grants
-- ----------------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS SHOPFLOW_ENGINEER
  COMMENT = 'Rôle applicatif du projet ShopFlow';

-- Rattacher le rôle à la hiérarchie (bonne pratique : sous SYSADMIN)
GRANT ROLE SHOPFLOW_ENGINEER TO ROLE SYSADMIN;

-- Se donner le rôle à soi-même (remplacer par votre username)
GRANT ROLE SHOPFLOW_ENGINEER TO USER PCBCYYR-FHB82490;

-- Droits sur les warehouses
GRANT USAGE, OPERATE ON WAREHOUSE WH_INGEST    TO ROLE SHOPFLOW_ENGINEER;
GRANT USAGE, OPERATE ON WAREHOUSE WH_TRANSFORM TO ROLE SHOPFLOW_ENGINEER;

-- Droits sur la base et les schemas
GRANT USAGE ON DATABASE SHOPFLOW_DB TO ROLE SHOPFLOW_ENGINEER;
GRANT ALL PRIVILEGES ON SCHEMA SHOPFLOW_DB.RAW     TO ROLE SHOPFLOW_ENGINEER;
GRANT ALL PRIVILEGES ON SCHEMA SHOPFLOW_DB.STAGING TO ROLE SHOPFLOW_ENGINEER;
GRANT ALL PRIVILEGES ON SCHEMA SHOPFLOW_DB.MARTS   TO ROLE SHOPFLOW_ENGINEER;

-- Droits sur les objets futurs (évite de re-granter à chaque création)
GRANT ALL ON FUTURE TABLES IN SCHEMA SHOPFLOW_DB.RAW     TO ROLE SHOPFLOW_ENGINEER;
GRANT ALL ON FUTURE TABLES IN SCHEMA SHOPFLOW_DB.STAGING TO ROLE SHOPFLOW_ENGINEER;
GRANT ALL ON FUTURE TABLES IN SCHEMA SHOPFLOW_DB.MARTS   TO ROLE SHOPFLOW_ENGINEER;

-- ----------------------------------------------------------------------------
-- 4. Contexte de travail : on bascule sur le rôle applicatif
-- ----------------------------------------------------------------------------
USE ROLE SHOPFLOW_ENGINEER;
USE WAREHOUSE WH_INGEST;
USE DATABASE SHOPFLOW_DB;
USE SCHEMA RAW;

-- ----------------------------------------------------------------------------
-- 5. Stage interne de landing
-- ----------------------------------------------------------------------------
CREATE STAGE IF NOT EXISTS RAW.STAGE_LANDING
  COMMENT = 'Stage interne recevant les fichiers sources (lots J1 et J2)';

-- >>> UPLOAD DES FICHIERS <<<
-- Option A — SnowSQL (depuis votre machine, dans le dossier data/j1) :
--   PUT file://orders.csv          @SHOPFLOW_DB.RAW.STAGE_LANDING AUTO_COMPRESS=TRUE;
--   PUT file://order_items.csv     @SHOPFLOW_DB.RAW.STAGE_LANDING AUTO_COMPRESS=TRUE;
--   PUT file://customers.parquet   @SHOPFLOW_DB.RAW.STAGE_LANDING AUTO_COMPRESS=FALSE;
--   (Parquet est déjà compressé snappy : ne pas re-compresser)
--
-- Option B — Snowsight UI :
--   Data > Databases > SHOPFLOW_DB > RAW > Stages > STAGE_LANDING > + Files

-- Vérifier la présence des fichiers sur le stage
LIST @RAW.STAGE_LANDING;

-- ----------------------------------------------------------------------------
-- 6. File formats
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT RAW.FF_CSV_ORDERS
  TYPE                         = 'CSV'
  FIELD_DELIMITER              = ','
  SKIP_HEADER                  = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF                      = ('', 'NULL', 'null')
  EMPTY_FIELD_AS_NULL          = TRUE
  ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
  COMMENT = 'Format CSV avec headers pour orders et order_items';

CREATE OR REPLACE FILE FORMAT RAW.FF_PARQUET
  TYPE = 'PARQUET'
  COMMENT = 'Format Parquet (compression snappy) pour customers';

-- ----------------------------------------------------------------------------
-- 7. Tables cibles RAW + COPY INTO
-- ----------------------------------------------------------------------------

-- 7.1 ORDERS ------------------------------------------------------------------
CREATE OR REPLACE TABLE RAW.ORDERS_RAW (
  ORDER_ID      NUMBER,
  CUSTOMER_ID   NUMBER,
  ORDER_DATE    DATE,
  STATUS        VARCHAR,
  TOTAL_AMOUNT  NUMBER(12,2),
  -- Métadonnées d'ingestion (utiles pour le debug et l'audit)
  _LOADED_AT    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  _SOURCE_FILE  VARCHAR
);

-- Contrôle préalable : valider le fichier SANS charger (détecte les erreurs)
COPY INTO RAW.ORDERS_RAW (ORDER_ID, CUSTOMER_ID, ORDER_DATE, STATUS, TOTAL_AMOUNT)
FROM @RAW.STAGE_LANDING
FILES = ('orders.csv.gz')          -- .gz si uploadé avec AUTO_COMPRESS=TRUE
FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_CSV_ORDERS')
VALIDATION_MODE = 'RETURN_ERRORS';

-- Chargement réel (avec capture du nom de fichier source)
COPY INTO RAW.ORDERS_RAW (ORDER_ID, CUSTOMER_ID, ORDER_DATE, STATUS, TOTAL_AMOUNT, _SOURCE_FILE)
FROM (
  SELECT $1, $2, $3, $4, $5, METADATA$FILENAME
  FROM @RAW.STAGE_LANDING
)
FILES = ('orders.csv.gz')
FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_CSV_ORDERS')
ON_ERROR = 'ABORT_STATEMENT';

-- 7.2 ORDER_ITEMS -------------------------------------------------------------
CREATE OR REPLACE TABLE RAW.ORDER_ITEMS_RAW (
  ORDER_ID      NUMBER,
  PRODUCT_ID    NUMBER,
  QUANTITY      NUMBER,
  UNIT_PRICE    NUMBER(12,2),
  _LOADED_AT    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  _SOURCE_FILE  VARCHAR
);

COPY INTO RAW.ORDER_ITEMS_RAW (ORDER_ID, PRODUCT_ID, QUANTITY, UNIT_PRICE)
FROM @RAW.STAGE_LANDING
FILES = ('order_items.csv.gz')
FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_CSV_ORDERS')
VALIDATION_MODE = 'RETURN_ERRORS';

COPY INTO RAW.ORDER_ITEMS_RAW (ORDER_ID, PRODUCT_ID, QUANTITY, UNIT_PRICE, _SOURCE_FILE)
FROM (
  SELECT $1, $2, $3, $4, METADATA$FILENAME
  FROM @RAW.STAGE_LANDING
)
FILES = ('order_items.csv.gz')
FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_CSV_ORDERS')
ON_ERROR = 'ABORT_STATEMENT';

-- 7.3 CUSTOMERS (Parquet) -----------------------------------------------------
-- Astuce : avec Parquet, on peut inspecter le schéma avant de créer la table
SELECT $1
FROM @RAW.STAGE_LANDING/customers.parquet
(FILE_FORMAT => 'RAW.FF_PARQUET')
LIMIT 5;

CREATE OR REPLACE TABLE RAW.CUSTOMERS_RAW (
  CUSTOMER_ID   NUMBER,
  EMAIL         VARCHAR,
  FIRST_NAME    VARCHAR,
  LAST_NAME     VARCHAR,
  CITY          VARCHAR,
  SIGNUP_DATE   DATE,
  _LOADED_AT    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  _SOURCE_FILE  VARCHAR
);

-- Parquet est colonnaire : on extrait les champs depuis $1 (VARIANT)
COPY INTO RAW.CUSTOMERS_RAW (CUSTOMER_ID, EMAIL, FIRST_NAME, LAST_NAME, CITY, SIGNUP_DATE, _SOURCE_FILE)
FROM (
  SELECT
    $1:customer_id::NUMBER,
    $1:email::VARCHAR,
    $1:first_name::VARCHAR,
    $1:last_name::VARCHAR,
    $1:city::VARCHAR,
    $1:signup_date::DATE,
    METADATA$FILENAME
  FROM @RAW.STAGE_LANDING/customers.parquet
)
FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_PARQUET')
ON_ERROR = 'ABORT_STATEMENT';

-- ----------------------------------------------------------------------------
-- 8. Vérifications post-chargement
-- ----------------------------------------------------------------------------
-- Volumes attendus : ~50 000 / ~150 000 / ~10 000
SELECT 'ORDERS_RAW'      AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM RAW.ORDERS_RAW
UNION ALL
SELECT 'ORDER_ITEMS_RAW', COUNT(*) FROM RAW.ORDER_ITEMS_RAW
UNION ALL
SELECT 'CUSTOMERS_RAW',   COUNT(*) FROM RAW.CUSTOMERS_RAW;

-- Historique des chargements (statuts, lignes chargées, erreurs)
SELECT FILE_NAME, STATUS, ROW_COUNT, ROW_PARSED, FIRST_ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => 'RAW.ORDERS_RAW',
  START_TIME => DATEADD(HOUR, -2, CURRENT_TIMESTAMP())
));

-- Contrôles de cohérence rapides
SELECT MIN(ORDER_DATE), MAX(ORDER_DATE) FROM RAW.ORDERS_RAW;
SELECT STATUS, COUNT(*) FROM RAW.ORDERS_RAW GROUP BY STATUS;
SELECT COUNT(*) AS ORPHAN_ITEMS
FROM RAW.ORDER_ITEMS_RAW i
LEFT JOIN RAW.ORDERS_RAW o USING (ORDER_ID)
WHERE o.ORDER_ID IS NULL;

-- ----------------------------------------------------------------------------
-- 9. Fin de journée : suspendre les warehouses (contrainte du projet)
-- ----------------------------------------------------------------------------
ALTER WAREHOUSE WH_INGEST    SUSPEND;
ALTER WAREHOUSE WH_TRANSFORM SUSPEND;