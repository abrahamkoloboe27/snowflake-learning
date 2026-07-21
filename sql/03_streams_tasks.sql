-- ShopFlow Day 3: Automated pipeline — Streams & Tasks
-- Co-authored with CoCo
-- ============================================================================
-- ShopFlow — Jour 3 : Pipeline automatisé (Streams & Tasks)
-- Fichier : 03_streams_tasks.sql
-- Prérequis : 01 et 02 exécutés → RAW contient déjà le lot J1
--
-- ORDRE DE GRANDEUR DES DÉCISIONS DE CONCEPTION (à défendre en soutenance) :
--
--  (a) 4 TASKS, pas 3. L'énoncé nomme 3 tasks mais 3 streams DONT
--      STR_ORDER_ITEMS. Un stream non consommé s'accumule sans fin. On ajoute
--      donc TSK_LOAD_STG_ORDER_ITEMS pour que le pipeline soit complet.
--
--  (b) STREAMS = INCRÉMENTAL, BACKFILL = INITIAL. J1 est déjà dans RAW ; un
--      stream créé maintenant ne verra que les changements POSTÉRIEURS (J2).
--      On backfill donc STAGING avec J1 (lecture directe de RAW), puis les
--      streams alimentent l'incrémental. Pas de double-comptage : le stream
--      ne contient pas J1. (En greenfield, on créerait les streams AVANT
--      tout chargement ; ici J1 précède, d'où le backfill.)
--
--  (c) PRODUCTS & CUSTOMERS = données de référence. Pas de stream (conforme à
--      l'énoncé : 3 streams). PRODUCTS est rafraîchi en full (INSERT OVERWRITE)
--      par la task racine ; CUSTOMERS est chargé une fois (snapshot CRM).
--
--  (d) NE PAS relancer le script 01 après ce point : recréer une table RAW
--      (CREATE OR REPLACE) invaliderait les streams posés dessus.
--
-- SCRIPT REJOUABLE : teardown des tasks + CREATE OR REPLACE des streams/tables.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 0. Privilège global EXECUTE TASK (obligatoire pour lancer des tasks)
--    À exécuter avec ACCOUNTADMIN — gotcha classique : sans ce grant, le
--    RESUME/EXECUTE des tasks échoue avec "insufficient privileges".
-- ----------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE SHOPFLOW_ENGINEER;

-- Contexte de travail
USE ROLE SHOPFLOW_ENGINEER;
USE WAREHOUSE WH_TRANSFORM;
USE DATABASE SHOPFLOW_DB;

-- ----------------------------------------------------------------------------
-- 1. Tables cibles STAGING (nettoyées / typées / enrichies)
-- ----------------------------------------------------------------------------
USE SCHEMA STAGING;

CREATE OR REPLACE TABLE STAGING.STG_CUSTOMERS (
  CUSTOMER_ID   VARCHAR,
  EMAIL         VARCHAR,
  FIRST_NAME    VARCHAR,
  LAST_NAME     VARCHAR,
  CITY          VARCHAR,
  SIGNUP_DATE   DATE,
  STG_LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE STAGING.STG_PRODUCTS (
  PRODUCT_ID       VARCHAR,
  NAME             VARCHAR,
  CATEGORY         VARCHAR,
  BRAND            VARCHAR,
  PRICE            NUMBER(10,2),
  COLOR            VARCHAR,
  WARRANTY_MONTHS  NUMBER,
  WEIGHT_G         NUMBER,
  NB_TAGS          NUMBER,
  STG_LOADED_AT    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE STAGING.STG_ORDERS (
  ORDER_ID      VARCHAR,
  CUSTOMER_ID   VARCHAR,
  ORDER_TS      TIMESTAMP_NTZ,   -- horodatage complet conservé
  ORDER_DATE    DATE,            -- dérivé, pour les agrégats du Jour 4
  STATUS        VARCHAR,
  TOTAL_AMOUNT  NUMBER(12,2),
  STG_LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE STAGING.STG_ORDER_ITEMS (
  ORDER_ID      VARCHAR,
  PRODUCT_ID    VARCHAR,
  QUANTITY      NUMBER,
  UNIT_PRICE    NUMBER(12,2),
  LINE_AMOUNT   NUMBER(14,2),    -- quantity * unit_price
  PRODUCT_NAME  VARCHAR,         -- enrichissement depuis STG_PRODUCTS
  CATEGORY      VARCHAR,         -- enrichissement
  BRAND         VARCHAR,         -- enrichissement
  STG_LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE STAGING.STG_WEB_EVENTS (
  EVENT_ID      VARCHAR,
  USER_ID       VARCHAR,         -- nullable dans la source
  IS_ANONYMOUS  BOOLEAN,         -- flag dérivé du null
  SESSION_ID    VARCHAR,
  EVENT_TYPE    VARCHAR,
  PRODUCT_ID    VARCHAR,         -- nullable dans la source
  EVENT_TS      TIMESTAMP_NTZ,
  EVENT_DATE    DATE,
  DEVICE        VARCHAR,
  REFERRER      VARCHAR,         -- COALESCE → 'direct' si null
  IP            VARCHAR,
  STG_LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ----------------------------------------------------------------------------
-- 2. Streams (CDC) sur les tables RAW à fort volume / changeantes
--    APPEND_ONLY = TRUE : nos tables RAW sont alimentées uniquement par COPY
--    (inserts). Un stream append-only est plus léger et suffit ici.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE STREAM RAW.STR_ORDERS
  ON TABLE RAW.ORDERS_RAW      APPEND_ONLY = TRUE
  COMMENT = 'CDC des nouvelles commandes';

CREATE OR REPLACE STREAM RAW.STR_ORDER_ITEMS
  ON TABLE RAW.ORDER_ITEMS_RAW APPEND_ONLY = TRUE
  COMMENT = 'CDC des nouvelles lignes de commande';

CREATE OR REPLACE STREAM RAW.STR_WEB_EVENTS
  ON TABLE RAW.WEB_EVENTS_RAW  APPEND_ONLY = TRUE
  COMMENT = 'CDC des nouveaux événements web';

-- À la création, les streams sont VIDES (J1 = état de base, pas un changement)
SELECT SYSTEM$STREAM_HAS_DATA('SHOPFLOW_DB.RAW.STR_ORDERS')      AS orders_has_data,
       SYSTEM$STREAM_HAS_DATA('SHOPFLOW_DB.RAW.STR_ORDER_ITEMS') AS items_has_data,
       SYSTEM$STREAM_HAS_DATA('SHOPFLOW_DB.RAW.STR_WEB_EVENTS')  AS events_has_data;

-- ----------------------------------------------------------------------------
-- 3. BACKFILL INITIAL : J1 (RAW → STAGING), lecture DIRECTE de RAW
--    Ne touche pas les tables RAW → n'alimente PAS les streams. Exécuté une
--    seule fois pour que STAGING contienne l'historique J1.
-- ----------------------------------------------------------------------------

-- 3.1 Référentiels d'abord (products avant order_items pour l'enrichissement)
INSERT OVERWRITE INTO STAGING.STG_PRODUCTS
  (PRODUCT_ID, NAME, CATEGORY, BRAND, PRICE, COLOR, WARRANTY_MONTHS, WEIGHT_G, NB_TAGS)
SELECT
  DATA:product_id::VARCHAR,
  DATA:name::VARCHAR,
  DATA:category::VARCHAR,
  DATA:brand::VARCHAR,
  DATA:price::NUMBER(10,2),
  DATA:attributes:color::VARCHAR,
  DATA:attributes:warranty_months::NUMBER,
  DATA:attributes:weight_g::NUMBER,
  COALESCE(ARRAY_SIZE(DATA:tags), 0)
FROM RAW.PRODUCTS_RAW;

INSERT OVERWRITE INTO STAGING.STG_CUSTOMERS
  (CUSTOMER_ID, EMAIL, FIRST_NAME, LAST_NAME, CITY, SIGNUP_DATE)
SELECT
  TRIM(CUSTOMER_ID),
  LOWER(TRIM(EMAIL)),
  FIRST_NAME,
  LAST_NAME,
  CITY,
  SIGNUP_DATE
FROM RAW.CUSTOMERS_RAW
WHERE CUSTOMER_ID IS NOT NULL;

-- 3.2 Commandes
INSERT INTO STAGING.STG_ORDERS
  (ORDER_ID, CUSTOMER_ID, ORDER_TS, ORDER_DATE, STATUS, TOTAL_AMOUNT)
SELECT
  TRIM(ORDER_ID),
  TRIM(CUSTOMER_ID),
  ORDER_DATE,
  ORDER_DATE::DATE,
  LOWER(TRIM(STATUS)),
  TOTAL_AMOUNT
FROM RAW.ORDERS_RAW
WHERE ORDER_ID IS NOT NULL;

-- 3.3 Lignes de commande (avec enrichissement produit)
INSERT INTO STAGING.STG_ORDER_ITEMS
  (ORDER_ID, PRODUCT_ID, QUANTITY, UNIT_PRICE, LINE_AMOUNT, PRODUCT_NAME, CATEGORY, BRAND)
SELECT
  TRIM(i.ORDER_ID),
  TRIM(i.PRODUCT_ID),
  i.QUANTITY,
  i.UNIT_PRICE,
  i.QUANTITY * i.UNIT_PRICE,
  p.NAME,
  p.CATEGORY,
  p.BRAND
FROM RAW.ORDER_ITEMS_RAW i
LEFT JOIN STAGING.STG_PRODUCTS p
  ON TRIM(i.PRODUCT_ID) = p.PRODUCT_ID
WHERE i.QUANTITY > 0 AND i.UNIT_PRICE >= 0;

-- 3.4 Événements web (parsing VARIANT + nettoyage nulls)
INSERT INTO STAGING.STG_WEB_EVENTS
  (EVENT_ID, USER_ID, IS_ANONYMOUS, SESSION_ID, EVENT_TYPE, PRODUCT_ID,
   EVENT_TS, EVENT_DATE, DEVICE, REFERRER, IP)
SELECT
  DATA:event_id::VARCHAR,
  DATA:user_id::VARCHAR,
  (DATA:user_id IS NULL)                            AS IS_ANONYMOUS,
  DATA:session_id::VARCHAR,
  LOWER(DATA:event_type::VARCHAR),
  DATA:product_id::VARCHAR,
  DATA:timestamp::TIMESTAMP_NTZ,
  DATA:timestamp::TIMESTAMP_NTZ::DATE,
  DATA:device::VARCHAR,
  COALESCE(DATA:context:referrer::VARCHAR, 'direct'),
  DATA:context:ip::VARCHAR
FROM RAW.WEB_EVENTS_RAW
WHERE DATA:event_id IS NOT NULL;

-- Contrôle du backfill
SELECT 'STG_CUSTOMERS'  AS T, COUNT(*) AS N FROM STAGING.STG_CUSTOMERS
UNION ALL SELECT 'STG_PRODUCTS',    COUNT(*) FROM STAGING.STG_PRODUCTS
UNION ALL SELECT 'STG_ORDERS',      COUNT(*) FROM STAGING.STG_ORDERS
UNION ALL SELECT 'STG_ORDER_ITEMS', COUNT(*) FROM STAGING.STG_ORDER_ITEMS
UNION ALL SELECT 'STG_WEB_EVENTS',  COUNT(*) FROM STAGING.STG_WEB_EVENTS;

-- ----------------------------------------------------------------------------
-- 4. Teardown des tasks (idempotence)
--    On suspend la racine (stoppe le graphe) puis on drop des feuilles vers la
--    racine. IF EXISTS → aucune erreur au premier passage.
-- ----------------------------------------------------------------------------
ALTER TASK IF EXISTS STAGING.TSK_LOAD_STG_PRODUCTS    SUSPEND;
DROP  TASK IF EXISTS STAGING.TSK_LOAD_STG_ORDER_ITEMS;
DROP  TASK IF EXISTS STAGING.TSK_LOAD_STG_WEB_EVENTS;
DROP  TASK IF EXISTS STAGING.TSK_LOAD_STG_ORDERS;
DROP  TASK IF EXISTS STAGING.TSK_LOAD_STG_PRODUCTS;

-- ----------------------------------------------------------------------------
-- 5. Tasks (graphe / DAG)
--
--   TSK_LOAD_STG_PRODUCTS  (RACINE, SCHEDULE 5 min, full refresh)
--        ├── TSK_LOAD_STG_ORDERS         (AFTER products, consomme STR_ORDERS)
--        │        └── TSK_LOAD_STG_ORDER_ITEMS (AFTER orders, enrichit)
--        └── TSK_LOAD_STG_WEB_EVENTS      (AFTER products, consomme le stream)
--
--   - Seule la RACINE porte un SCHEDULE ; les enfants utilisent AFTER.
--   - WHEN SYSTEM$STREAM_HAS_DATA(...) : la task est ignorée s'il n'y a rien à
--     traiter → économie de crédits. Une task ignorée est considérée terminée,
--     donc ses dépendantes évaluent quand même leur propre condition.
--   - METADATA$ACTION = 'INSERT' : on ne prend que les insertions du stream.
-- ----------------------------------------------------------------------------

-- 5.1 RACINE : rafraîchissement complet des produits (référence, sans stream)
CREATE OR REPLACE TASK STAGING.TSK_LOAD_STG_PRODUCTS
  WAREHOUSE = WH_TRANSFORM
  SCHEDULE  = '5 MINUTE'
  COMMENT   = 'Task racine : full refresh des produits, déclenche le graphe'
AS
  INSERT OVERWRITE INTO STAGING.STG_PRODUCTS
    (PRODUCT_ID, NAME, CATEGORY, BRAND, PRICE, COLOR, WARRANTY_MONTHS, WEIGHT_G, NB_TAGS)
  SELECT
    DATA:product_id::VARCHAR, DATA:name::VARCHAR, DATA:category::VARCHAR,
    DATA:brand::VARCHAR, DATA:price::NUMBER(10,2), DATA:attributes:color::VARCHAR,
    DATA:attributes:warranty_months::NUMBER, DATA:attributes:weight_g::NUMBER,
    COALESCE(ARRAY_SIZE(DATA:tags), 0)
  FROM RAW.PRODUCTS_RAW;

-- 5.2 Commandes (dépend de products)
CREATE OR REPLACE TASK STAGING.TSK_LOAD_STG_ORDERS
  WAREHOUSE = WH_TRANSFORM
  COMMENT = 'Consomme STR_ORDERS → STG_ORDERS'
  AFTER STAGING.TSK_LOAD_STG_PRODUCTS
  WHEN SYSTEM$STREAM_HAS_DATA('SHOPFLOW_DB.RAW.STR_ORDERS')
AS
  INSERT INTO STAGING.STG_ORDERS
    (ORDER_ID, CUSTOMER_ID, ORDER_TS, ORDER_DATE, STATUS, TOTAL_AMOUNT)
  SELECT
    TRIM(ORDER_ID), TRIM(CUSTOMER_ID), ORDER_DATE, ORDER_DATE::DATE,
    LOWER(TRIM(STATUS)), TOTAL_AMOUNT
  FROM RAW.STR_ORDERS
  WHERE METADATA$ACTION = 'INSERT' AND ORDER_ID IS NOT NULL;

-- 5.3 Lignes de commande (dépend d'orders ; enrichit via STG_PRODUCTS)
CREATE OR REPLACE TASK STAGING.TSK_LOAD_STG_ORDER_ITEMS
  WAREHOUSE = WH_TRANSFORM
  COMMENT = 'Consomme STR_ORDER_ITEMS → STG_ORDER_ITEMS (task ajoutée, cf. en-tête)'
  AFTER STAGING.TSK_LOAD_STG_ORDERS
  WHEN SYSTEM$STREAM_HAS_DATA('SHOPFLOW_DB.RAW.STR_ORDER_ITEMS')
AS
  INSERT INTO STAGING.STG_ORDER_ITEMS
    (ORDER_ID, PRODUCT_ID, QUANTITY, UNIT_PRICE, LINE_AMOUNT, PRODUCT_NAME, CATEGORY, BRAND)
  SELECT
    TRIM(i.ORDER_ID), TRIM(i.PRODUCT_ID), i.QUANTITY, i.UNIT_PRICE,
    i.QUANTITY * i.UNIT_PRICE, p.NAME, p.CATEGORY, p.BRAND
  FROM RAW.STR_ORDER_ITEMS i
  LEFT JOIN STAGING.STG_PRODUCTS p ON TRIM(i.PRODUCT_ID) = p.PRODUCT_ID
  WHERE i.METADATA$ACTION = 'INSERT' AND i.QUANTITY > 0 AND i.UNIT_PRICE >= 0;

-- 5.4 Événements web (dépend de products ; parsing VARIANT)
CREATE OR REPLACE TASK STAGING.TSK_LOAD_STG_WEB_EVENTS
  WAREHOUSE = WH_TRANSFORM
  COMMENT = 'Consomme STR_WEB_EVENTS → STG_WEB_EVENTS'
  AFTER STAGING.TSK_LOAD_STG_PRODUCTS
  WHEN SYSTEM$STREAM_HAS_DATA('SHOPFLOW_DB.RAW.STR_WEB_EVENTS')
AS
  INSERT INTO STAGING.STG_WEB_EVENTS
    (EVENT_ID, USER_ID, IS_ANONYMOUS, SESSION_ID, EVENT_TYPE, PRODUCT_ID,
     EVENT_TS, EVENT_DATE, DEVICE, REFERRER, IP)
  SELECT
    DATA:event_id::VARCHAR, DATA:user_id::VARCHAR, (DATA:user_id IS NULL),
    DATA:session_id::VARCHAR, LOWER(DATA:event_type::VARCHAR), DATA:product_id::VARCHAR,
    DATA:timestamp::TIMESTAMP_NTZ, DATA:timestamp::TIMESTAMP_NTZ::DATE,
    DATA:device::VARCHAR, COALESCE(DATA:context:referrer::VARCHAR, 'direct'),
    DATA:context:ip::VARCHAR
  FROM RAW.STR_WEB_EVENTS
  WHERE METADATA$ACTION = 'INSERT' AND DATA:event_id IS NOT NULL;

-- ----------------------------------------------------------------------------
-- 6. Activer le graphe (racine + toutes les dépendantes en un appel)
--    NB : les tasks sont créées SUSPENDUES par défaut.
-- ----------------------------------------------------------------------------
SELECT SYSTEM$TASK_DEPENDENTS_ENABLE('SHOPFLOW_DB.STAGING.TSK_LOAD_STG_PRODUCTS');

-- Vérifier l'état (doit afficher "started" pour les 4)
SHOW TASKS IN SCHEMA STAGING;

-- ============================================================================
-- 7. TEST DYNAMIQUE — arrivage du lot J2 (la démo de la soutenance)
-- ============================================================================

-- 7.1 Snapshot AVANT : compteurs STAGING de référence
SELECT 'AVANT' AS phase, 'STG_ORDERS' AS T, COUNT(*) AS N FROM STAGING.STG_ORDERS
UNION ALL SELECT 'AVANT','STG_ORDER_ITEMS', COUNT(*) FROM STAGING.STG_ORDER_ITEMS
UNION ALL SELECT 'AVANT','STG_WEB_EVENTS',  COUNT(*) FROM STAGING.STG_WEB_EVENTS;

-- 7.2 Uploader le lot J2 dans le dossier j2/ du stage, PUIS charger dans RAW.
--     Snowsight > Data > SHOPFLOW_DB > RAW > Stages > STAGE_LANDING > + Files
--     (chemin /j2). Fichiers : orders_j2.csv, order_items_j2.csv, web_events_j2.json
USE WAREHOUSE WH_INGEST;
LIST @RAW.STAGE_LANDING/j2/;

-- Ces COPY insèrent dans RAW → alimentent automatiquement les streams
COPY INTO RAW.ORDERS_RAW (ORDER_ID, CUSTOMER_ID, ORDER_DATE, STATUS, TOTAL_AMOUNT, _SOURCE_FILE)
FROM (
  SELECT t.$1::VARCHAR, t.$2::VARCHAR, t.$3::TIMESTAMP_NTZ, t.$4::VARCHAR,
         t.$5::NUMBER(12,2), 'j2/orders_j2.csv'
  FROM @RAW.STAGE_LANDING/j2/orders_j2.csv t
)
FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_CSV_ORDERS')
ON_ERROR = 'ABORT_STATEMENT';

COPY INTO RAW.ORDER_ITEMS_RAW (ORDER_ID, PRODUCT_ID, QUANTITY, UNIT_PRICE, _SOURCE_FILE)
FROM (
  SELECT t.$1::VARCHAR, t.$2::VARCHAR, t.$3::NUMBER, t.$4::NUMBER(12,2),
         'j2/order_items_j2.csv'
  FROM @RAW.STAGE_LANDING/j2/order_items_j2.csv t
)
FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_CSV_ORDERS')
ON_ERROR = 'ABORT_STATEMENT';

COPY INTO RAW.WEB_EVENTS_RAW (DATA, _SOURCE_FILE)
FROM (
  SELECT t.$1, 'j2/web_events_j2.json'
  FROM @RAW.STAGE_LANDING/j2/web_events_j2.json t
)
FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_JSON')
ON_ERROR = 'ABORT_STATEMENT';

-- 7.3 Observer les streams SE REMPLIR (SELECT/COUNT ne consomme PAS le stream)
SELECT SYSTEM$STREAM_HAS_DATA('SHOPFLOW_DB.RAW.STR_ORDERS')      AS orders_has_data,
       SYSTEM$STREAM_HAS_DATA('SHOPFLOW_DB.RAW.STR_ORDER_ITEMS') AS items_has_data,
       SYSTEM$STREAM_HAS_DATA('SHOPFLOW_DB.RAW.STR_WEB_EVENTS')  AS events_has_data;

SELECT 'STR_ORDERS'      AS stream, COUNT(*) AS nb_changes FROM RAW.STR_ORDERS
UNION ALL SELECT 'STR_ORDER_ITEMS', COUNT(*) FROM RAW.STR_ORDER_ITEMS
UNION ALL SELECT 'STR_WEB_EVENTS',  COUNT(*) FROM RAW.STR_WEB_EVENTS;

-- 7.4 Déclencher le graphe IMMÉDIATEMENT (démo live sans attendre 5 min).
--     EXECUTE TASK sur la racine lance tout le graphe de façon asynchrone.
EXECUTE TASK STAGING.TSK_LOAD_STG_PRODUCTS;

-- 7.5 Suivre l'exécution (relancer jusqu'à SUCCEEDED pour les 4 tasks)
SELECT NAME, STATE, SCHEDULED_TIME, COMPLETED_TIME, ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
  SCHEDULED_TIME_RANGE_START => DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
))
ORDER BY SCHEDULED_TIME DESC
LIMIT 20;

-- 7.6 Snapshot APRÈS : les compteurs ont augmenté du volume J2, et les streams
--     sont de nouveau vides (consommés par les tasks).
SELECT 'APRES' AS phase, 'STG_ORDERS' AS T, COUNT(*) AS N FROM STAGING.STG_ORDERS
UNION ALL SELECT 'APRES','STG_ORDER_ITEMS', COUNT(*) FROM STAGING.STG_ORDER_ITEMS
UNION ALL SELECT 'APRES','STG_WEB_EVENTS',  COUNT(*) FROM STAGING.STG_WEB_EVENTS;

SELECT SYSTEM$STREAM_HAS_DATA('SHOPFLOW_DB.RAW.STR_ORDERS')     AS orders_has_data_apres,
       SYSTEM$STREAM_HAS_DATA('SHOPFLOW_DB.RAW.STR_WEB_EVENTS') AS events_has_data_apres;

-- ----------------------------------------------------------------------------
-- 8. Fin de journée : suspendre le graphe (stoppe les runs toutes les 5 min)
--    + suspendre les warehouses (bloc tolérant à l'état déjà suspendu).
--    >>> Avant la soutenance : réactiver via SYSTEM$TASK_DEPENDENTS_ENABLE. <<<
-- ----------------------------------------------------------------------------
ALTER TASK IF EXISTS STAGING.TSK_LOAD_STG_PRODUCTS SUSPEND;  -- suspend la racine = stoppe le graphe

EXECUTE IMMEDIATE $$
DECLARE
  result STRING DEFAULT '';
BEGIN
  BEGIN
    ALTER WAREHOUSE WH_INGEST SUSPEND;
    result := 'WH_INGEST : suspendu';
  EXCEPTION WHEN OTHER THEN result := 'WH_INGEST : déjà suspendu';
  END;
  BEGIN
    ALTER WAREHOUSE WH_TRANSFORM SUSPEND;
    result := result || ' | WH_TRANSFORM : suspendu';
  EXCEPTION WHEN OTHER THEN result := result || ' | WH_TRANSFORM : déjà suspendu';
  END;
  RETURN result;
END;
$$;SHOPFLOW_DB.STAGING.TSK_LOAD_STG_ORDERS