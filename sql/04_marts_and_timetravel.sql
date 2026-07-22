-- ShopFlow Day 4: Analytical marts (Dynamic Tables) & Time Travel
-- Co-authored with CoCo
-- ============================================================================
-- ShopFlow — Jour 4 : Marts analytiques & Time Travel
-- Fichier : 04_marts_and_timetravel.sql
-- Prérequis : 01→03 exécutés. STAGING contient J1 (backfill) + J2 (pipeline).
--
-- NOTES DE CONCEPTION (à défendre en soutenance) :
--
--  (a) FENÊTRE "30 DERNIERS JOURS" ANCRÉE SUR LES DONNÉES, pas sur CURRENT_DATE.
--      Le dataset est synthétique (commandes fév.→juin 2026). Un filtre relatif
--      à aujourd'hui (22/07/2026) ne renverrait quasi rien. On ancre donc sur
--      MAX(ORDER_DATE) → le top produits reste pertinent et la fenêtre suit
--      automatiquement les nouvelles données.
--
--  (b) MODE DE RAFRAÎCHISSEMENT. DT_DAILY_REVENUE (GROUP BY + SUM/COUNT) peut se
--      rafraîchir en INCRÉMENTAL. DT_TOP_PRODUCTS (fonction fenêtre + sous-
--      requête) et DT_CUSTOMER_COHORTS (CTE + DISTINCT) basculent probablement
--      en FULL refresh — sans impact sur l'exactitude.
--
--  (c) TIME TRAVEL & STREAM APPEND-ONLY : voir section 6, la restauration
--      réinsère des lignes que le stream recapterait → on réinitialise le stream.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 0. Contexte
-- ----------------------------------------------------------------------------
USE ROLE SHOPFLOW_ENGINEER;
USE WAREHOUSE WH_TRANSFORM;
USE DATABASE SHOPFLOW_DB;
USE SCHEMA MARTS;

-- ----------------------------------------------------------------------------
-- 1. DT_DAILY_REVENUE — CA et nombre de commandes par jour
--    (CA calculé sur TOUTES les commandes ; pour un CA net, ajouter
--     WHERE STATUS = 'completed' — à adapter selon les statuts réels.)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE MARTS.DT_DAILY_REVENUE
  TARGET_LAG = '5 minutes'
  WAREHOUSE  = WH_TRANSFORM
  COMMENT    = 'CA et nombre de commandes par jour'
AS
SELECT
  ORDER_DATE,
  COUNT(DISTINCT ORDER_ID)                                          AS NB_ORDERS,
  ROUND(SUM(TOTAL_AMOUNT), 2)                                       AS REVENUE,
  ROUND(SUM(TOTAL_AMOUNT) / NULLIF(COUNT(DISTINCT ORDER_ID), 0), 2) AS AVG_BASKET
FROM STAGING.STG_ORDERS
GROUP BY ORDER_DATE;

-- ----------------------------------------------------------------------------
-- 2. DT_TOP_PRODUCTS — top 20 produits par CA sur les 30 derniers jours
--    Jointure order_items × orders pour récupérer la date de commande.
--    Fenêtre ancrée sur MAX(ORDER_DATE) (cf. note a).
-- ----------------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE MARTS.DT_TOP_PRODUCTS
  TARGET_LAG = '5 minutes'
  WAREHOUSE  = WH_TRANSFORM
  COMMENT    = 'Top 20 produits par CA, 30 derniers jours de données'
AS
WITH bornes AS (
  SELECT DATEADD(day, -30, MAX(ORDER_DATE)) AS date_debut
  FROM STAGING.STG_ORDERS
),
ventes AS (
  SELECT
    i.PRODUCT_ID,
    i.PRODUCT_NAME,
    i.CATEGORY,
    i.BRAND,
    SUM(i.QUANTITY)    AS UNITS_SOLD,
    SUM(i.LINE_AMOUNT) AS REVENUE
  FROM STAGING.STG_ORDER_ITEMS i
  JOIN STAGING.STG_ORDERS o ON o.ORDER_ID = i.ORDER_ID
  CROSS JOIN bornes b
  WHERE o.ORDER_DATE >= b.date_debut
  GROUP BY 1, 2, 3, 4
)
SELECT
  PRODUCT_ID, PRODUCT_NAME, CATEGORY, BRAND, UNITS_SOLD,
  ROUND(REVENUE, 2)                        AS REVENUE,
  RANK() OVER (ORDER BY REVENUE DESC)      AS REVENUE_RANK
FROM ventes
QUALIFY REVENUE_RANK <= 20;

-- ----------------------------------------------------------------------------
-- 3. DT_CUSTOMER_COHORTS — rétention par mois d'inscription
--    Format "long" : une ligne par (cohorte, décalage en mois).
--    MONTHS_SINCE_SIGNUP = 0 → activité le mois même de l'inscription.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE MARTS.DT_CUSTOMER_COHORTS
  TARGET_LAG = '5 minutes'
  WAREHOUSE  = WH_TRANSFORM
  COMMENT    = 'Rétention par cohorte (mois d inscription)'
AS
WITH cohorte AS (
  SELECT CUSTOMER_ID, DATE_TRUNC('month', SIGNUP_DATE) AS COHORT_MONTH
  FROM STAGING.STG_CUSTOMERS
  WHERE SIGNUP_DATE IS NOT NULL
),
taille AS (
  SELECT COHORT_MONTH, COUNT(*) AS COHORT_SIZE
  FROM cohorte
  GROUP BY 1
),
activite AS (
  SELECT
    c.COHORT_MONTH,
    DATEDIFF('month', c.COHORT_MONTH, DATE_TRUNC('month', o.ORDER_DATE)) AS MONTHS_SINCE_SIGNUP,
    COUNT(DISTINCT o.CUSTOMER_ID) AS ACTIVE_CUSTOMERS
  FROM cohorte c
  JOIN STAGING.STG_ORDERS o ON o.CUSTOMER_ID = c.CUSTOMER_ID
  WHERE o.ORDER_DATE >= c.COHORT_MONTH
  GROUP BY 1, 2
)
SELECT
  a.COHORT_MONTH,
  a.MONTHS_SINCE_SIGNUP,
  t.COHORT_SIZE,
  a.ACTIVE_CUSTOMERS,
  ROUND(100.0 * a.ACTIVE_CUSTOMERS / NULLIF(t.COHORT_SIZE, 0), 2) AS RETENTION_PCT
FROM activite a
JOIN taille t USING (COHORT_MONTH)
WHERE a.MONTHS_SINCE_SIGNUP >= 0;

-- ----------------------------------------------------------------------------
-- 4. Interroger les dynamic tables
-- ----------------------------------------------------------------------------
SELECT * FROM MARTS.DT_DAILY_REVENUE  ORDER BY ORDER_DATE DESC          LIMIT 14;
SELECT * FROM MARTS.DT_TOP_PRODUCTS   ORDER BY REVENUE_RANK             LIMIT 20;
SELECT * FROM MARTS.DT_CUSTOMER_COHORTS
  ORDER BY COHORT_MONTH, MONTHS_SINCE_SIGNUP                            LIMIT 50;

-- ----------------------------------------------------------------------------
-- 5. Vérifier le rafraîchissement automatique
-- ----------------------------------------------------------------------------
-- État général : scheduling_state = ACTIVE, target_lag, warehouse
SHOW DYNAMIC TABLES IN SCHEMA MARTS;

-- Forcer un refresh manuel (utile pour la démo, sans attendre le lag)
ALTER DYNAMIC TABLE MARTS.DT_DAILY_REVENUE     REFRESH;
ALTER DYNAMIC TABLE MARTS.DT_TOP_PRODUCTS      REFRESH;
ALTER DYNAMIC TABLE MARTS.DT_CUSTOMER_COHORTS  REFRESH;

-- Historique des rafraîchissements (état, mode incrémental vs full, durée)
SELECT NAME, STATE, REFRESH_ACTION, DATA_TIMESTAMP, REFRESH_START_TIME, REFRESH_END_TIME
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
  NAME_PREFIX  => 'SHOPFLOW_DB.MARTS.DT_',
  RESULT_LIMIT => 50
))
ORDER BY REFRESH_START_TIME DESC;

-- ============================================================================
-- 6. EXERCICE TIME TRAVEL — suppression accidentelle puis recovery
--    NB : le graphe de tasks est suspendu (fin du Jour 3) → rien ne s'exécute
--    automatiquement pendant l'exercice.
-- ============================================================================

-- 6.1 Compteur de référence
SELECT COUNT(*) AS avant_suppression FROM RAW.ORDERS_RAW;

-- 6.2 Suppression "accidentelle" de 1 000 commandes
DELETE FROM RAW.ORDERS_RAW
WHERE ORDER_ID IN (
  SELECT ORDER_ID FROM RAW.ORDERS_RAW ORDER BY ORDER_ID LIMIT 1000
);

-- Capturer l'ID de la requête DELETE (méthode de restauration la + robuste)
SET delete_qid = LAST_QUERY_ID();

-- Confirmer la perte (~1 000 lignes de moins)
SELECT COUNT(*) AS apres_suppression FROM RAW.ORDERS_RAW;

-- 6.3 RESTAURATION via Time Travel
--     On réinjecte UNIQUEMENT les lignes manquantes : état d'avant le DELETE
--     MINUS état courant = les 1 000 lignes supprimées.
INSERT INTO RAW.ORDERS_RAW
SELECT * FROM RAW.ORDERS_RAW BEFORE(STATEMENT => $delete_qid)
MINUS
SELECT * FROM RAW.ORDERS_RAW;

-- Variante demandée par l'énoncé (AT OFFSET) — à utiliser si < 60 s se sont
-- écoulées depuis le DELETE ; la méthode BEFORE(STATEMENT) ci-dessus est
-- préférable car indépendante du temps écoulé :
--   INSERT INTO RAW.ORDERS_RAW
--   SELECT * FROM RAW.ORDERS_RAW AT(OFFSET => -60)
--   MINUS
--   SELECT * FROM RAW.ORDERS_RAW;

-- 6.4 Vérifier le retour au compte initial
SELECT COUNT(*) AS apres_restauration FROM RAW.ORDERS_RAW;

-- 6.5 IMPORTANT — réinitialiser le stream sur ORDERS_RAW.
--     La restauration a réinséré 1 000 lignes ; le stream (APPEND_ONLY) les
--     verrait comme de nouveaux inserts et les dupliquerait dans STG au
--     prochain run. On recrée le stream → son offset repart de maintenant.
--     (Sans risque ici : tout J2 a déjà été traité au Jour 3.)
CREATE OR REPLACE STREAM RAW.STR_ORDERS
  ON TABLE RAW.ORDERS_RAW APPEND_ONLY = TRUE
  COMMENT = 'CDC des nouvelles commandes (réinitialisé après Time Travel)';

-- 6.6 Alternative de recovery : UNDROP (restauration d'une table supprimée)
--     Démonstration isolée sur une table jetable.
CREATE OR REPLACE TABLE RAW.ORDERS_BACKUP CLONE RAW.ORDERS_RAW;  -- clone zéro-copie
DROP TABLE RAW.ORDERS_BACKUP;                                    -- "oups"
UNDROP TABLE RAW.ORDERS_BACKUP;                                  -- récupérée !
SELECT COUNT(*) AS backup_restauree FROM RAW.ORDERS_BACKUP;
DROP TABLE IF EXISTS RAW.ORDERS_BACKUP;                          -- nettoyage

-- ----------------------------------------------------------------------------
-- 7. Fin de journée : suspendre les dynamic tables + les warehouses
--    Les DT continueraient sinon à se rafraîchir toutes les 5 min (crédits).
--    >>> Avant le dashboard (Jour 5) : les réactiver via ... RESUME. <<<
--    Les DT restent interrogeables une fois suspendues (données du dernier
--    refresh conservées).
-- ----------------------------------------------------------------------------
ALTER DYNAMIC TABLE MARTS.DT_DAILY_REVENUE     SUSPEND;
ALTER DYNAMIC TABLE MARTS.DT_TOP_PRODUCTS      SUSPEND;
ALTER DYNAMIC TABLE MARTS.DT_CUSTOMER_COHORTS  SUSPEND;

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
$$;