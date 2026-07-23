"""
ShopFlow — Dashboard analytique (Streamlit + Plotly)
Co-authored with CoCo

Lit les couches STAGING et MARTS de SHOPFLOW_DB sur Snowflake.

Deux modes de connexion, gérés automatiquement :
  • Streamlit in Snowflake  → session active (aucun credential)
  • Streamlit en local       → .streamlit/secrets.toml (NON commité)

Lancement local :
    pip install -r requirements.txt
    streamlit run streamlit_app.py

Prérequis : réactiver les Dynamic Tables si elles ont été suspendues au Jour 4
    ALTER DYNAMIC TABLE SHOPFLOW_DB.MARTS.DT_CUSTOMER_COHORTS RESUME;  -- etc.
"""

import os
import sys
import time
from datetime import timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import logging
from plotly.subplots import make_subplots

# ----------------------------------------------------------------------------
# Logging (stdlib) — console uniquement
# ----------------------------------------------------------------------------
LOG_LEVEL = os.getenv("SHOPFLOW_LOG_LEVEL", "DEBUG").upper()

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)-8s | %(name)s:%(funcName)s:%(lineno)d - %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------------
# Palette délibérée (bleu nuit + ambre) plutôt que les couleurs Plotly par défaut
# ----------------------------------------------------------------------------
INK = "#1B2A41"        # bleu nuit — texte / barres principales
AMBER = "#E0A458"      # ambre — accent (CA, points clés)
TEAL = "#2E6E8E"       # bleu acier — série secondaire
SAGE = "#6A994E"       # vert — valeurs positives / conversion
CLAY = "#BC4B51"       # brique — alertes / abandons
MUTED = "#8D99AE"      # gris — libellés discrets
SEQ = [INK, TEAL, AMBER, SAGE, CLAY, MUTED]   # séquence catégorielle

PLOTLY_TEMPLATE = "plotly_white"
CHART_H = 360

st.set_page_config(page_title="ShopFlow • Dashboard", page_icon="📦", layout="wide")

# Habillage : cartes KPI + titres/sections centrés sur fond
st.markdown(
    """
    <style>
      .block-container {padding-top: 2rem;}

      /* Cartes KPI */
      div[data-testid="stMetric"] {
        background: #F7F8FA; border: 1px solid #E6E9EF;
        border-radius: 12px; padding: 14px 16px;
      }
      div[data-testid="stMetricLabel"] {color: #55607A;}

      /* Titre principal (st.title → h1) — bandeau bleu nuit */
      h1 {
        text-align: center;
        color: #FFFFFF;
        background: linear-gradient(135deg, #1B2A41 0%, #2E6E8E 100%);
        border-radius: 14px;
        padding: 22px 20px;
        margin: 0.2rem 0 2.2rem 0;
        box-shadow: 0 2px 10px rgba(27, 42, 65, 0.18);
        letter-spacing: 0.5px;
      }

      /* En-têtes de section (st.header → h2) — bandeau ambre clair */
      h2 {
        text-align: center;
        color: #1B2A41;
        background: #FDF3E3;
        border: 1px solid #F0DFBF;
        border-radius: 10px;
        padding: 12px 16px;
        margin: 2.6rem 0 1.4rem 0;
        letter-spacing: 0.3px;
      }

      /* Sous-titres (st.subheader → h3) — pastille discrète centrée */
      h3 {
        text-align: center;
        color: #2E6E8E;
        background: #F2F6F8;
        border-radius: 8px;
        padding: 8px 14px;
        margin: 0.6rem 0 1.2rem 0;
        font-weight: 600;
      }

      /* Espace entre les titres et les graphiques Plotly / tableaux */
      div[data-testid="stPlotlyChart"],
      div[data-testid="stDataFrame"] {
        margin-top: 0.6rem;
      }

      /* Légende sous le titre principal, centrée */
      div[data-testid="stCaptionContainer"] {text-align: center;}
    </style>
    """,
    unsafe_allow_html=True,
)


# ----------------------------------------------------------------------------
# Connexion & requêtes
# ----------------------------------------------------------------------------
@st.cache_resource
def get_session():
    """Session Snowpark : active (SiS) ou construite depuis les secrets (local)."""
    try:
        from snowflake.snowpark.context import get_active_session
        session = get_active_session()
        logger.info("Session Snowpark active récupérée (mode Streamlit in Snowflake).")
        return session
    except Exception as exc:
        logger.debug("Pas de session active (%s), bascule vers les secrets locaux.", exc)
        from snowflake.snowpark import Session
        session = Session.builder.configs(dict(st.secrets["snowflake"])).create()
        logger.info("Session Snowpark construite depuis secrets.toml (mode local).")
        return session


@st.cache_data(ttl=300, show_spinner=False)
def run_query(sql: str) -> pd.DataFrame:
    """Exécute une requête et renvoie un DataFrame (colonnes en MAJUSCULES)."""
    # SQL condensé sur une ligne pour un log lisible
    sql_compact = " ".join(sql.split())
    logger.debug("Exécution SQL : %s", sql_compact)
    t0 = time.perf_counter()
    try:
        df = get_session().sql(sql).to_pandas()
    except Exception:
        logger.exception("Échec de la requête SQL : %s", sql_compact)
        raise
    elapsed_ms = (time.perf_counter() - t0) * 1000
    logger.debug(
        "Requête OK — %s lignes × %s colonnes en %.0f ms",
        len(df), df.shape[1], elapsed_ms,
    )
    return df


DB = "SHOPFLOW_DB"


def sql_in(col: str, values) -> str:
    """Construit un IN (...) sûr ; liste vide => aucune contrainte (TRUE)."""
    if not values:
        return "TRUE"
    escaped = ",".join("'" + str(v).replace("'", "''") + "'" for v in values)
    return f"{col} IN ({escaped})"


def euro(x) -> str:
    if x is None or pd.isna(x):
        return "—"
    return f"{x:,.0f} €".replace(",", " ")


# ----------------------------------------------------------------------------
# Options de filtres (mises en cache)
# ----------------------------------------------------------------------------
@st.cache_data(ttl=600, show_spinner=False)
def load_filter_options():
    bornes = run_query(
        f"SELECT MIN(ORDER_DATE) AS DMIN, MAX(ORDER_DATE) AS DMAX "
        f"FROM {DB}.STAGING.STG_ORDERS"
    ).iloc[0]
    statuts = run_query(
        f"SELECT DISTINCT STATUS FROM {DB}.STAGING.STG_ORDERS "
        f"WHERE STATUS IS NOT NULL ORDER BY 1"
    )["STATUS"].tolist()
    categories = run_query(
        f"SELECT DISTINCT CATEGORY FROM {DB}.STAGING.STG_PRODUCTS "
        f"WHERE CATEGORY IS NOT NULL ORDER BY 1"
    )["CATEGORY"].tolist()
    devices = run_query(
        f"SELECT DISTINCT DEVICE FROM {DB}.STAGING.STG_WEB_EVENTS "
        f"WHERE DEVICE IS NOT NULL ORDER BY 1"
    )["DEVICE"].tolist()
    logger.info(
        "Options de filtres chargées — bornes %s→%s, %s statuts, %s catégories, %s appareils",
        bornes["DMIN"], bornes["DMAX"], len(statuts), len(categories), len(devices),
    )
    return pd.to_datetime(bornes["DMIN"]).date(), pd.to_datetime(bornes["DMAX"]).date(), statuts, categories, devices


logger.info("Démarrage du rendu du dashboard ShopFlow.")
d_min, d_max, statuts_opts, cat_opts, dev_opts = load_filter_options()

# ----------------------------------------------------------------------------
# Sidebar — filtres
# ----------------------------------------------------------------------------
st.sidebar.title("📦 ShopFlow")
st.sidebar.caption("Filtres du tableau de bord")

periode = st.sidebar.date_input(
    "Période (date de commande)",
    value=(d_min, d_max),
    min_value=d_min,
    max_value=d_max,
)
# date_input peut renvoyer une seule date tant que la 2ᵉ n'est pas choisie
if isinstance(periode, (list, tuple)) and len(periode) == 2:
    date_from, date_to = periode
else:
    date_from, date_to = d_min, d_max

statuts = st.sidebar.multiselect("Statut de commande", statuts_opts, default=statuts_opts)
categories = st.sidebar.multiselect("Catégories produit", cat_opts, default=[],
                                    help="Vide = toutes les catégories")
devices = st.sidebar.multiselect("Appareil (événements web)", dev_opts, default=[],
                                 help="Vide = tous les appareils")

st.sidebar.divider()
granularite = st.sidebar.radio("Granularité de la tendance", ["Jour", "Semaine", "Mois"],
                               horizontal=True)
top_n = st.sidebar.slider("Nombre de produits (classement)", 5, 20, 10)

st.sidebar.divider()
if st.sidebar.button("🔄 Rafraîchir les données", use_container_width=True):
    logger.info("Bouton « Rafraîchir » cliqué — vidage du cache et rerun.")
    st.cache_data.clear()
    st.rerun()

logger.debug(
    "Filtres actifs — période=%s→%s, statuts=%s, catégories=%s, appareils=%s, "
    "granularité=%s, top_n=%s",
    date_from, date_to, statuts, categories or "toutes", devices or "tous",
    granularite, top_n,
)

# Fragments WHERE réutilisables
d0, d1 = date_from.isoformat(), date_to.isoformat()
where_orders = f"ORDER_DATE BETWEEN '{d0}' AND '{d1}' AND {sql_in('STATUS', statuts)}"
where_items = (f"o.ORDER_DATE BETWEEN '{d0}' AND '{d1}' AND {sql_in('o.STATUS', statuts)} "
               f"AND {sql_in('i.CATEGORY', categories)}")
where_events = f"EVENT_DATE BETWEEN '{d0}' AND '{d1}' AND {sql_in('DEVICE', devices)}"
trunc = {"Jour": "day", "Semaine": "week", "Mois": "month"}[granularite]

# ----------------------------------------------------------------------------
# Chargement des données filtrées
# ----------------------------------------------------------------------------
daily = run_query(f"""
    SELECT ORDER_DATE,
           COUNT(DISTINCT ORDER_ID)                                          AS NB_ORDERS,
           ROUND(SUM(TOTAL_AMOUNT), 2)                                       AS REVENUE,
           ROUND(SUM(TOTAL_AMOUNT) / NULLIF(COUNT(DISTINCT ORDER_ID), 0), 2) AS AVG_BASKET
    FROM {DB}.STAGING.STG_ORDERS
    WHERE {where_orders}
    GROUP BY ORDER_DATE
    ORDER BY ORDER_DATE
""")

st.title("Tableau de bord e-commerce")
if daily.empty:
    logger.warning("Aucune commande pour les filtres courants — arrêt du rendu (st.stop).")
    st.warning("Aucune commande ne correspond aux filtres. Élargissez la période ou les statuts.")
    st.stop()
    logger.debug("Série journalière chargée : %s jours de données.", len(daily))

daily["ORDER_DATE"] = pd.to_datetime(daily["ORDER_DATE"])
dernier_jour = daily["ORDER_DATE"].max().date()
st.caption(f"Dernier jour de données disponible : **{dernier_jour:%d/%m/%Y}** — "
           f"période analysée du {date_from:%d/%m/%Y} au {date_to:%d/%m/%Y}")

# ============================================================================
# SECTION 1 — Indicateurs clés
# ============================================================================
with st.container():
    st.header("Indicateurs clés")

    last = daily.iloc[-1]
    prev = daily.iloc[-2] if len(daily) > 1 else None
    nb_clients = run_query(
        f"SELECT COUNT(DISTINCT CUSTOMER_ID) AS N FROM {DB}.STAGING.STG_ORDERS "
        f"WHERE {where_orders}"
    ).iloc[0]["N"]

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Commandes (dernier jour)", f"{int(last['NB_ORDERS'])}",
              delta=None if prev is None else int(last["NB_ORDERS"] - prev["NB_ORDERS"]))
    c2.metric("Panier moyen (dernier jour)", euro(last["AVG_BASKET"]),
              delta=None if prev is None else euro(last["AVG_BASKET"] - prev["AVG_BASKET"]))
    c3.metric("CA (dernier jour)", euro(last["REVENUE"]),
              delta=None if prev is None else euro(last["REVENUE"] - prev["REVENUE"]))
    c4.metric("CA total (période)", euro(daily["REVENUE"].sum()))
    c5.metric("Clients uniques (période)", f"{int(nb_clients):,}".replace(",", " "))

# ============================================================================
# SECTION 2 — Évolution du chiffre d'affaires
# ============================================================================
with st.container():
    st.header("Évolution du chiffre d'affaires")
    col_a, col_b = st.columns([1, 1])

    # 2.1 — Ligne : CA des 14 derniers jours (exigence)
    with col_a:
        st.subheader("CA — 14 derniers jours")
        last14 = daily[daily["ORDER_DATE"] >= (daily["ORDER_DATE"].max() - timedelta(days=13))]
        fig = px.line(last14, x="ORDER_DATE", y="REVENUE", markers=True,
                      template=PLOTLY_TEMPLATE)
        fig.update_traces(line_color=AMBER, line_width=3, marker=dict(size=7, color=INK))
        fig.update_layout(height=CHART_H, xaxis_title=None, yaxis_title="CA (€)",
                          margin=dict(t=10, b=0, l=0, r=0))
        st.plotly_chart(fig, use_container_width=True)

    # 2.2 — Barres + ligne : CA et commandes par période (granularité réglable)
    with col_b:
        st.subheader(f"CA & commandes par {granularite.lower()}")
        trend = run_query(f"""
            SELECT DATE_TRUNC('{trunc}', ORDER_DATE) AS PERIODE,
                   COUNT(DISTINCT ORDER_ID)          AS NB_ORDERS,
                   ROUND(SUM(TOTAL_AMOUNT), 2)        AS REVENUE
            FROM {DB}.STAGING.STG_ORDERS
            WHERE {where_orders}
            GROUP BY 1 ORDER BY 1
        """)
        trend["PERIODE"] = pd.to_datetime(trend["PERIODE"])
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_bar(x=trend["PERIODE"], y=trend["REVENUE"], name="CA (€)",
                    marker_color=TEAL, opacity=0.85)
        fig.add_scatter(x=trend["PERIODE"], y=trend["NB_ORDERS"], name="Commandes",
                        mode="lines+markers", line=dict(color=AMBER, width=3),
                        secondary_y=True)
        fig.update_layout(template=PLOTLY_TEMPLATE, height=CHART_H,
                          margin=dict(t=10, b=0, l=0, r=0),
                          legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0))
        fig.update_yaxes(title_text="CA (€)", secondary_y=False)
        fig.update_yaxes(title_text="Commandes", secondary_y=True)
        st.plotly_chart(fig, use_container_width=True)

# ============================================================================
# SECTION 3 — Produits
# ============================================================================
with st.container():
    st.header("Produits")
    col_a, col_b = st.columns([3, 2])

    produits = run_query(f"""
        SELECT i.PRODUCT_NAME, i.CATEGORY, i.BRAND,
               SUM(i.QUANTITY)             AS UNITS_SOLD,
               ROUND(SUM(i.LINE_AMOUNT),2) AS REVENUE
        FROM {DB}.STAGING.STG_ORDER_ITEMS i
        JOIN {DB}.STAGING.STG_ORDERS o ON o.ORDER_ID = i.ORDER_ID
        WHERE {where_items}
        GROUP BY 1, 2, 3
        ORDER BY REVENUE DESC
        LIMIT {top_n}
    """)

    # 3.1 — Barres horizontales : top N produits par CA (exigence)
    with col_a:
        st.subheader(f"Top {top_n} produits par CA")
        if produits.empty:
            st.info("Aucune vente sur ce périmètre.")
        else:
            fig = px.bar(produits.sort_values("REVENUE"), x="REVENUE", y="PRODUCT_NAME",
                         orientation="h", color="CATEGORY", template=PLOTLY_TEMPLATE,
                         color_discrete_sequence=SEQ, text="REVENUE")
            fig.update_traces(texttemplate="%{text:.0f} €", textposition="outside")
            fig.update_layout(height=max(CHART_H, 40 * len(produits)),
                              xaxis_title="CA (€)", yaxis_title=None,
                              margin=dict(t=10, b=0, l=0, r=0),
                              legend_title_text="Catégorie")
            st.plotly_chart(fig, use_container_width=True)

    # 3.2 — Treemap : répartition du CA par catégorie (bonus)
    with col_b:
        st.subheader("CA par catégorie")
        parcat = run_query(f"""
            SELECT i.CATEGORY, ROUND(SUM(i.LINE_AMOUNT),2) AS REVENUE
            FROM {DB}.STAGING.STG_ORDER_ITEMS i
            JOIN {DB}.STAGING.STG_ORDERS o ON o.ORDER_ID = i.ORDER_ID
            WHERE {where_items}
            GROUP BY 1 ORDER BY REVENUE DESC
        """)
        if parcat.empty:
            st.info("Aucune donnée.")
        else:
            fig = px.treemap(parcat, path=["CATEGORY"], values="REVENUE",
                             color="REVENUE", color_continuous_scale="Teal",
                             template=PLOTLY_TEMPLATE)
            fig.update_layout(height=CHART_H, margin=dict(t=10, b=0, l=0, r=0),
                              coloraxis_showscale=False)
            st.plotly_chart(fig, use_container_width=True)

# ============================================================================
# SECTION 4 — Comportement web (bonus)
# ============================================================================
with st.container():
    st.header("Comportement web")
    col_a, col_b = st.columns([3, 2])

    # 4.1 — Entonnoir de conversion
    with col_a:
        st.subheader("Entonnoir de conversion")
        ev = run_query(f"""
            SELECT EVENT_TYPE, COUNT(*) AS N
            FROM {DB}.STAGING.STG_WEB_EVENTS
            WHERE {where_events}
            GROUP BY 1
        """)
        if ev.empty:
            st.info("Aucun événement sur ce périmètre.")
        else:
            counts = dict(zip(ev["EVENT_TYPE"], ev["N"]))
            etapes = ["page_view", "add_to_cart", "checkout", "purchase"]
            labels = ["Page vue", "Ajout panier", "Checkout", "Achat"]
            vals = [int(counts.get(e, 0)) for e in etapes]
            fig = go.Figure(go.Funnel(y=labels, x=vals, textinfo="value+percent initial",
                                      marker_color=[INK, TEAL, AMBER, SAGE]))
            fig.update_layout(template=PLOTLY_TEMPLATE, height=CHART_H,
                              margin=dict(t=10, b=0, l=0, r=0))
            st.plotly_chart(fig, use_container_width=True)

    # 4.2 — Répartition par appareil
    with col_b:
        st.subheader("Événements par appareil")
        dev = run_query(f"""
            SELECT DEVICE, COUNT(*) AS N
            FROM {DB}.STAGING.STG_WEB_EVENTS
            WHERE {where_events}
            GROUP BY 1 ORDER BY N DESC
        """)
        if dev.empty:
            st.info("Aucune donnée.")
        else:
            fig = px.pie(dev, names="DEVICE", values="N", hole=0.55,
                         template=PLOTLY_TEMPLATE, color_discrete_sequence=SEQ)
            fig.update_traces(textposition="inside", textinfo="percent+label")
            fig.update_layout(height=CHART_H, margin=dict(t=10, b=0, l=0, r=0),
                              showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

# ============================================================================
# SECTION 5 — Rétention clients (bonus, alimenté par DT_CUSTOMER_COHORTS)
# ============================================================================
with st.container():
    st.header("Rétention par cohorte d'inscription")
    st.caption("Source : DT_CUSTOMER_COHORTS (mart). % de clients d'une cohorte "
               "actifs N mois après leur inscription. Non affecté par les filtres.")
    cohortes = run_query(f"""
        SELECT TO_VARCHAR(COHORT_MONTH, 'YYYY-MM') AS COHORTE,
               MONTHS_SINCE_SIGNUP, RETENTION_PCT
        FROM {DB}.MARTS.DT_CUSTOMER_COHORTS
        WHERE MONTHS_SINCE_SIGNUP BETWEEN 0 AND 12
        ORDER BY COHORTE, MONTHS_SINCE_SIGNUP
    """)
    if cohortes.empty:
        st.info("La Dynamic Table DT_CUSTOMER_COHORTS est vide ou suspendue. "
                "Réactivez-la : ALTER DYNAMIC TABLE SHOPFLOW_DB.MARTS.DT_CUSTOMER_COHORTS RESUME;")
    else:
        pivot = cohortes.pivot(index="COHORTE", columns="MONTHS_SINCE_SIGNUP",
                               values="RETENTION_PCT").sort_index()
        fig = px.imshow(pivot, text_auto=".0f", aspect="auto",
                        color_continuous_scale="Blues", template=PLOTLY_TEMPLATE,
                        labels=dict(x="Mois depuis l'inscription", y="Cohorte", color="% actifs"))
        fig.update_layout(height=max(CHART_H, 26 * len(pivot)),
                          margin=dict(t=10, b=0, l=0, r=0))
        st.plotly_chart(fig, use_container_width=True)

# ============================================================================
# SECTION 6 — Dernières commandes traitées (exigence)
# ============================================================================
with st.container():
    st.header("Dernières commandes traitées")
    commandes = run_query(f"""
        SELECT o.ORDER_ID, o.ORDER_TS,
               o.CUSTOMER_ID, c.FIRST_NAME || ' ' || c.LAST_NAME AS CLIENT,
               o.STATUS, o.TOTAL_AMOUNT
        FROM {DB}.STAGING.STG_ORDERS o
        LEFT JOIN {DB}.STAGING.STG_CUSTOMERS c ON c.CUSTOMER_ID = o.CUSTOMER_ID
        WHERE {where_orders}
        ORDER BY o.ORDER_TS DESC
        LIMIT 50
    """)
    st.dataframe(
        commandes, use_container_width=True, hide_index=True,
        column_config={
            "ORDER_ID": "Commande",
            "ORDER_TS": st.column_config.DatetimeColumn("Date/heure", format="DD/MM/YYYY HH:mm"),
            "CUSTOMER_ID": "ID client",
            "CLIENT": "Client",
            "STATUS": "Statut",
            "TOTAL_AMOUNT": st.column_config.NumberColumn("Montant", format="%.2f €"),
        },
    )

st.caption("ShopFlow • données SHOPFLOW_DB (STAGING + MARTS) • cache 5 min")

logger.info("Rendu du dashboard terminé avec succès.")