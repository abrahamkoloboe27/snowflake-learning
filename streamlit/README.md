# ShopFlow — Dashboard analytique (Streamlit + Plotly)

Tableau de bord e-commerce qui restitue les couches **STAGING** et **MARTS** de la base
`SHOPFLOW_DB` (Snowflake), dernière étape du projet fil rouge ShopFlow (Jour 5).

L'application lit directement les tables Snowflake via **Snowpark**, sans copie intermédiaire,
et se met en cache 5 minutes pour limiter la consommation de crédits.

---

## Aperçu

Le dashboard est organisé en six sections :

| Section | Contenu | Source |
|---|---|---|
| **Indicateurs clés** | Commandes du jour, panier moyen, CA du jour, CA période, clients uniques (avec deltas) | `STAGING.STG_ORDERS` |
| **Évolution du CA** | Ligne CA sur 14 jours + barres/ligne CA & commandes (granularité réglable) | `STAGING.STG_ORDERS` |
| **Produits** | Top N produits par CA + treemap du CA par catégorie | `STG_ORDER_ITEMS` ⨝ `STG_ORDERS` |
| **Comportement web** | Entonnoir de conversion (page → panier → checkout → achat) + répartition par appareil | `STAGING.STG_WEB_EVENTS` |
| **Rétention par cohorte** | Heatmap de rétention par mois d'inscription | `MARTS.DT_CUSTOMER_COHORTS` |
| **Dernières commandes** | 50 dernières commandes traitées, enrichies du nom client | `STG_ORDERS` ⨝ `STG_CUSTOMERS` |

Filtres disponibles dans la barre latérale : **période** de commande, **statut**, **catégories
produit**, **appareil**, **granularité** de la tendance (jour / semaine / mois) et **taille du
classement produits** (Top 5 à 20). Un bouton *Rafraîchir* vide le cache et relance la requête.

---

## Prérequis

- Une base `SHOPFLOW_DB` avec les schémas `STAGING` et `MARTS` alimentés (scripts SQL des Jours 1 à 4 — voir le [README du projet](../README.md)).
- Les Dynamic Tables du schéma `MARTS` doivent être **actives**. Si elles ont été suspendues au Jour 4 :

  ```sql
  ALTER DYNAMIC TABLE SHOPFLOW_DB.MARTS.DT_CUSTOMER_COHORTS RESUME;
  ALTER DYNAMIC TABLE SHOPFLOW_DB.MARTS.DT_DAILY_REVENUE     RESUME;
  ALTER DYNAMIC TABLE SHOPFLOW_DB.MARTS.DT_TOP_PRODUCTS      RESUME;
  ```

- Python 3.9+ (pour un lancement en local).

---

## Modes de connexion

L'application détecte automatiquement son contexte d'exécution :

- **Streamlit in Snowflake (SiS)** — la session active est récupérée via
  `get_active_session()`. Aucun credential à fournir.
- **Local** — la session est construite à partir de `.streamlit/secrets.toml`
  (fichier **non commité**).

---

## Lancement en local

```bash
cd streamlit

# 1. Dépendances
pip install -r requirements.txt

# 2. Configuration Snowflake
cp .streamlit/secrets.toml.example .streamlit/secrets.toml
#    → éditer .streamlit/secrets.toml avec votre compte, user, role, warehouse

# 3. Lancement
streamlit run streamlit_app.py
```

Le dashboard est alors accessible sur <http://localhost:8501>.

### Configuration des secrets

`.streamlit/secrets.toml.example` fournit un modèle à copier. Renseignez au minimum :

```toml
[snowflake]
account   = "xxxxx-xxxxx"        # ex : ab12345.eu-west-1
user      = "VOTRE_USER"
role      = "SHOPFLOW_ENGINEER"
warehouse = "WH_TRANSFORM"
database  = "SHOPFLOW_DB"
schema    = "STAGING"

# Authentification par navigateur (SSO/MFA) — recommandé, aucun mot de passe stocké
authenticator = "externalbrowser"
```

> ⚠️ **Ne jamais commiter `.streamlit/secrets.toml`.** Seul le fichier `*.example`
> est versionné ; le vrai fichier de secrets est ignoré par Git.

---

## Déploiement en Streamlit in Snowflake

1. Dans Snowsight : **Projects → Streamlit → + Streamlit App**.
2. Choisir la base `SHOPFLOW_DB`, un schéma et le warehouse `WH_TRANSFORM`.
3. Copier le contenu de `streamlit_app.py` dans l'éditeur.
4. Ajouter `plotly` dans l'onglet **Packages** (pandas et snowpark sont fournis).
5. Exécuter — la connexion se fait via la session active, sans secrets.

---

## Structure

```
streamlit/
├── streamlit_app.py              # application (une seule page)
├── requirements.txt              # dépendances local
├── README.md                     # ce fichier
└── .streamlit/
    └── secrets.toml.example      # modèle de configuration (secrets.toml ignoré par Git)
```

---

## Notes techniques

- **Cache** : `@st.cache_resource` pour la session Snowpark, `@st.cache_data(ttl=300)` pour les résultats de requêtes (5 min).
- **Requêtes filtrées** : les filtres de la sidebar sont poussés dans des clauses `WHERE` construites côté SQL ; l'agrégation est réalisée par Snowflake, pas côté pandas.
- **Palette** : couleurs volontairement définies (bleu nuit + ambre) plutôt que les défauts Plotly, pour une identité visuelle cohérente.
- **Logs** : logging standard (stdlib) vers `stderr`, niveau réglable via la variable d'environnement `SHOPFLOW_LOG_LEVEL` (défaut `DEBUG`).

---

## Dépannage

| Symptôme | Cause probable | Solution |
|---|---|---|
| « Aucune commande ne correspond aux filtres » | Période ou statuts trop restrictifs | Élargir la période / cocher plus de statuts |
| Section rétention vide | `DT_CUSTOMER_COHORTS` suspendue ou vide | `ALTER DYNAMIC TABLE ... RESUME;` (voir Prérequis) |
| Erreur de connexion en local | `secrets.toml` absent ou incomplet | Copier le `.example` et renseigner le compte/role/warehouse |
| Objet introuvable (`STG_*`) | Schémas STAGING/MARTS non alimentés | Rejouer les scripts SQL des Jours 1 à 4 |
