# Projet Big Data — Ingestion Bronze (BCE / KBO)

Pipeline d'ingestion des données des entreprises belges (Banque-Carrefour des
Entreprises). Cette journée couvre la **couche Bronze** : on peuple MongoDB avec
les entreprises, on initialise une **State DB** qui garantit l'idempotence des
téléchargements, puis des **DAGs Airflow** ingèrent les documents NBB/CBSO et
eJustice dans **HDFS**.

## Architecture

```
                +---------------------+
                |   enterprise.csv    |  (Open Data BCE/KBO)
                +----------+----------+
                           |  seed
                           v
   MongoDB  ┌──────────────────────────┐     ┌───────────────────────────┐
            │  companies                │     │  ingestion_state (State DB) │
            │  1 doc / entreprise (BCE) │     │  1 doc / fichier a charger  │
            └────────────┬─────────────┘     │  bce, deposit_id, year,     │
                         │                    │  status(pending/done/error),│
                         │ lit les BCE        │  hdfs_path, timestamps      │
                         v                    └──────────────┬──────────────┘
            ┌──────────────────────────┐                    ^
            │     Airflow DAGs          │  delta detection   │ mise a jour
            │  ingestion_bronze         ├────────────────────┘
            │   - ingest_nbb (CSV+PDF)  │
            │   - ingest_ejustice (PDF) │ ── via Tor (tor1/2/3, rotation IP)
            │   - ingest_stapor (JSON)  │ ── via cookie Playwright (notaire)
            └────────────┬─────────────┘
                         │ ecrit les fichiers bruts
                         v
                 ┌────────────────┐
                 │   HDFS Bronze  │   /data/raw/{source}/{bce}/{type}/...
                 └────────────────┘
```

### Rotation Tor (anti-blocage)

Les sources publiques limitent les requêtes par IP. Le scraping NBB et eJustice
est donc routé à travers trois proxies Tor (`tor1`, `tor2`, `tor3`) en
**round-robin**, avec possibilité de renouveler le circuit (NEWNYM → nouvelle
IP de sortie). Voir `include/http_client.py`. La rotation se vérifie ainsi :

```bash
docker compose exec airflow-scheduler python -c \
  "from include import http_client; print(http_client.public_ip(use_tor=True))"
```

### Source stapor / notaire (cookie anti-bot)

`statuts.notaire.be` est protégé par un challenge JavaScript. Le cookie est
obtenu via **Playwright (Chromium)** puis mis en cache dans HDFS pour être
partagé entre les tâches Airflow (voir `include/notaire_cookie.py`). Un cookie
peut aussi être fourni directement via la variable `COOKIE_NOTAIRE`. Comme le
cookie est lié à l'IP, les appels stapor partent en **direct** (sans Tor).

### Le rôle de la State DB (delta detection)

Dès que les numéros BCE sont dans MongoDB, la collection `ingestion_state` suit
**chaque fichier** : numéro BCE, `deposit_id`, année, statut
(`pending` / `done` / `error`), chemin HDFS et horodatages. Avant chaque
téléchargement, le pipeline vérifie si la clé est déjà `done` : si oui, il
**saute** le fichier. On ne re-télécharge donc jamais ce qui existe déjà.

Clé déterministe d'un fichier : `bce | source | deposit_id | doc_type | year`.

## Structure du dépôt

```
.
├── docker-compose.yml        # Airflow + MongoDB + HDFS (+ mongo-express)
├── Dockerfile                # image Airflow + dependances du pipeline
├── hadoop.env                # configuration HDFS
├── requirements.txt          # dependances pour execution locale
├── .env.example              # variables d'environnement
├── dags/
│   ├── seed_companies_mongo.py  # init : peuple MongoDB + State DB
│   └── ingestion_bronze.py      # ingestion NBB + eJustice + stapor -> HDFS
├── include/
│   ├── config.py             # configuration centrale
│   ├── mongo_utils.py        # referentiel + State DB (delta detection)
│   ├── hdfs_utils.py         # client WebHDFS + ecriture Bronze
│   ├── http_client.py        # GET avec rotation Tor (round-robin + NEWNYM)
│   ├── notaire_cookie.py     # cookie stapor via Playwright + cache HDFS
│   ├── seed.py               # chargement des entreprises dans MongoDB
│   └── sources.py            # ingestion NBB / eJustice / stapor
└── data/                     # enterprise.csv (non versionne)
```

## Sources de données

| Source       | Contenu                              | Format         | Accès        |
|--------------|--------------------------------------|----------------|--------------|
| KBO Open Data| référentiel des entreprises belges   | CSV → MongoDB  | local        |
| NBB / CBSO   | comptes annuels (consult.cbso.nbb.be)| CSV + PDF      | via Tor      |
| eJustice     | publications du Moniteur belge       | PDF            | via Tor      |
| stapor       | statuts notariés (statuts.notaire.be)| JSON (+ PDF)   | cookie direct|

## Démarrage rapide

### 1. Lancer l'infrastructure

```bash
docker compose build
docker compose up -d
```

Services exposés :

| Service        | URL                     | Identifiants     |
|----------------|-------------------------|------------------|
| Airflow        | http://localhost:8080   | airflow / airflow|
| HDFS NameNode  | http://localhost:9870   | —                |
| Mongo Express  | http://localhost:8081   | —                |
| Tor (SOCKS5)   | localhost:9050/9052/9054| —                |


### 3. Exécuter les DAGs

1. Déclencher **`seed_companies_mongo`** → peuple MongoDB et crée les index de
   la State DB.
2. Déclencher **`ingestion_bronze`** → télécharge CSV + PDF vers HDFS Bronze
   en appliquant la delta detection.

Limiter le nombre d'entreprises par run via la variable `INGEST_LIMIT`
(défaut : 10).

### 4. Vérifier le résultat

```bash
# Fichiers Bronze dans HDFS
docker exec -it namenode hdfs dfs -ls -R /data/raw | head

# Etat de la State DB
docker exec -it mongo mongosh bce --eval \
  'db.ingestion_state.aggregate([{$group:{_id:"$status",n:{$sum:1}}}])'
```

## Idempotence (preuve de la delta detection)

Relancer `ingestion_bronze` une seconde fois : tous les fichiers déjà chargés
passent en `skipped` (aucun nouveau téléchargement), ce qui démontre que la
State DB empêche les re-téléchargements.

## Workflow Git

- Le travail de chaque journée est livré sur une branche dédiée.
- Cette journée : branche **`INGESTION-BRONZE`**.
- La branche **`main`** ne contient que la dernière version fonctionnelle.
