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
            │   - ingest_ejustice (PDF) │
            └────────────┬─────────────┘
                         │ ecrit les fichiers bruts
                         v
                 ┌────────────────┐
                 │   HDFS Bronze  │   /data/raw/{source}/{bce}/{type}/...
                 └────────────────┘
```

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
│   └── ingestion_bronze.py      # ingestion NBB/CBSO + eJustice -> HDFS
├── include/
│   ├── config.py             # configuration centrale
│   ├── mongo_utils.py        # referentiel + State DB (delta detection)
│   ├── hdfs_utils.py         # client WebHDFS + ecriture Bronze
│   ├── seed.py               # chargement des entreprises dans MongoDB
│   └── sources.py            # ingestion NBB/CBSO et eJustice
└── data/                     # enterprise.csv (non versionne)
```

## Sources de données

| Source       | Contenu                              | Format        |
|--------------|--------------------------------------|---------------|
| KBO Open Data| référentiel des entreprises belges   | CSV → MongoDB |
| NBB / CBSO   | comptes annuels (consult.cbso.nbb.be)| CSV + PDF     |
| eJustice     | publications du Moniteur belge       | PDF           |

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

### 2. (Optionnel) Charger l'Open Data BCE

Placer `enterprise.csv` dans `data/`. Sans ce fichier, le seed bascule
automatiquement sur un jeu de démonstration (Google / Apple / SNCB).

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
