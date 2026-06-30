"""Configuration centrale du pipeline d'ingestion Bronze BCE/KBO.

Toutes les valeurs sont lues depuis l'environnement (fournies par
docker-compose en conteneur, ou par un fichier .env en local).
"""

from __future__ import annotations

import os
from pathlib import Path

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    # python-dotenv est optionnel ; en conteneur les variables existent deja.
    pass


# --- MongoDB : referentiel entreprises + State DB ---------------------------
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "bce")
MONGO_COMPANIES = os.getenv("MONGO_COMPANIES", "companies")
MONGO_STATE = os.getenv("MONGO_STATE", "ingestion_state")

# --- HDFS (Bronze data lake) ------------------------------------------------
HDFS_URL = os.getenv("HDFS_URL", "http://localhost:9870")
HDFS_USER = os.getenv("HDFS_USER", "root")
BRONZE_ROOT = os.getenv("BRONZE_ROOT", "/data/raw")

# --- Fenetre d'annees pour les comptes annuels NBB/CBSO ---------------------
YEAR_MIN = int(os.getenv("YEAR_MIN", "2021"))
YEAR_MAX = int(os.getenv("YEAR_MAX", "2025"))


def _as_bool(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _as_list(value: str) -> list[str]:
    return [item.strip() for item in (value or "").split(",") if item.strip()]


# --- Rotation Tor (anti-blocage du scraping) --------------------------------
# Liste de proxies SOCKS5 ; les requetes tournent en round-robin dessus.
USE_TOR = _as_bool(os.getenv("USE_TOR", "false"))
TOR_PROXIES = _as_list(os.getenv("TOR_PROXIES", ""))
TOR_CONTROL_HOSTS = _as_list(os.getenv("TOR_CONTROL_HOSTS", ""))
TOR_CONTROL_PORT = int(os.getenv("TOR_CONTROL_PORT", "9051"))
TOR_CONTROL_PASSWORD = os.getenv("TOR_CONTROL_PASSWORD", "mypass")

# --- Notaire / stapor -------------------------------------------------------
# Cookie anti-bot : fourni directement, sinon acquis via Playwright.
COOKIE_NOTAIRE = os.getenv("COOKIE_NOTAIRE", "").strip()
NOTAIRE_SEED_BCE = os.getenv("NOTAIRE_SEED_BCE", "0836157420")
# Cache du cookie dans HDFS pour le partager entre les taches Airflow.
COOKIE_HDFS_PATH = os.getenv("COOKIE_HDFS_PATH", "/data/raw/_cookies/notaire.txt")

# --- Chemins locaux ---------------------------------------------------------
# Dans le conteneur Airflow le dossier data/ est monte sur /opt/airflow/data.
DATA_DIR = Path(os.getenv("DATA_DIR", "/opt/airflow/data"))
ENTERPRISE_CSV = DATA_DIR / "enterprise.csv"

# Jeu de demonstration utilise quand aucun CSV KBO n'est present.
# (Google Belgium, Apple Retail Belgium, SNCB).
DEMO_COMPANIES = [
    {"bce": "0878065378", "name": "GOOGLE BELGIUM"},
    {"bce": "0836157420", "name": "APPLE RETAIL BELGIUM"},
    {"bce": "0203430576", "name": "SNCB"},
]

# --- HTTP -------------------------------------------------------------------
HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; BCE-Bronze-Ingestion/1.0)",
    "Accept-Language": "fr-FR,fr;q=0.9",
}
HTTP_TIMEOUT = 60
