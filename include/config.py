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

# --- Jour 2 : collections Bronze consolide, Silver et State DB hotellerie ----
MONGO_FINALE = os.getenv("MONGO_FINALE", "enterprise_finale")
MONGO_SILVER = os.getenv("MONGO_SILVER", "enterprise_silver")
MONGO_HOTEL_STATE = os.getenv("MONGO_HOTEL_STATE", "hotel_state")

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

# --- Fichiers Open Data KBO utilises pour construire enterprise_finale -------
KBO_FILES = {
    "enterprise": DATA_DIR / "enterprise.csv",
    "denomination": DATA_DIR / "denomination.csv",
    "address": DATA_DIR / "address.csv",
    "activity": DATA_DIR / "activity.csv",
    "code": DATA_DIR / "code.csv",
}

# --- Jour 2 : ciblage hotellerie --------------------------------------------
# Codes NACE retenus pour le secteur hotelier (Nace2008 + Nace2025).
HOTEL_NACE_CODES = {
    "55100",  # Hotels et hebergement similaire
    "55201",  # Auberges de jeunesse
    "55202",  # Centres et villages de vacances
    "55203",  # Gites, appartements et meubles de vacances
    "55204",  # Chambres d'hotes
    "55209",  # Autres hebergements de courte duree n.c.a.
    "55300",  # Terrains de camping et parcs pour caravanes
    "55400",  # Intermediation pour l'hebergement (Airbnb/Booking)
    "55900",  # Autres hebergements
}

# Formes juridiques exclues (entites publiques, services, communes...).
EXCLUDED_JURIDICAL_FORMS = {
    "110", "114", "116", "117",
    "301", "302", "303",
    "310", "320", "330", "340", "350",
    "400", "411", "412", "413", "414", "415", "416", "417", "418", "419", "420",
}

# Fenetre d'exercices scrapes pour l'hotellerie.
HOTEL_YEAR_MIN = int(os.getenv("HOTEL_YEAR_MIN", "2021"))

# Vrais hotels belges (avec comptes annuels NBB) utilises comme jeu de
# demonstration quand les CSV KBO ne sont pas presents dans data/.
DEMO_HOTELS = [
    {"bce": "0533820890", "name": "SOCIETE HOTELIERE DE BRUXELLES ET DU NORD"},
    {"bce": "0448410115", "name": "HOTEL VAN BELLE"},
    {"bce": "0711984948", "name": "A&O HOSTEL AND HOTEL BRUXELLES"},
    {"bce": "0421169149", "name": "ATLAS HOTEL BRUSSELS"},
]

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
