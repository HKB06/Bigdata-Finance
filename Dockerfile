FROM apache/airflow:2.10.4

# Dependances systeme pour Playwright (acquisition du cookie notaire/stapor).
# xvfb permet de lancer Chromium en mode "non headless" sous un display virtuel.
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    xvfb wget gnupg ca-certificates \
    && rm -rf /var/lib/apt/lists/*
USER airflow

# Dependances Python du pipeline d'ingestion Bronze.
# - requests[socks] : scraping + support des proxies SOCKS5 (Tor)
# - beautifulsoup4 / lxml : parsing HTML
# - hdfs : client WebHDFS pour ecrire dans le data lake Bronze
# - pymongo : referentiel entreprises + State DB
# - playwright / pyvirtualdisplay : cookie anti-bot pour stapor (notaire)
# - python-dotenv : configuration locale
RUN pip install --no-cache-dir \
    "requests[socks]==2.32.5" \
    beautifulsoup4==4.12.3 \
    lxml==5.3.0 \
    hdfs==2.7.3 \
    pymongo==4.10.1 \
    python-dotenv==1.0.1 \
    pyvirtualdisplay==3.0 \
    playwright==1.49.1

# Navigateur Chromium pour Playwright.
USER root
RUN /home/airflow/.local/bin/playwright install-deps chromium
USER airflow
RUN playwright install chromium
