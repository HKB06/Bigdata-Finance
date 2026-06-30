FROM apache/airflow:2.10.4

# Dependances Python du pipeline d'ingestion Bronze.
# - requests / beautifulsoup4 / lxml : scraping des sources publiques
# - hdfs : client WebHDFS pour ecrire dans le data lake Bronze
# - pymongo : referentiel entreprises + State DB
# - python-dotenv : chargement de configuration locale
RUN pip install --no-cache-dir \
    requests==2.32.5 \
    beautifulsoup4==4.12.3 \
    lxml==5.3.0 \
    hdfs==2.7.3 \
    pymongo==4.10.1 \
    python-dotenv==1.0.1
