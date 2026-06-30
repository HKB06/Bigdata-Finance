"""DAG principal d'ingestion Bronze.

Flux :
1. lit les entreprises depuis MongoDB ;
2. pour chaque entreprise, telecharge en parallele :
   - les comptes annuels NBB/CBSO (CSV + PDF) ;
   - les publications eJustice (PDF) ;
3. chaque telechargement consulte la State DB (delta detection) puis la met
   a jour -> on ne re-telecharge jamais un fichier deja present dans Bronze ;
4. publie un resume de l'etat de la State DB.

La variable d'environnement `INGEST_LIMIT` borne le nombre d'entreprises
traitees par run (utile pour les demos / tests).
"""

from datetime import datetime, timedelta
import os
import sys

from airflow.decorators import dag, task

sys.path.append("/opt/airflow")

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="ingestion_bronze",
    schedule="@monthly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    max_active_tasks=4,
    tags=["bce", "bronze", "ingestion", "nbb", "ejustice", "hdfs"],
)
def ingestion_bronze():

    @task
    def list_companies() -> list[str]:
        from include import mongo_utils

        limit = int(os.getenv("INGEST_LIMIT", "10"))
        numbers = mongo_utils.iter_company_bce(limit=limit)
        print(f"{len(numbers)} entreprises a traiter (limite={limit}).")
        return numbers

    @task
    def ingest_nbb(bce: str) -> dict:
        from include import sources

        return sources.ingest_nbb(bce)

    @task
    def ingest_ejustice(bce: str) -> dict:
        from include import sources

        return sources.ingest_ejustice(bce)

    @task(trigger_rule="all_done")
    def report() -> dict:
        from include import mongo_utils

        summary = mongo_utils.state_summary()
        print(f"Etat de la State DB : {summary}")
        return summary

    companies = list_companies()

    nbb = ingest_nbb.expand(bce=companies)
    ejustice = ingest_ejustice.expand(bce=companies)

    [nbb, ejustice] >> report()


ingestion_bronze()
