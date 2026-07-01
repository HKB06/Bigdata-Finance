"""DAG Jour 2 : consolidation Bronze -> Silver -> ciblage & scraping hotellerie.

Flux :
1. build_finale   : fusionne les 5 CSV KBO (ou le jeu de demo) -> enterprise_finale ;
2. build_silver   : nettoie / enrichit                          -> enterprise_silver ;
3. filter_hotels  : filtre le secteur hotelier -> State DB hotellerie (pending) ;
4. scrape_hotels  : boucle sur tous les hotels pending et telecharge leurs CSV
                    NBB (>= 2021) vers HDFS Bronze (delta detection + reprise
                    sur 429 via la State DB, rotation Tor periodique) ;
5. report         : resume des collections et de la State DB.

Variables d'environnement utiles :
- SEED_LIMIT  : borne le nombre d'entreprises chargees dans enterprise_finale ;
- HOTEL_LIMIT : borne le nombre d'hotels scrapes par run.
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
    dag_id="jour2_silver_hotellerie",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    max_active_tasks=3,
    tags=["bce", "silver", "hotellerie", "nbb", "transformation"],
)
def jour2_silver_hotellerie():

    @task
    def build_finale() -> int:
        from include import build_enterprise_finale

        return build_enterprise_finale.build_enterprise_finale()

    @task
    def build_silver(_upstream: int) -> int:
        from include import silver

        return silver.build_silver()

    @task
    def filter_hotels(_upstream: int) -> int:
        from include import hotel

        return hotel.filter_hotels()

    @task(execution_timeout=timedelta(hours=12))
    def scrape_hotels(_upstream: int) -> dict:
        from include import hotel

        limit = int(os.getenv("HOTEL_LIMIT", "0")) or None
        return hotel.scrape_all_pending(limit=limit)

    @task(trigger_rule="all_done")
    def report() -> dict:
        from include import mongo_utils

        summary = {
            "enterprise_finale": mongo_utils.finale_collection().count_documents({}),
            "enterprise_silver": mongo_utils.silver_collection().count_documents({}),
            "hotel_state": mongo_utils.hotel_summary(),
            "files": mongo_utils.state_summary(),
        }
        print(f"Resume Jour 2 : {summary}")
        return summary

    finale = build_finale()
    silver_done = build_silver(finale)
    hotels_found = filter_hotels(silver_done)
    scraped = scrape_hotels(hotels_found)

    scraped >> report()


jour2_silver_hotellerie()
