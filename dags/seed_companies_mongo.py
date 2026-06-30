"""DAG d'initialisation : peuple MongoDB et prepare la State DB.

A lancer en premier (manuellement). Il :
1. cree les index MongoDB (referentiel + State DB) ;
2. charge les entreprises belges (CSV KBO ou jeu de demonstration) ;
3. verifie la connexion HDFS Bronze.
"""

from datetime import datetime
import sys

from airflow.decorators import dag, task

sys.path.append("/opt/airflow")


@dag(
    dag_id="seed_companies_mongo",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["bce", "bronze", "init", "mongodb"],
)
def seed_companies_mongo():

    @task
    def check_hdfs() -> str:
        from include import hdfs_utils

        hdfs_utils.check_connection()
        return "hdfs-ok"

    @task
    def init_state_db() -> str:
        from include import mongo_utils

        mongo_utils.ensure_indexes()
        return "indexes-ok"

    @task
    def load_companies() -> int:
        from include import seed

        return seed.seed_companies()

    [check_hdfs(), init_state_db()] >> load_companies()


seed_companies_mongo()
