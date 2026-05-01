from airflow import DAG
import pendulum
from datetime import timedelta, date
from api.video_stats import get_playlist_id, get_video_ids, get_video_details, save_to_json
from datawarehouse.dwh import staging_table, core_table
from dataquality.soda import yt_elt_data_quality
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


local_tz = pendulum.timezone("Europe/Malta")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
     "dagrun_timeout": timedelta(minutes=60),
    
    "email_on_failure": False,
    "email_on_retry": False,
    "email":"tanguturi.kranthi1008@gmail.com",
    "max_active_runs": 1,
    "start_date": pendulum.datetime(2026, 3, 6, tz=local_tz),
   
}

staging_schema = "staging"
core_schema = "core"

with DAG(
    dag_id = "youtube_video_stats",
    default_args=default_args,
    description="A DAG to fetch YouTube video stats and save them to a JSON file",
    schedule = "0 14 * * *",
    catchup=False,
) as dag_produce:
    

    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extracted_data = get_video_details(video_ids)
    save_to_json_task = save_to_json(extracted_data)

    trigger_update_db = TriggerDagRunOperator(
        task_id = "trigger_update_db",
        trigger_dag_id = "update_db",
    )

    playlist_id >> video_ids >> extracted_data >> save_to_json_task >> trigger_update_db

with DAG(
    dag_id = "update_db",
    default_args=default_args,
    description="DAG to update the staging and core tables in the data warehouse",
    schedule = None,
    catchup=False,
) as dag_update:
    

    update_staging = staging_table()
    update_core = core_table()

    trigger_data_quality = TriggerDagRunOperator(
        task_id = "trigger_data_quality",
        trigger_dag_id = "data_quality",
    )

    update_staging >> update_core >> trigger_data_quality


with DAG(
    dag_id = "data_quality",
    default_args = default_args,
    description = "DAG to run data quality checks on both layers in the db",
    schedule = None,
    catchup=False,
) as dag_update:
    

    soda_validation_staging = yt_elt_data_quality(staging_schema)
    soda_validation_core = yt_elt_data_quality(core_schema)


    soda_validation_staging >> soda_validation_core