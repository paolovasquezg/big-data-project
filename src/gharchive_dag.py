from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import sys

# Composer guarda los DAGs y libs aquí
sys.path.append("/home/airflow/gcs/dags")

from utils_bigdata import (
    procesar_dia,
    procesar_metricas,
    upload_outputs_to_gcs,
    upload_to_mongodb
)


DEFAULT_ARGS = {
    "owner": "isaac",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="gharchive_composer",
    default_args=DEFAULT_ARGS,
    description="Pipeline GHArchive con limpieza, muestras y métricas",
    schedule_interval="0 7 * * *",  # 07:00 UTC = 02:00 Perú
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["bigdata", "gharchive"],
) as dag:

    # Día a procesar: siempre el día anterior
    fecha_template = "{{ macros.ds_add(ds, -1) }}"

    t1_procesar_dia = PythonOperator(
        task_id="procesar_dia",
        python_callable=procesar_dia,
        op_kwargs={
            "fecha_input": fecha_template,
        },
    )

    t2_metricas = PythonOperator(
        task_id="procesar_metricas",
        python_callable=procesar_metricas,
        op_kwargs={
            "fecha_input": fecha_template,
        },
    )

    t3_upload_gcs = PythonOperator(
        task_id="upload_outputs",
        python_callable=upload_outputs_to_gcs,
        op_kwargs={
            "fecha": fecha_template,
            "bucket_name": "bigdata-gharchive-isaac",
        },
    )

    t4_mongo = PythonOperator(
        task_id="upload_to_mongodb",
        python_callable=upload_to_mongodb,
        op_kwargs={
            "fecha_input": fecha_template,
        },
    )

    t1_procesar_dia >> t2_metricas >> t3_upload_gcs >> t4_mongo
