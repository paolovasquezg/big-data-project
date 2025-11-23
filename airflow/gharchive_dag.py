from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import sys

# Composer guarda los DAGs y libs aquÃ­
sys.path.append("/home/airflow/gcs/dags")

from utils_bigdata import (
    procesar_dia,
    procesar_metricas,
    upload_outputs_to_gcs,
    upload_to_mongodb,
    calcular_kpis_global
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
    dag_id="gharchive_composer_v2",
    default_args=DEFAULT_ARGS,
    description="Pipeline GHArchive con limpieza, muestras y mÃ©tricas",
    # schedule_interval="0 7 * * *",  # 07:00 UTC = 02:00 PerÃº
    schedule = "0 7 * * *",
    start_date=datetime(2025, 1, 1),
    end_date = datetime(2025, 1, 9),
    catchup=True,
    max_active_runs=1,
    tags=["bigdata", "gharchive"],
) as dag:

    # Día a procesar: siempre el día anterior
    # fecha_template = "{{ macros.ds_add(ds), -1 }}"
    fecha_template = "{{ ds }}"

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

    t5_kpis_global = PythonOperator(
        task_id="calcular_kpis_global",
        python_callable=calcular_kpis_global
    )

    t1_procesar_dia >> t2_metricas >> t3_upload_gcs >> t4_mongo >> t5_kpis_global