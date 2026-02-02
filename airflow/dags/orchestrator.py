from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys


sys.path.append('/opt/airflow/api_request')
def safe_main_callable():
    from insert_record import main
    return main()

def example_function1():
    print("This is example function 1")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 24),
    "catchup": False,
}

dag = DAG(
    dag_id="nyc311_orchestrator",
    default_args=default_args,
    schedule=timedelta(minutes=10),
    catchup=False,

)

with dag:
    task1 = PythonOperator(
        task_id="ingest_data_from_socrata",
        python_callable=safe_main_callable)