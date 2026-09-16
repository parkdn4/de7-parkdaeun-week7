from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator


def hello():
    return "Hello Airflow"


def goodbye():
    return "Goodbye Airflow"


with DAG(
    dag_id="sample_dag",
    start_date=datetime(2026, 9, 16),
    schedule="@daily",
    catchup=False,
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    hello_task = PythonOperator(
        task_id="hello_task",
        python_callable=hello
    )

    goodbye_task = PythonOperator(
        task_id="goodbye_task",
        python_callable=goodbye
    )

    end = EmptyOperator(
        task_id="end"
    )

start >> hello_task >> goodbye_task >> end >>