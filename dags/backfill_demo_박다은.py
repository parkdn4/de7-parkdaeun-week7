from pathlib import Path

import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


OUTPUT_DIR = Path("/opt/airflow/backfill_output")


def write_daily_file(**context):
    logical_date = context["logical_date"]
    date_str = logical_date.in_timezone("Asia/Seoul").format("YYYY-MM-DD")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    file_path = OUTPUT_DIR / f"daily_{date_str}.txt"

    file_path.write_text(
        f"logical_date={date_str}\n",
        encoding="utf-8",
    )

    print(f"생성 파일: {file_path}")


with DAG(
    dag_id="backfill_demo_박다은",
    start_date=pendulum.datetime(2026, 9, 10, tz="Asia/Seoul"),
    schedule="@daily",
    catchup=True,
) as dag:

    write_daily_file_task = PythonOperator(
        task_id="write_daily_file",
        python_callable=write_daily_file,
    )