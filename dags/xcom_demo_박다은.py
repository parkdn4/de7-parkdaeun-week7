from datetime import timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
import pendulum


def calculate_value(**context):
    if context["ti"].try_number == 1:
        raise ValueError("첫 번째 시도 실패 - retry 테스트")

    value = 42
    print(f"계산된 값: {value}")
    return value


def use_value(**context):
    value = context["ti"].xcom_pull(
        task_ids="calculate_value",
        key="return_value",
    )

    print(f"XCom으로 받은 값: {value}")


with DAG(
    dag_id="xcom_demo_박다은",
    start_date=pendulum.datetime(2026, 9, 17, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
) as dag:

    calculate_value_task = PythonOperator(
        task_id="calculate_value",
        python_callable=calculate_value,
        retries=2,
        retry_delay=timedelta(minutes=1),
    )

    use_value_task = PythonOperator(
        task_id="use_value",
        python_callable=use_value,
    )

    calculate_value_task >> use_value_task