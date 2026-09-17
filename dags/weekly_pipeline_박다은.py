import os
import subprocess
from datetime import datetime
from pathlib import Path

import boto3
import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


WORK_DIR = Path("/opt/airflow/data")
INPUT_FILE = WORK_DIR / "netflix_titles.csv"
OUTPUT_DIR = WORK_DIR / "silver_output"

TRANSFORM_SCRIPT = "/opt/airflow/dags/jobs/transform.py"


def get_bucket_name():
    bucket = os.environ.get("S3_BUCKET_NAME")

    if not bucket:
        raise ValueError("S3_BUCKET_NAME 환경변수가 설정되지 않았습니다.")

    return bucket


# Task 1: S3 bronze CSV 다운로드
def download_csv():
    bucket = get_bucket_name()

    WORK_DIR.mkdir(parents=True, exist_ok=True)

    s3 = boto3.client("s3")

    key = "bronze/netflix_titles.csv"

    print(f"다운로드: s3://{bucket}/{key}")
    print(f"저장 위치: {INPUT_FILE}")

    s3.download_file(
        bucket,
        key,
        str(INPUT_FILE),
    )

    print(f"다운로드 완료: {INPUT_FILE}")
    print(f"파일 크기: {INPUT_FILE.stat().st_size} bytes")


# Task 2: spark-submit 실행
def run_transform():
    command = [
        "spark-submit",
        TRANSFORM_SCRIPT,
        "--input",
        str(INPUT_FILE),
        "--output",
        str(OUTPUT_DIR),
        "--year",
        "2015",
    ]

    print("실행 명령:")
    print(" ".join(command))

    subprocess.run(
        command,
        check=True,
    )


# Task 3: 결과를 S3 silver/오늘날짜/ 업로드
def upload_silver():
    bucket = get_bucket_name()

    if not OUTPUT_DIR.exists():
        raise FileNotFoundError(f"Spark 결과가 없습니다: {OUTPUT_DIR}")

    today = datetime.now().strftime("%Y-%m-%d")
    prefix = f"silver/{today}/"

    s3 = boto3.client("s3")

    uploaded_keys = []

    for file_path in OUTPUT_DIR.iterdir():
        if file_path.is_file() and file_path.suffix == ".parquet":
            key = prefix + file_path.name

            s3.upload_file(
                str(file_path),
                bucket,
                key,
            )

            uploaded_keys.append(key)
            print(f"업로드 완료: s3://{bucket}/{key}")

    print("\n업로드된 객체 목록:")

    response = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix,
    )

    objects = response.get("Contents", [])

    for obj in objects:
        print(f"{obj['Key']} - {obj['Size']} bytes")

    print(f"업로드된 객체 개수: {len(objects)}")


with DAG(
    dag_id="weekly_pipeline_박다은",
    start_date=pendulum.datetime(2026, 9, 17, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
) as dag:

    download_csv_task = PythonOperator(
        task_id="download_csv",
        python_callable=download_csv,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=run_transform,
    )

    upload_silver_task = PythonOperator(
        task_id="upload_silver",
        python_callable=upload_silver,
    )

    download_csv_task >> transform_task >> upload_silver_task