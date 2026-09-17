import argparse
import csv
from pathlib import Path

import boto3


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    args = parser.parse_args()

    bucket = args.bucket
    prefix = "bronze/"
    key = "bronze/netflix_titles.csv"

    s3 = boto3.client("s3")

    # 1. bronze/ 아래 객체 목록과 크기 출력
    print(f"[1] list s3://{bucket}/{prefix}")

    response = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix,
    )

    for obj in response.get("Contents", []):
        print(f"    {obj['Key']}    {obj['Size']} bytes")

    # 2. netflix_titles.csv를 project/data/ 아래로 다운로드
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)

    local_path = data_dir / "netflix_titles.csv"

    print(f"[2] download s3://{bucket}/{key} -> {local_path}")

    s3.download_file(
        bucket,
        key,
        str(local_path),
    )

    print(f"    downloaded: {local_path.stat().st_size} bytes")

    # 3. CSV 레코드 수 출력 (헤더 제외)
    with local_path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as f:
        reader = csv.reader(f)
        next(reader, None)
        record_count = sum(1 for _ in reader)

    print(f"[3] CSV records: {record_count}")


if __name__ == "__main__":
    main()