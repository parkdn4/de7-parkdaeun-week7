import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, explode, split, trim


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--year", type=int, default=2015)
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("NetflixTransform")
        .getOrCreate()
    )

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .csv(args.input)
    )

    filtered_df = df.filter(
        col("release_year") >= args.year
    )

    genre_df = (
        filtered_df
        .withColumn("genre", explode(split(col("listed_in"), ",")))
        .withColumn("genre", trim(col("genre")))
    )

    result_df = (
        genre_df
        .groupBy("type", "genre")
        .agg(count("*").alias("title_count"))
        .orderBy("type", "genre")
    )

    aggregate_row_count = result_df.count()

    print(f"기준 연도: {args.year}")
    print(f"집계 행 수: {aggregate_row_count}")

    result_df.show(truncate=False)

    # parquet + snappy 저장
    (
        result_df.write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(args.output)
    )

    spark.stop()


if __name__ == "__main__":
    main()