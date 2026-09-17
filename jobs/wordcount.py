from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, explode, split


spark = (
    SparkSession.builder
    .appName("Q4_WordCount")
    .getOrCreate()
)

df = spark.read.text("/opt/spark/work-dir/data/wordcount.txt")

words = df.select(
    explode(split(col("value"), r"\s+")).alias("word")
)

word_counts = (
    words
    .filter(col("word") != "")
    .groupBy("word")
    .count()
    .orderBy(desc("count"))
)

word_counts.show(20, truncate=False)

spark.stop()