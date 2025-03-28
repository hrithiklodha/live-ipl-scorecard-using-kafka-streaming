from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import *
import os
import shutil

shutil.rmtree("/tmp/bowling_teams_parquet", ignore_errors=True)
shutil.rmtree("/tmp/bowling_teams_checkpoint", ignore_errors=True)
spark = SparkSession.builder \
    .appName("iplscores") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .config("spark.sql.parquet.compression.codec", "uncompressed") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()
    
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("match_id", StringType(), nullable=False),
    StructField("inning", StringType(), nullable=False),
    StructField("batting_team", StringType(), nullable=False),
    StructField("bowling_team", StringType(), nullable=False),
    StructField("over", StringType(), nullable=False),
    StructField("ball", StringType(), nullable=False),
    StructField("batter", StringType(), nullable=False),
    StructField("bowler", StringType(), nullable=False),
    StructField("non_striker", StringType(), nullable=False),
    StructField("batsman_runs", StringType(), nullable=True),
    StructField("extra_runs", StringType(), nullable=True),
    StructField("total_runs", StringType(), nullable=False),
    StructField("extras_type", StringType(), nullable=True),
    StructField("is_wicket", StringType(), nullable=True),
    StructField("player_dismissed", StringType(), nullable=True),
    StructField("dismissal_kind", StringType(), nullable=True),
    StructField("fielder", StringType(), nullable=True)
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "iplscores") \
    .load()

parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("processing_time", current_timestamp())

numeric_fields = ["match_id", "inning", "over", "ball", 
                "batsman_runs", "extra_runs", "total_runs", "is_wicket"]
for field in numeric_fields:
    parsed_df = parsed_df.withColumn(field, col(field).cast(IntegerType()))
import shutil
import os

checkpoint_dir = "/tmp/bowling_teams_checkpoint"
if os.path.exists(checkpoint_dir):
    shutil.rmtree(checkpoint_dir)
query = parsed_df.writeStream \
    .format("parquet") \
    .option("path", "/tmp/bowling_teams_parquet") \
    .option("checkpointLocation", "/tmp/bowling_teams_checkpoint") \
    .trigger(processingTime="5 seconds") \
    .outputMode("append") \
    .start()
print("Spark streaming query started. Waiting for termination...")
query.awaitTermination()