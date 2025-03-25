from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("IPLConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN") 

# Define schema for the incoming data
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

# Read data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "iplscores") \
    .load()

# Convert the value column from Kafka to a string
df = df.selectExpr("CAST(value AS STRING)")

# Parse the JSON data and apply the schema
df = df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Write the streaming data to console (for debugging)
query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()