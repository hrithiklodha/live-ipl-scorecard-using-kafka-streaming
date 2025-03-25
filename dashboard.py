import streamlit as st
import pandas as pd
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
    StructField("match_id", IntegerType(), nullable=False),
    StructField("inning", IntegerType(), nullable=False),
    StructField("batting_team", StringType(), nullable=False),
    StructField("bowling_team", StringType(), nullable=False),
    StructField("over", IntegerType(), nullable=False),
    StructField("ball", IntegerType(), nullable=False),
    StructField("batter", StringType(), nullable=False),
    StructField("bowler", StringType(), nullable=False),
    StructField("non_striker", StringType(), nullable=False),
    StructField("batsman_runs", IntegerType(), nullable=True),
    StructField("extra_runs", IntegerType(), nullable=True),
    StructField("total_runs", IntegerType(), nullable=False),
    StructField("extras_type", StringType(), nullable=True),
    StructField("is_wicket", IntegerType(), nullable=True),
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

# Cast the relevant columns to their appropriate types
df = df.withColumn("batsman_runs", col("batsman_runs").cast(IntegerType()))
df = df.withColumn("extra_runs", col("extra_runs").cast(IntegerType()))
df = df.withColumn("total_runs", col("total_runs").cast(IntegerType()))
df = df.withColumn("is_wicket", col("is_wicket").cast(IntegerType()))

# Calculate total runs scored by each batsman
batsman_runs = df.groupBy("batter").sum("batsman_runs").withColumnRenamed("sum(batsman_runs)", "total_runs")

# Calculate the current score of the batting team
current_score = df.groupBy("batting_team", "bowling_team").sum("total_runs").withColumnRenamed("sum(total_runs)", "current_score")

# Calculate the number of wickets
wickets = df.filter(df.is_wicket == 1).groupBy("batting_team").count().withColumnRenamed("count", "wickets")

# Function to update the Streamlit dashboard
def update_dashboard(batsman_runs_df, current_score_df, wickets_df):
    st.title("IPL Live Scoreboard")

    # Display the teams playing
    st.header("Teams Playing")
    st.write(f"Batting Team: {current_score_df['batting_team'].iloc[0]}")
    st.write(f"Bowling Team: {current_score_df['bowling_team'].iloc[0]}")

    # Display the current score and wickets
    st.header("Current Score")
    st.write(f"Score: {current_score_df['current_score'].iloc[0]}")
    st.write(f"Wickets: {wickets_df['wickets'].iloc[0]}")

    # Display the batsman, bowler, and batsman runs
    st.header("Batsman and Bowler")
    st.write(f"Batsman: {batsman_runs_df['batter'].iloc[0]}")
    st.write(f"Bowler: {batsman_runs_df['bowler'].iloc[0]}")
    st.write(f"Batsman Runs: {batsman_runs_df['total_runs'].iloc[0]}")

# Write the results to a file and update the Streamlit dashboard
def process_batch(batch_df, batch_id):
    batsman_runs_df = batch_df.groupBy("batter").sum("batsman_runs").withColumnRenamed("sum(batsman_runs)", "total_runs").toPandas()
    current_score_df = batch_df.groupBy("batting_team", "bowling_team").sum("total_runs").withColumnRenamed("sum(total_runs)", "current_score").toPandas()
    wickets_df = batch_df.filter(batch_df.is_wicket == 1).groupBy("batting_team").count().withColumnRenamed("count", "wickets").toPandas()
    update_dashboard(batsman_runs_df, current_score_df, wickets_df)

query = df.writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()