import streamlit as st
from pyspark.sql import SparkSession
import time

# Initialize Spark with the SAME configurations as consumer
def init_spark():
    return SparkSession.builder \
        .appName("StreamlitApp") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
        .config("spark.ui.enabled", "false") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .enableHiveSupport() \
        .getOrCreate()

# Rest of your dashboard code remains the same...
st.title("🏏 IPL Live Score Dashboard")
st.header("Bowling Team Information")

# Initialize Spark
spark = init_spark()

# Display current tables for debugging
st.sidebar.subheader("Debug Info")
try:
    tables = spark.sql("SHOW TABLES").collect()
    st.sidebar.write("Available tables:", tables)
except Exception as e:
    st.sidebar.error(f"Error listing tables: {e}")

# Main display
placeholder = st.empty()

def get_bowling_team():
    try:
        df = spark.read.parquet("/tmp/bowling_teams_parquet") \
            .select("bowling_team", "processing_time") \
            .orderBy("processing_time", ascending=False) \
            .limit(1)

        if df.count() > 0:
            return df.collect()[0]["bowling_team"]
    except Exception as e:
        st.error(f"Error querying Spark: {e}")
        return None
    return None

# Update loop
while True:
    team = get_bowling_team()
    if team:
        placeholder.markdown(f"""
            <div style="background-color:#f0f2f6;padding:20px;border-radius:10px">
                <h2 style="color:#1f77b4;text-align:center;">Current Bowling Team</h2>
                <h1 style="color:#ff7f0e;text-align:center;">{team}</h1>
            </div>
        """, unsafe_allow_html=True)
    else:
        placeholder.warning("Waiting for bowling team data...")
    
    time.sleep(1)