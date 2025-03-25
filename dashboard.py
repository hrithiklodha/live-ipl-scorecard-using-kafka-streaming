import streamlit as st
from pyspark.sql import SparkSession, functions as F
import time
from datetime import datetime

def init_spark():
    return SparkSession.builder \
        .appName("StreamlitApp") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
        .getOrCreate()

# 🔹 Initialize Spark & Streamlit UI
spark = init_spark()
st.set_page_config(layout="wide")
st.title("🏏 Live IPL Scorecard")

# 🔹 Initialize Session State
if 'total_runs_cumulative' not in st.session_state:
    st.session_state.total_runs_cumulative = 0
if 'wickets_cumulative' not in st.session_state:
    st.session_state.wickets_cumulative = 0

# 🔹 CSS Styling for UI
st.markdown("""
<style>
.metric-card {
    padding: 15px;
    border-radius: 10px;
    background-color: #f0f2f6;
    margin: 10px 0;
    text-align: center;
}
.team-header {
    color: #1f77b4;
    font-size: 1.5rem !important;
    font-weight: bold;
}
.metric-value {
    color: #ff7f0e;
    font-size: 2rem !important;
    font-weight: bold;
}
</style>
""", unsafe_allow_html=True)

def get_latest_stats():
    try:
        df = spark.read.parquet("/tmp/bowling_teams_parquet")
        latest = df.orderBy(F.col("processing_time").desc()).limit(1)
        if latest.count() > 0:
            return latest.collect()[0]
        return None
    except Exception as e:
        st.error(f"❌ Error reading data: {str(e)}")
        return None

def safe_get(row, field, default="N/A"):
    """Safely get a field from a Row object or return default"""
    try:
        return row[field] if field in row else default
    except:
        return default

def display_scorecard():
    stats = get_latest_stats()
    
    if not stats:
        st.warning("⏳ Waiting for match data...")
        return

    # 🔹 Update Cumulative Totals
    if 'last_processed' not in st.session_state or st.session_state.last_processed != stats['processing_time']:
        st.session_state.total_runs_cumulative += safe_get(stats, 'total_runs', 0)
        st.session_state.wickets_cumulative += safe_get(stats, 'is_wicket', 0)
        st.session_state.last_processed = stats['processing_time']

    # 🔹 Display Batting & Bowling Teamsx
    col1, col2 = st.columns(2)
    col1.metric("🏏 Batting Team", safe_get(stats, 'batting_team'))
    col2.metric("🎯 Bowling Team", safe_get(stats, 'bowling_team'))

    # 🔹 Match Metrics
    cols = st.columns(4)
    cols[0].metric("📊 Total Runs", st.session_state.total_runs_cumulative)
    cols[1].metric("🎯 Wickets", st.session_state.wickets_cumulative)
    cols[2].metric("🔢 Over", f"{safe_get(stats, 'over', 0)}.{safe_get(stats, 'ball', 0)}")
    cols[3].metric("⏱️ Last Updated", datetime.now().strftime("%H:%M:%S"))

    # 🔹 Player Info
    st.subheader("⚡ Current Players")
    player_cols = st.columns(3)
    player_cols[0].metric("🧑 Batter", safe_get(stats, 'batter'))
    player_cols[1].metric("🎯 Bowler", safe_get(stats, 'bowler'))
    player_cols[2].metric("🧑 Non-Striker", safe_get(stats, 'non_striker'))

# 🔹 Run Dashboard
if __name__ == "__main__":
    display_scorecard()

    # 🔄 Auto-refresh every 5 seconds
    time.sleep(2)
    st.rerun()
