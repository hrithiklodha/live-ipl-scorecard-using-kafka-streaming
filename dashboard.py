import streamlit as st
from pyspark.sql import SparkSession, functions as F
import time
from datetime import datetime

def init_spark():
    return SparkSession.builder \
        .appName("StreamlitApp") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
        .getOrCreate()

# Initialize Spark and UI
spark = init_spark()
st.set_page_config(layout="wide")
st.title("🏏 Live IPL Scorecard")

# Initialize session state for cumulative totals
if 'total_runs_cumulative' not in st.session_state:
    st.session_state.total_runs_cumulative = 0
if 'wickets_cumulative' not in st.session_state:
    st.session_state.wickets_cumulative = 0

# CSS for cards
st.markdown("""
<style>
.metric-card {
    padding: 20px;
    border-radius: 10px;
    background-color: #f0f2f6;
    margin: 10px 0;
}
.team-header {
    color: #1f77b4;
    font-size: 1.5rem !important;
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
        st.error(f"Error reading data: {str(e)}")
        return None

def safe_get(row, field, default=None):
    """Safely get a field from a Row object or return default"""
    try:
        return row[field] if field in row else default
    except:
        return default

def display_scorecard():
    stats = get_latest_stats()
    
    if not stats:
        st.warning("Waiting for match data...")
        return

    # Update cumulative totals
    current_runs = safe_get(stats, 'total_runs', 0)
    current_wickets = safe_get(stats, 'is_wicket', 0)
    
    # Only update if we have new data (not the same ball)
    if 'last_processed' not in st.session_state or st.session_state.last_processed != stats['processing_time']:
        st.session_state.total_runs_cumulative += current_runs
        st.session_state.wickets_cumulative += current_wickets
        st.session_state.last_processed = stats['processing_time']

    # Header with teams
    col1, col2 = st.columns(2)
    with col1:
        st.markdown(f"""
        <div class="metric-card">
            <div class="team-header">Batting Team</div>
            <div class="metric-value">{safe_get(stats, 'batting_team', 'N/A')}</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div class="metric-card">
            <div class="team-header">Bowling Team</div>
            <div class="metric-value">{safe_get(stats, 'bowling_team', 'N/A')}</div>
        </div>
        """, unsafe_allow_html=True)

    # Match metrics
    cols = st.columns(4)
    metrics = [
        ("📊 Total Runs", st.session_state.total_runs_cumulative, "#4CAF50"),
        ("🎯 Wickets", st.session_state.wickets_cumulative, "#F44336"), 
        ("🔢 Current Over", f"{safe_get(stats, 'over', 0)}.{safe_get(stats, 'ball', 0)}", "#2196F3"),
        ("⏱️ Last Updated", datetime.now().strftime("%H:%M:%S"), "#9C27B0")
    ]

    for i, (title, value, color) in enumerate(metrics):
        with cols[i]:
            st.markdown(f"""
            <div class="metric-card" style="border-left: 5px solid {color}">
                <div>{title}</div>
                <div class="metric-value">{value}</div>
            </div>
            """, unsafe_allow_html=True)

    # Batsman/Bowler info
    st.subheader("Current Players")
    player_cols = st.columns(3)
    player_metrics = [
        ("🧑 Batter", safe_get(stats, 'batter', 'N/A')),
        ("🎯 Bowler", safe_get(stats, 'bowler', 'N/A')),
        ("🧑 Non-Striker", safe_get(stats, 'non_striker', 'N/A'))
    ]
    
    for i, (title, player) in enumerate(player_metrics):
        with player_cols[i]:
            st.markdown(f"""
            <div class="metric-card">
                <div>{title}</div>
                <div class="metric-value">{player}</div>
            </div>
            """, unsafe_allow_html=True)

# Main app logic
if __name__ == "__main__":
    display_scorecard()
    
    # Auto-refresh every 5 seconds
    time.sleep(2)
    st.rerun()