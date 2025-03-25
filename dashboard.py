
import streamlit as st
from pyspark.sql import SparkSession, functions as F
import time
from datetime import datetime
def init_spark():
    return SparkSession.builder \
        .appName("StreamlitApp") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
        .getOrCreate()
spark = init_spark()
st.set_page_config(layout="wide")
st.title("🏏 Live IPL Scorecard")
if 'match_data' not in st.session_state:
    st.session_state.match_data = {
        'current_innings': 1,
        'innings_scores': {},
        'total_runs': 0,
        'wickets': 0,
        'last_processed': None,
        'batting_team': None,
        'bowling_team': None,
        'batsmen_scores': {},
        'dismissed_batsmen': []
    }
st.markdown("""
<style>
.metric-card {
    padding: 15px;
    border-radius: 10px;
    background-color:
    margin: 10px 0;
    text-align: center;
}
.team-header {
    color:
    font-size: 1.5rem !important;
    font-weight: bold;
}
.metric-value {
    color:
    font-size: 2rem !important;
    font-weight: bold;
}
.innings-card {
    padding: 10px;
    border-radius: 8px;
    background-color:
    margin: 8px 0;
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
def safe_get(row, field, default=None):
    try:
        return row[field] if field in row else default
    except:
        return default
def update_innings_data(stats):
    current_innings = safe_get(stats, 'inning', 1)
    batting_team = safe_get(stats, 'batting_team')
    bowling_team = safe_get(stats, 'bowling_team')
    if st.session_state.match_data['batting_team'] is None:
        st.session_state.match_data['batting_team'] = batting_team
        st.session_state.match_data['bowling_team'] = bowling_team
    if current_innings != st.session_state.match_data['current_innings']:
        innings_key = f"Innings {st.session_state.match_data['current_innings']}"
        st.session_state.match_data['innings_scores'][innings_key] = {
            'team': st.session_state.match_data['batting_team'],
            'score': st.session_state.match_data['total_runs'],
            'wickets': st.session_state.match_data['wickets']
        }
        st.session_state.match_data['current_innings'] = current_innings
        st.session_state.match_data['total_runs'] = 0
        st.session_state.match_data['wickets'] = 0
        st.session_state.match_data['batting_team'] = batting_team
        st.session_state.match_data['bowling_team'] = bowling_team
        st.session_state.match_data['batsmen_scores'] = {}
        st.session_state.match_data['dismissed_batsmen'] = []
    if stats['processing_time'] != st.session_state.match_data.get('last_processed'):
        st.session_state.match_data['total_runs'] += safe_get(stats, 'total_runs', 0)

        batter = safe_get(stats, 'batter')
        batsman_runs = safe_get(stats, 'batsman_runs', 0)
        if batter:
            if batter not in st.session_state.match_data['batsmen_scores']:
                st.session_state.match_data['batsmen_scores'][batter] = 0
            st.session_state.match_data['batsmen_scores'][batter] += batsman_runs

        if safe_get(stats, 'is_wicket', 0):
            st.session_state.match_data['wickets'] += 1
            dismissed_player = safe_get(stats, 'player_dismissed')
            if dismissed_player:
                final_score = st.session_state.match_data['batsmen_scores'].get(dismissed_player, 0)
                st.session_state.match_data['dismissed_batsmen'].append({
                    'name': dismissed_player,
                    'runs': final_score
                })
        st.session_state.match_data['last_processed'] = stats['processing_time']
def display_scorecard():
    stats = get_latest_stats()
    if not stats:
        st.warning("⏳ Waiting for match data...")
        return
    update_innings_data(stats)
    if st.session_state.match_data['innings_scores']:
        st.subheader("📊 Innings Summary")
        for innings, data in st.session_state.match_data['innings_scores'].items():
            st.markdown(f"""
            <div class="innings-card">
                <strong>{innings}:</strong> {data['team']} -
                <span class="metric-value">{data['score']}/{data['wickets']}</span>
            </div>
            """, unsafe_allow_html=True)
    st.subheader("🏟️ Current Match")
    col1, col2 = st.columns(2)
    col1.metric("Batting Team", st.session_state.match_data['batting_team'])
    col2.metric("Bowling Team", st.session_state.match_data['bowling_team'])
    cols = st.columns(4)
    cols[0].metric("Total Runs", st.session_state.match_data['total_runs'])
    cols[1].metric("Wickets", st.session_state.match_data['wickets'])
    cols[2].metric("Current Over", f"{safe_get(stats, 'over', 0)}.{safe_get(stats, 'ball', 0)}")
    cols[3].metric("Last Updated", datetime.now().strftime("%H:%M:%S"))
    st.subheader("🏃 Current Players")
    player_cols = st.columns(3)

    current_batter = safe_get(stats, 'batter', '-')
    batter_runs = st.session_state.match_data['batsmen_scores'].get(current_batter, 0)
    player_cols[0].metric("Batter", f"{current_batter} ({batter_runs}*)")
    player_cols[1].metric("Bowler", safe_get(stats, 'bowler', '-'))
    non_striker = safe_get(stats, 'non_striker', '-')
    non_striker_runs = st.session_state.match_data['batsmen_scores'].get(non_striker, 0)
    player_cols[2].metric("Non-Striker", f"{non_striker} ({non_striker_runs}*)")

    if st.session_state.match_data['dismissed_batsmen']:
        st.subheader("🏏 Dismissed Batsmen")
        dismissed_data = {
            'Batsman': [b['name'] for b in st.session_state.match_data['dismissed_batsmen']],
            'Runs': [b['runs'] for b in st.session_state.match_data['dismissed_batsmen']]
        }
        st.table(dismissed_data)
if __name__ == "__main__":
    display_scorecard()
    time.sleep(2)
    st.rerun()