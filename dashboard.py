import streamlit as st
import redis
import json
import pandas as pd
import plotly.express as px
import time
import os
from datetime import datetime

# --- Configuration ---
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
TELEMETRY_STREAM = "telemetry-stream"
ACTIONS_STREAM = "healing-actions"
NUM_DATAPOINTS_TO_FETCH = 100

st.set_page_config(
    page_title="AIOps Heal Dashboard",
    layout="wide",
)

@st.cache_resource
def get_redis_connection():
    """Establishes a connection to Redis."""
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def fetch_stream_data(r_conn, stream_name, num_entries):
    """Fetches the latest N entries from a Redis stream."""
    response = r_conn.xrevrange(stream_name, count=num_entries)
    if not response:
        return []
    
    data = []
    for msg_id, msg_data in response:
        # The actual data is in a key within the message, e.g., 'data' or 'action'
        for key in msg_data:
            try:
                record = json.loads(msg_data[key])
                record['redis_id'] = msg_id # Keep for uniqueness
                data.append(record)
            except json.JSONDecodeError:
                continue # Skip malformed JSON
    return data

def main():
    """Main function to render the Streamlit dashboard."""
    st.title("🤖 AIOps Heal: Self-Healing System Dashboard")
    
    r = get_redis_connection()

    # --- Fetch Data ---
    telemetry_data = fetch_stream_data(r, TELEMETRY_STREAM, NUM_DATAPOINTS_TO_FETCH)
    actions_data = fetch_stream_data(r, ACTIONS_STREAM, 20)

    if not telemetry_data:
        st.warning("No telemetry data received yet. Please make sure the telemetry_generator is running.")
        st.stop()

    # --- Prepare DataFrames ---
    df = pd.DataFrame(telemetry_data)
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    df.sort_values('timestamp', inplace=True)
    
    df_anomalies = df[df['is_anomalous'] == True]

    # --- System Health Overview ---
    st.subheader("System Health Overview")
    latest_telemetry = df.drop_duplicates(subset='service_name', keep='last')
    
    cols = st.columns(len(latest_telemetry))
    for i, (idx, row) in enumerate(latest_telemetry.iterrows()):
        with cols[i]:
            if row['is_anomalous']:
                st.metric(label=row['service_name'], value="Unhealthy", delta="Anomaly Detected", delta_color="inverse")
            else:
                st.metric(label=row['service_name'], value="Healthy", delta="Nominal", delta_color="normal")

    # --- Telemetry Charts ---
    st.subheader("Real-time Service Metrics")
    
    metric_to_plot = st.selectbox(
        "Select a metric to visualize:",
        ["cpu_utilization", "memory_usage", "network_latency", "error_count", "request_count"]
    )

    fig = px.line(df, x='timestamp', y=metric_to_plot, color='service_name', 
                  title=f"{metric_to_plot.replace('_', ' ').title()} Over Time",
                  markers=False)
    
    # --- Improve Readability ---
    fig.update_traces(line=dict(width=3))

    # Add anomaly markers
    if not df_anomalies.empty:
        fig.add_scatter(x=df_anomalies['timestamp'], y=df_anomalies[metric_to_plot], 
                        mode='markers', name='Anomalies',
                        marker=dict(color='red', size=12, symbol='x-thin', line=dict(width=3)))

    fig.update_layout(xaxis_title="Time", yaxis_title="Value", legend_title="Service")
    st.plotly_chart(fig, use_container_width=True)

    # --- Recent Actions Log ---
    st.subheader("Recent Self-Healing Actions")
    
    actions_placeholder = st.empty()
    
    df_actions = pd.DataFrame(actions_data)
    
    if not df_actions.empty:
        df_actions['timestamp'] = pd.to_datetime(df_actions['timestamp'], unit='s')
        df_actions.sort_values('timestamp', ascending=False, inplace=True)
        
        actions_placeholder.table(df_actions[['timestamp', 'service_name', 'action_taken']].rename(columns={
            "timestamp": "Time of Action",
            "service_name": "Affected Service",
            "action_taken": "Action Taken"
        }))
    else:
        actions_placeholder.info("No healing actions have been taken yet.")

    # Auto-refresh
    time.sleep(5)
    st.rerun()


if __name__ == "__main__":
    main() 