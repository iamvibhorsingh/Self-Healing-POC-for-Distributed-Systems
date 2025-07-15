import redis
import time
import json
import pandas as pd
from sklearn.ensemble import IsolationForest
import os

# --- Configuration ---
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
INPUT_STREAM = "telemetry-stream"
OUTPUT_STREAM = "anomaly-alerts"
CONSUMER_GROUP = "detection-group"
CONSUMER_NAME = "detector-1"
TRAINING_WINDOW_SIZE = 50
MIN_DATA_FOR_TRAINING = 20

# --- State ---
# Dictionary to store dataframes and models for each service
service_data = {}

def get_redis_connection():
    """Establishes a connection to Redis."""
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def initialize_stream_and_group(r):
    """Creates the consumer group if it doesn't exist."""
    try:
        r.xgroup_create(INPUT_STREAM, CONSUMER_GROUP, id="0", mkstream=True)
        print(f"Consumer group '{CONSUMER_GROUP}' created for stream '{INPUT_STREAM}'.")
    except redis.exceptions.ResponseError as e:
        if "already exists" in str(e):
            print(f"Consumer group '{CONSUMER_GROUP}' already exists.")
        else:
            raise

def process_message(message, r):
    """Processes a single telemetry message."""
    global service_data
    
    try:
        data = json.loads(message["data"])
        service_name = data["service_name"]
        
        # Initialize storage for a new service
        if service_name not in service_data:
            service_data[service_name] = {
                "df": pd.DataFrame(columns=["cpu_utilization", "memory_usage", "network_latency", "error_count", "request_count"]),
                "model": None
            }

        s_data = service_data[service_name]
        
        # Append new data
        new_row = pd.DataFrame([data], columns=s_data["df"].columns)
        s_data["df"] = pd.concat([s_data["df"], new_row], ignore_index=True)

        # Keep dataframe at a max size (sliding window)
        if len(s_data["df"]) > TRAINING_WINDOW_SIZE:
            s_data["df"] = s_data["df"].iloc[-TRAINING_WINDOW_SIZE:]

        df = s_data["df"]

        # --- IMPROVED TRAINING LOGIC ---
        # Train the model only once when enough data is collected.
        if s_data["model"] is None and len(df) >= MIN_DATA_FOR_TRAINING:
            print(f"Collected enough data ({len(df)} points). Training initial model for {service_name}...")
            model = IsolationForest(contamination='auto', random_state=42)
            model.fit(df)
            s_data["model"] = model
            print(f"Initial model for {service_name} trained and frozen.")

        # Detect anomalies using the frozen model.
        if s_data["model"]:
            prediction = s_data["model"].predict(df.tail(1))
            if prediction[0] == -1:
                # Anomaly detected
                anomaly_score = s_data["model"].decision_function(df.tail(1))[0]
                alert = {
                    "service_name": service_name,
                    "timestamp": time.time(),
                    "anomaly_score": round(anomaly_score, 2),
                    "metrics": data,
                    "reason": f"Isolation Forest detected an outlier."
                }
                r.xadd(OUTPUT_STREAM, {"alert": json.dumps(alert)})
                print(f"ALERT: Anomaly detected in {service_name}: {alert}")

    except (json.JSONDecodeError, KeyError) as e:
        print(f"Error processing message: {e}")

def main():
    """Main function to detect anomalies."""
    r = get_redis_connection()
    print("Anomaly detector started. Connecting to Redis...")
    r.ping()
    print("Redis connection successful.")
    initialize_stream_and_group(r)
    
    print("Waiting for telemetry data...")
    while True:
        # Read from the stream using the consumer group
        messages = r.xreadgroup(
            CONSUMER_GROUP,
            CONSUMER_NAME,
            {INPUT_STREAM: ">"},
            count=10,
            block=2000 # Block for 2 seconds
        )
        
        if messages:
            for stream, msgs in messages:
                for msg_id, msg_data in msgs:
                    process_message(msg_data, r)
                    r.xack(INPUT_STREAM, CONSUMER_GROUP, msg_id)

if __name__ == "__main__":
    main() 