import redis
import time
import json
import os
import random

# --- Configuration ---
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
INPUT_STREAM = "anomaly-alerts"
OUTPUT_STREAM = "healing-actions"
STATE_STREAM = "system-state-stream" # Stream for state changes
CONSUMER_GROUP = "orchestration-group"
CONSUMER_NAME = "orchestrator-1"
ACTION_COOLDOWN_SECONDS = 30 # Cooldown period for each service
DEPENDENCY_MAP = {
    "service-c": "service-a",
    "service-d": "service-a"
}
RECENT_ANOMALY_WINDOW_SECONDS = 5 # How long to consider an anomaly "active" for correlation

# --- State ---
# A sophisticated state machine to track the healing lifecycle
service_states = {
    service: {"state": "HEALTHY", "last_action_time": 0, "last_action": None, "verification_time": 0}
    for service in ["service-a", "service-b", "service-c", "service-d"]
}
recent_anomalies = {}


def get_redis_connection():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def initialize_stream_and_group(r):
    try:
        r.xgroup_create(INPUT_STREAM, CONSUMER_GROUP, id="0", mkstream=True)
        print(f"Consumer group '{CONSUMER_GROUP}' created for stream '{INPUT_STREAM}'.")
    except redis.exceptions.ResponseError as e:
        if "already exists" in str(e):
            print(f"Consumer group '{CONSUMER_GROUP}' already exists.")
        else:
            raise

def decide_action(alert):
    """A simple rule-based engine to decide on a recovery action."""
    service_name = alert.get("service_name")
    metrics = alert.get("metrics", {})
    
    if metrics.get("cpu_utilization", 0) > 70:
        return f"Restarting pod for {service_name} due to high CPU.", None
    elif metrics.get("memory_usage", 0) > 1200:
        return f"Scaling up memory for {service_name} due to high memory usage.", None
    elif metrics.get("error_count", 0) > 30:
        # Command to isolate the service for 60 seconds
        command = {"service_name": service_name, "state": "ISOLATED", "duration_seconds": 60}
        return f"Isolating {service_name} for investigation due to high error rate.", command
    elif metrics.get("network_latency", 0) > 90:
        # Command to reroute traffic away from the service for 45 seconds
        command = {"service_name": service_name, "state": "REROUTING", "duration_seconds": 45}
        return f"Rerouting traffic from {service_name} due to high latency.", command
    
    return f"No specific action defined. Monitoring {service_name} closely.", None

def process_alert(alert_data, r):
    global service_states
    try:
        alert = json.loads(alert_data["alert"])
        service_name = alert.get("service_name")
        if not service_name: return

        print(f"Received alert: {alert}")
        
        action_string, state_command = decide_action(alert)
        
        if state_command:
            r.xadd(STATE_STREAM, {"command": json.dumps(state_command)})
            print(f"[STATE CHANGE] Issued command: {state_command}")

        # Log the action and update state to VERIFYING_FIX
        if action_string:
            print(f"[ACTION] {action_string}")
            service_states[service_name]["state"] = "VERIFYING_FIX"
            service_states[service_name]["last_action_time"] = time.time()
            service_states[service_name]["last_action"] = action_string
            service_states[service_name]["verification_time"] = time.time() + 45 # Check in 45s
        
        # Publish the action to the healing-actions stream for logging
        action_log = {
            "service_name": service_name,
            "timestamp": time.time(),
            "action_taken": action_string,
            "triggering_alert": alert
        }
        r.xadd(OUTPUT_STREAM, {"action": json.dumps(action_log)})

    except (json.JSONDecodeError, KeyError) as e:
        print(f"Error processing alert: {e}")

def verify_actions(r):
    """Checks if recent healing actions were successful."""
    global service_states
    current_time = time.time()
    
    for service, s_state in service_states.items():
        if s_state["state"] == "VERIFYING_FIX" and current_time >= s_state["verification_time"]:
            # Check Redis for the latest anomaly for this service
            if service in recent_anomalies and current_time - recent_anomalies[service] < 45:
                # Failure: Anomaly is still active after the action
                print(f"[FAILURE] Action '{s_state['last_action']}' for {service} FAILED. Anomaly is still present. Escalating.")
                # Reset state but could trigger a real alert here
                s_state["state"] = "HEALTHY" 
                s_state["last_action_time"] = time.time() # Start a longer cooldown after failure
            else:
                # No recent anomalies for this service
                print(f"[SUCCESS] Action '{s_state['last_action']}' for {service} was successful. Service is now stable.")
                s_state["state"] = "HEALTHY"

def main():
    """Main function to orchestrate self-healing actions."""
    r = get_redis_connection()
    print("Self-healing orchestrator started. Connecting to Redis...")
    r.ping()
    print("Redis connection successful.")
    initialize_stream_and_group(r)
    
    print("Waiting for anomaly alerts...")
    while True:
        verify_actions(r) # Check on ongoing fixes

        # Prune old anomalies from our state to keep it current
        current_time = time.time()
        for service, timestamp in list(recent_anomalies.items()):
            if current_time - timestamp > RECENT_ANOMALY_WINDOW_SECONDS:
                del recent_anomalies[service]

        messages = r.xreadgroup(
            CONSUMER_GROUP,
            CONSUMER_NAME,
            {INPUT_STREAM: ">"},
            count=1,
            block=5000
        )
        
        if messages:
            for stream, msgs in messages:
                for msg_id, msg_data in msgs:
                    alert = json.loads(msg_data.get("alert", "{}"))
                    service_name = alert.get("service_name")
                    
                    if not service_name:
                        r.xack(INPUT_STREAM, CONSUMER_GROUP, msg_id)
                        continue

                    # Log the anomaly for state tracking
                    recent_anomalies[service_name] = time.time()

                    # --- ROOT CAUSE ANALYSIS LOGIC ---
                    upstream_dependency = DEPENDENCY_MAP.get(service_name)
                    if upstream_dependency and upstream_dependency in recent_anomalies:
                        print(f"[RCA] Alert on {service_name} is likely a symptom of an issue with upstream {upstream_dependency}. Ignoring alert for now.")
                        r.xack(INPUT_STREAM, CONSUMER_GROUP, msg_id)
                        continue

                    # --- COOLDOWN & STATE LOGIC ---
                    s_state = service_states[service_name]
                    if s_state["state"] != "HEALTHY":
                        print(f"INFO: Service {service_name} is currently in state '{s_state['state']}'. Ignoring new alert.")
                        r.xack(INPUT_STREAM, CONSUMER_GROUP, msg_id)
                        continue

                    if time.time() - s_state["last_action_time"] < ACTION_COOLDOWN_SECONDS:
                        print(f"INFO: Cooldown active for {service_name}. Ignoring alert.")
                        r.xack(INPUT_STREAM, CONSUMER_GROUP, msg_id)
                        continue

                    process_alert(msg_data, r)
                    
                    r.xack(INPUT_STREAM, CONSUMER_GROUP, msg_id)

if __name__ == "__main__":
    main() 