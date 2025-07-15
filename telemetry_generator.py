import redis
import time
import json
import random
import numpy as np
import os

# --- Configuration ---
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
STREAM_NAME = "telemetry-stream"
STATE_STREAM = "system-state-stream" # Listen for state changes
SERVICES = ["service-a", "service-b", "service-c", "service-d"]
UPDATE_INTERVAL_SECONDS = 5
DEPENDENCY_MAP = {
    "service-c": "service-a", # c depends on a
    "service-d": "service-a"  # d depends on a
}

# --- Anomaly State ---
# Refactored to be a dictionary, with one entry per service.
# This allows for concurrent anomalies on different services.
anomaly_states = {
    service: {"type": None, "duration": 0, "original_value": 0, "steps": 0}
    for service in SERVICES
}

# --- Impairment State (for reacting to healing actions) ---
service_impairments = {
    service: {"state": "HEALTHY", "expires_at": 0} for service in SERVICES
}

def get_redis_connection():
    """Establishes a connection to Redis."""
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def generate_normal_telemetry(service_name):
    """Generates normal telemetry data for a given service."""
    return {
        "service_name": service_name,
        "timestamp": time.time(),
        "cpu_utilization": round(random.uniform(20, 40) + hash(service_name) % 10, 2),
        "memory_usage": round(random.uniform(500, 1000) + hash(service_name) % 100, 2),
        "network_latency": round(random.uniform(50, 80), 2),
        "error_count": random.randint(0, 5),
        "request_count": random.randint(100, 200) # New metric
    }

def inject_anomaly(data):
    """Injects an anomaly into the telemetry data based on the current state."""
    service_name = data["service_name"]
    state = anomaly_states[service_name]

    if state["type"] is not None:
        if state["type"] == "cpu_spike":
            data["cpu_utilization"] = round(state["original_value"] * 2.5, 2)
        
        elif state["type"] == "memory_leak":
            leak_increase = state["original_value"] * 0.1 * state["steps"]
            data["memory_usage"] = round(state["original_value"] + leak_increase, 2)
            state["steps"] += 1
            
        elif state["type"] == "high_errors":
            data["error_count"] = random.randint(50, 100)
            
        elif state["type"] == "network_spike":
            data["network_latency"] = round(state["original_value"] * 2, 2)

        elif state["type"] == "request_spike":
            data["request_count"] *= 50 # 50x the normal request count
            
        state["duration"] -= 1
        if state["duration"] <= 0:
            print(f"INFO: Anomaly '{state['type']}' finished for {service_name}.")
            # Reset state for this specific service
            anomaly_states[service_name] = {"type": None, "duration": 0, "original_value": 0, "steps": 0}
        return True
    return False

def start_new_anomalies():
    """Randomly starts a new anomaly for any service that is currently normal."""
    for service in SERVICES:
        state = anomaly_states[service]
        # If the service is normal, there's a small chance of starting an anomaly
        if state["type"] is None and random.random() < 0.05: # 5% chance each cycle per service
            anomaly_type = random.choice(["cpu_spike", "memory_leak", "high_errors", "network_spike", "request_spike"])
            
            state["type"] = anomaly_type
            
            if anomaly_type == "cpu_spike":
                state["duration"] = random.randint(3, 5)
                state["original_value"] = random.uniform(30, 40)
            elif anomaly_type == "memory_leak":
                state["duration"] = random.randint(10, 15)
                state["original_value"] = random.uniform(600, 800)
                state["steps"] = 1
            elif anomaly_type == "high_errors":
                state["duration"] = random.randint(5, 10)
            elif anomaly_type == "network_spike":
                state["duration"] = random.randint(4, 8)
                state["original_value"] = random.uniform(50, 80)
            
            elif anomaly_type == "request_spike":
                state["duration"] = random.randint(5, 10)

            print(f"INFO: Starting anomaly '{anomaly_type}' on {service} for {state['duration']} cycles.")

def check_for_state_commands(r):
    """Does a non-blocking check for new commands from the orchestrator."""
    messages = r.xread({STATE_STREAM: "$"}, count=10, block=1)
    if messages:
        for stream, msgs in messages:
            for msg_id, msg_data in msgs:
                try:
                    command = json.loads(msg_data["command"])
                    service = command["service_name"]
                    state = command["state"]
                    duration = command["duration_seconds"]
                    
                    if service in service_impairments:
                        service_impairments[service]["state"] = state
                        service_impairments[service]["expires_at"] = time.time() + duration
                        print(f"INFO: Acknowledged command. Setting {service} to {state} for {duration}s.")
                except (json.JSONDecodeError, KeyError) as e:
                    print(f"Error processing state command: {e}")

def propagate_cascading_failures():
    """Simulates cascading failures based on the dependency map."""
    for downstream, upstream in DEPENDENCY_MAP.items():
        upstream_state = anomaly_states[upstream]
        downstream_state = anomaly_states[downstream]

        # If the upstream has a critical anomaly and the downstream is healthy
        if upstream_state["type"] in ["high_errors", "network_spike"] and downstream_state["type"] is None:
            print(f"INFO: CASCADING FAILURE! {upstream}'s problem is causing high errors in {downstream}.")
            # Trigger a 'high_errors' anomaly on the downstream service
            downstream_state["type"] = "high_errors"
            downstream_state["duration"] = upstream_state["duration"] # The cascade lasts as long as the parent problem
            downstream_state["steps"] = 1 # To satisfy the logic in inject_anomaly

def main():
    """Main function to generate and publish telemetry."""
    r = get_redis_connection()
    print("Telemetry generator started. Connecting to Redis...")
    r.ping()
    print("Redis connection successful.")

    while True:
        start_new_anomalies()
        propagate_cascading_failures() # Check for and create cascading failures
        check_for_state_commands(r) # Check for new commands each cycle

        # Check for any active rerouting to apply load to other services
        is_any_rerouting = any(
            s["state"] == "REROUTING" and time.time() < s["expires_at"] 
            for s in service_impairments.values()
        )
        load_factor = 1.2 if is_any_rerouting else 1.0 # 20% extra load

        for service in SERVICES:
            impairment = service_impairments[service]
            
            # Clear expired impairments
            if time.time() >= impairment["expires_at"]:
                if impairment["state"] != "HEALTHY":
                    print(f"INFO: {service} state has returned to HEALTHY.")
                    impairment["state"] = "HEALTHY"

            # --- Apply Impairment Logic ---
            if impairment["state"] == "ISOLATED":
                print(f"INFO: {service} is ISOLATED. Skipping telemetry generation.")
                continue # Skip telemetry for isolated services

            data = generate_normal_telemetry(service)

            # 1. Inject anomaly FIRST
            is_anomalous = inject_anomaly(data)
            data["is_anomalous"] = is_anomalous 

            # 2. Apply impairment state SECOND (as the final override)
            if impairment["state"] == "REROUTING":
                # Service is rerouted, so its metrics should look unusually healthy
                data["cpu_utilization"] *= 0.5
                data["network_latency"] *= 0.5
                data["request_count"] = 0 # Traffic is rerouted away completely
            elif impairment["state"] == "HEALTHY" and load_factor > 1.0:
                 # This service is healthy but taking on extra load
                data["cpu_utilization"] *= load_factor
                data["memory_usage"] *= load_factor
                data["request_count"] *= int(load_factor * 1.5)

            # Publish to Redis Stream
            r.xadd(STREAM_NAME, {"data": json.dumps(data)})
            print(f"Published: {data}")

        time.sleep(UPDATE_INTERVAL_SECONDS)

if __name__ == "__main__":
    main() 