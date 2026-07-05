import redis
import json
from datetime import datetime, timezone

def trigger():
    r = redis.Redis(host="localhost", port=6380, decode_responses=True)
    
    # We want Agent 3 to decide SCALE_DOWN
    # Both cpu and memory risks should be LOW
    # Let's target the pod: stress-test-app-8578ddff6d-2nd2l
    namespace = "test-workload"
    pod = "stress-test-app-8578ddff6d-2nd2l"
    
    # We set predicted cpu and memory to low values (pressure ~ 0.05)
    cpu_limit = 2.0
    mem_limit = 536870912
    
    cpu_forecast = {
        5: {0.5: 0.1, 0.7: 0.11, 0.9: 0.12},
        15: {0.5: 0.1, 0.7: 0.11, 0.9: 0.12}
    }
    mem_forecast = {
        5: {0.5: 10000000, 0.7: 11000000, 0.9: 12000000},
        15: {0.5: 10000000, 0.7: 11000000, 0.9: 12000000}
    }
    
    forecast_payload = {
        "namespace": namespace,
        "pod": pod,
        "container": "stress-container",
        "cpu_limit": cpu_limit,
        "memory_limit": mem_limit,
        "cpu_forecast": cpu_forecast,
        "memory_forecast": mem_forecast,
        "throttle_prob": 0.01,
        "oom_prob": 0.01,
        "throttle_risk": {"risk_level": "LOW", "probability": 0.01, "reason": "forecast below threshold"},
        "oom_risk": {"oom_risk": "LOW", "probability": 0.01, "reason": "forecast below threshold"},
        "confidence": 0.95,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    
    payload = {
        "namespace": namespace,
        "pod": pod,
        "container": "stress-container",
        "forecast_json": json.dumps(forecast_payload),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    
    print("Publishing mock prediction to Redis stream...")
    
    # Let's check the stream name: "stream:prediction:complete"
    res = r.xadd("stream:prediction:complete", payload)
    print(f"Message published to stream:prediction:complete, ID: {res}")

if __name__ == "__main__":
    trigger()
