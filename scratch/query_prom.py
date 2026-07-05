import requests
import json

url = "http://localhost:30000/api/v1/query"
pod = "stress-test-app-79d76cff6d-jdg7t"

queries = {
    "cpu_rate": f'rate(container_cpu_usage_seconds_total{{namespace="test-workload", pod="{pod}", container="stress-container"}}[5m])',
    "cpu_rate_no_c": f'rate(container_cpu_usage_seconds_total{{namespace="test-workload", pod="{pod}"}}[5m])',
    "memory_ws": f'container_memory_working_set_bytes{{namespace="test-workload", pod="{pod}", container="stress-container"}}',
    "memory_ws_no_c": f'container_memory_working_set_bytes{{namespace="test-workload", pod="{pod}"}}',
}

for name, q in queries.items():
    try:
        r = requests.get(url, params={"query": q})
        res = r.json()
        print(f"=== {name} ===")
        print(json.dumps(res.get("data", {}).get("result", []), indent=2))
    except Exception as e:
        print(f"Error {name}: {e}")
