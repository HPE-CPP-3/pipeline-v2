import os
import sys
import json
import asyncio
import httpx
import logging
import subprocess
from datetime import datetime
from typing import Dict, Any, List, Optional
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
import redis.asyncio as redis

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dashboard")

app = FastAPI(title="K8s Sentinel Telemetry Dashboard")

# Paths
DASHBOARD_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(DASHBOARD_DIR)
TEMPLATES_DIR = os.path.join(DASHBOARD_DIR, "templates")

# Log files mapping
LOG_FILES = {
    "agent3": os.path.join(ROOT_DIR, "decision_agents", "agent3", "agent3_log.txt"),
    "agent4": os.path.join(ROOT_DIR, "decision_agents", "agent4", "agent4_log.txt"),
    "agent5": os.path.join(ROOT_DIR, "decision_agents", "agent5", "agent5_log.txt"),
    "stdout": os.path.join(ROOT_DIR, "all_agents_stdout.log")
}

# Redis configuration
REDIS_HOST = os.getenv("PIPELINE_REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("PIPELINE_REDIS_PORT", "6380"))
REDIS_PASSWORD = os.getenv("PIPELINE_REDIS_PASSWORD") or None

redis_client: Optional[redis.Redis] = None

@app.on_event("startup")
async def startup_event():
    global redis_client
    try:
        redis_client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD,
            decode_responses=True,
            socket_timeout=2.0
        )
        logger.info(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")

@app.on_event("shutdown")
async def shutdown_event():
    global redis_client
    if redis_client:
        await redis_client.close()

def check_process_status() -> Dict[str, Dict[str, Any]]:
    """Check running python processes to see if agents are active."""
    status = {
        "agent12": {"running": False, "pid": None},
        "agent3": {"running": False, "pid": None},
        "agent4": {"running": False, "pid": None},
        "agent5": {"running": False, "pid": None},
    }
    try:
        # Run ps aux to find python processes
        output = subprocess.check_output(["ps", "aux"], text=True)
        for line in output.splitlines():
            if "python" in line or "pipeline-agentic" in line:
                parts = line.split()
                if len(parts) < 11:
                    continue
                pid = int(parts[1])
                cmdline = " ".join(parts[10:])
                if "src.agents.runtime" in cmdline or "pipeline-agentic" in cmdline:
                    status["agent12"] = {"running": True, "pid": pid}
                elif "agent3_optimization.py" in cmdline:
                    status["agent3"] = {"running": True, "pid": pid}
                elif "agent4_governance.py" in cmdline:
                    status["agent4"] = {"running": True, "pid": pid}
                elif "agent5_executor.py" in cmdline:
                    status["agent5"] = {"running": True, "pid": pid}
    except Exception as e:
        logger.error(f"Error checking processes: {e}")
    return status

@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    index_path = os.path.join(TEMPLATES_DIR, "index.html")
    if not os.path.exists(index_path):
        raise HTTPException(status_code=404, detail="Dashboard index.html not found.")
    with open(index_path, "r") as f:
        return f.read()

@app.get("/api/status")
async def get_status():
    global redis_client
    status_data = {
        "timestamp": datetime.now().isoformat(),
        "redis": {"online": False, "info": None},
        "prometheus": {"online": False, "url": "http://localhost:30000"},
        "llm": {"online": False, "url": "https://elian-isochimal-kathaleen.ngrok-free.dev"},
        "agents": check_process_status(),
        "locks": {
            "agent3": {"active": False, "holder": None},
            "agent4": {"active": False, "holder": None},
            "agent5": {"active": False, "holder": None},
        },
        "drift_ratio": 0.0,
        "last_recommendation": "HOLD",
        "confidence": 0.0,
        "last_governance_decision": "N/A"
    }

    # 1. Test Redis & Fetch locks
    if redis_client:
        try:
            await redis_client.ping()
            status_data["redis"]["online"] = True
            
            # Fetch locks
            for key, agent in [
                ("lock:agent3:optimization", "agent3"),
                ("lock:agent4:governance", "agent4"),
                ("lock:agent5:executor", "agent5")
            ]:
                holder = await redis_client.get(key)
                if holder:
                    status_data["locks"][agent] = {"active": True, "holder": holder}
        except Exception as e:
            logger.warning(f"Redis ping failed: {e}")

    # 2. Test Prometheus
    async with httpx.AsyncClient(timeout=1.5) as client:
        try:
            resp = await client.get("http://localhost:30000/api/v1/query?query=up")
            if resp.status_code == 200:
                status_data["prometheus"]["online"] = True
        except Exception:
            pass

    # 3. Test LLM
    async with httpx.AsyncClient(timeout=2.0) as client:
        try:
            resp = await client.get("https://elian-isochimal-kathaleen.ngrok-free.dev")
            if resp.status_code in [200, 401, 404, 405, 302]:
                status_data["llm"]["online"] = True
        except Exception:
            pass

    # 4. Get active pod from stream:ingestion:complete
    active_pod = None
    active_namespace = None
    if redis_client:
        try:
            ingestion_msgs = await redis_client.xrevrange("stream:ingestion:complete", max="+", min="-", count=1)
            if ingestion_msgs:
                _, fields = ingestion_msgs[0]
                active_pod = fields.get("pod")
                active_namespace = fields.get("namespace")
        except Exception as e:
            logger.warning(f"Error reading ingestion stream: {e}")

    # 5. Fetch drift ratio
    if redis_client:
        try:
            if active_pod and active_namespace:
                val = await redis_client.get(f"metrics:drift_ratio:{active_namespace}:{active_pod}")
                if val:
                    status_data["drift_ratio"] = float(val)
            else:
                # Fallback scan keys
                keys = await redis_client.keys("metrics:drift_ratio:*")
                if keys:
                    val = await redis_client.get(keys[0])
                    if val:
                        status_data["drift_ratio"] = float(val)
        except Exception as e:
            logger.warning(f"Error reading drift ratio: {e}")

    # 6. Fetch last recommendation and confidence from stream:optimization:complete
    if redis_client:
        try:
            opt_msgs = await redis_client.xrevrange("stream:optimization:complete", max="+", min="-", count=1)
            if opt_msgs:
                _, fields = opt_msgs[0]
                status_data["last_recommendation"] = fields.get("recommended_action", "HOLD").upper()
                status_data["confidence"] = float(fields.get("confidence", "0.0"))
        except Exception as e:
            logger.warning(f"Error reading optimization stream: {e}")

    # 7. Fetch last governance decision from stream:governance:complete
    if redis_client:
        try:
            gov_msgs = await redis_client.xrevrange("stream:governance:complete", max="+", min="-", count=1)
            if gov_msgs:
                _, fields = gov_msgs[0]
                status_data["last_governance_decision"] = fields.get("outcome", "N/A").upper()
        except Exception as e:
            logger.warning(f"Error reading governance stream: {e}")

    return status_data

@app.get("/api/logs/{agent}")
async def get_logs(agent: str, lines: int = Query(default=150, ge=1, le=1000)):
    if agent not in LOG_FILES:
        raise HTTPException(status_code=400, detail="Invalid agent log name.")
    
    log_path = LOG_FILES[agent]
    if not os.path.exists(log_path):
        return {"agent": agent, "exists": False, "content": f"Log file for {agent} does not exist yet."}
    
    try:
        # Efficiently read last N lines
        # Using tail command via subprocess is fastest and safest for large files
        output = subprocess.check_output(["tail", "-n", str(lines), log_path], text=True)
        return {"agent": agent, "exists": True, "content": output}
    except Exception as e:
        logger.error(f"Error reading log for {agent}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/redis/streams/{stream_name}")
async def get_stream_messages(stream_name: str, count: int = Query(default=50, ge=1, le=200)):
    global redis_client
    if not redis_client:
        raise HTTPException(status_code=503, detail="Redis connection unavailable.")
    
    try:
        # Use xrevrange to get the latest messages in reverse chronological order
        messages = await redis_client.xrevrange(stream_name, max="+", min="-", count=count)
        
        parsed_messages = []
        for msg_id, fields in messages:
            parsed_messages.append({
                "id": msg_id,
                "fields": fields
            })
        return {"stream": stream_name, "count": len(parsed_messages), "messages": parsed_messages}
    except Exception as e:
        logger.error(f"Error reading stream {stream_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/locks/clear")
async def clear_all_locks():
    global redis_client
    if not redis_client:
        raise HTTPException(status_code=503, detail="Redis connection unavailable.")
    
    cleared = []
    keys = ["lock:agent3:optimization", "lock:agent4:governance", "lock:agent5:executor"]
    for key in keys:
        try:
            res = await redis_client.delete(key)
            if res > 0:
                cleared.append(key)
        except Exception as e:
            logger.error(f"Error deleting key {key}: {e}")
            
    return {"status": "success", "cleared": cleared}

@app.post("/api/locks/clear/{agent}")
async def clear_agent_lock(agent: str):
    global redis_client
    if not redis_client:
        raise HTTPException(status_code=503, detail="Redis connection unavailable.")
    
    mapping = {
        "agent3": "lock:agent3:optimization",
        "agent4": "lock:agent4:governance",
        "agent5": "lock:agent5:executor"
    }
    if agent not in mapping:
        raise HTTPException(status_code=400, detail="Invalid agent name.")
    
    key = mapping[agent]
    try:
        res = await redis_client.delete(key)
        return {"status": "success", "cleared": res > 0, "key": key}
    except Exception as e:
        logger.error(f"Error deleting key {key}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
