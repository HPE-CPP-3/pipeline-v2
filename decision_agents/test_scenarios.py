import os
import json
import asyncio
from datetime import datetime, timezone, timedelta
import sys

# Append the current directory so we can import agents
sys.path.append(os.path.dirname(os.path.abspath(__file__)))


# ---------------------------------------------------------
# Mocking Agent Classes inline for testing
# ---------------------------------------------------------

class Agent3Optimization:
    def optimize(self, metrics: dict) -> dict:
        confidence = metrics.get("confidence", 1.0)
        predicted_cpu = metrics.get("predicted_cpu", 0)  # e.g., 4000
        limit = metrics.get("limit", 1000)
        current_replicas = metrics.get("current_replicas", 1)
        last_scaled_mins = metrics.get("last_scaled_mins", 999)

        # Anti-Flapping
        if last_scaled_mins < 5:
            return {
                "action": "HOLD",
                "proposed_replicas": current_replicas,
                "requires_llm": False,
                "reason": "Blocked by 5-min cooldown"
            }

        pressure = predicted_cpu / limit if limit > 0 else 0
        if pressure > 1.0:
            action = "SCALE_UP"
            # Target 70% utilization -> pressure / 0.7 
            scale_factor = pressure / 0.7
            
            requires_llm = False
            # The Ghost Spike (cautious scaling + escalation)
            if confidence < 0.5:
                requires_llm = True
                scale_factor = min(scale_factor, 1.5)  # Cautious scale

            proposed = max(current_replicas + 1, int(current_replicas * scale_factor))
            
            return {
                "action": action,
                "proposed_replicas": proposed,
                "requires_llm": requires_llm,
                "reason": f"High pressure detected ({(pressure*100):.0f}%)"
            }
            
        elif pressure < 0.5:
            action = "SCALE_DOWN"
            if confidence >= 0.7:
                proposed = max(1, current_replicas - 1)
                return {
                    "action": action,
                    "proposed_replicas": proposed,
                    "requires_llm": False,
                    "reason": "Low pressure, safe to scale down"
                }

        return {
            "action": "HOLD",
            "proposed_replicas": current_replicas,
            "requires_llm": False,
            "reason": "Stable"
        }

# Migrated from Groq to Gemini
import google.generativeai as genai

class Agent4Governance:
    def __init__(self, api_key: str):
        self.max_bounds = 50
        # Initialize Gemini client
        genai.configure(api_key=api_key)
        self.client = genai.GenerativeModel(
            model_name="gemini-1.5-flash",
            generation_config={
                "temperature": 0.1,
                "max_output_tokens": 60,
                "response_mime_type": "text/plain",  # plain text, we'll parse simple responses
            }
        )

    async def review(self, payload: dict, context_string: str = "") -> dict:
        proposed = payload.get("proposed_replicas", 1)
        requires_llm = payload.get("requires_llm", False)

        # The Hard Limit Rule
        if proposed > self.max_bounds:
            return {
                "outcome": "REJECTED",
                "reason": "Exceeds max cluster bounds"
            }

        # The Fast-Track Rule
        if not requires_llm:
            return {
                "outcome": "APPROVED",
                "reason": "Automated rules passed"
            }

        # The LLM Approval (Gemini)
        try:
            prompt = f"We are proposing to scale to {proposed} replicas. Context: {context_string}. Reply with exactly APPROVED or REJECTED and a short reason."
            
            # Gemini doesn't have native async, run in thread pool
            def _sync_call():
                response = self.client.generate_content(prompt)
                return response.text
            
            response_text = await asyncio.get_event_loop().run_in_executor(None, _sync_call)
            
            # Simple parsing
            outcome = "APPROVED" if "APPROVED" in response_text else "REJECTED"
            return {
                "outcome": outcome,
                "reason": response_text.replace('\n', ' ')
            }
        except Exception as e:
            # LLM Fallback (Safety Net)
            return {
                "outcome": "REJECTED",
                "reason": "LLM unreachable, defaulting to safe hold"
            }


# ---------------------------------------------------------
# Test Runners
# ---------------------------------------------------------

def run_agent3_tests():
    print("========================================")
    print("          AGENT 3 TESTS")
    print("========================================")
    agent = Agent3Optimization()

    # Test 1: The Spike
    t1 = {"confidence": 0.9, "predicted_cpu": 4000, "limit": 1000, "current_replicas": 2, "last_scaled_mins": 30}
    res1 = agent.optimize(t1)
    print(f"[The Spike] Inputs: {t1}")
    print(f" -> Output: {res1['action']} to {res1['proposed_replicas']} pods, requires_llm: {res1['requires_llm']}\n")

    # Test 2: Ghost Spike
    t2 = {"confidence": 0.3, "predicted_cpu": 4000, "limit": 1000, "current_replicas": 2, "last_scaled_mins": 30}
    res2 = agent.optimize(t2)
    print(f"[The Ghost Spike] Inputs: {t2}")
    print(f" -> Output: {res2['action']} to {res2['proposed_replicas']} pods, requires_llm: {res2['requires_llm']}\n")

    # Test 3: Safe Scale Down
    t3 = {"confidence": 0.8, "predicted_cpu": 200, "limit": 1000, "current_replicas": 4, "last_scaled_mins": 30}
    res3 = agent.optimize(t3)
    print(f"[Safe Scale Down] Inputs: {t3}")
    print(f" -> Output: {res3['action']} to {res3['proposed_replicas']} pods, requires_llm: {res3['requires_llm']}\n")

    # Test 4: Anti-Flapping
    t4 = {"confidence": 0.8, "predicted_cpu": 200, "limit": 1000, "current_replicas": 4, "last_scaled_mins": 2}
    res4 = agent.optimize(t4)
    print(f"[Anti-Flapping] Inputs: {t4}")
    print(f" -> Output: {res4['action']} to {res4['proposed_replicas']} pods, requires_llm: {res4['requires_llm']} - Reason: {res4['reason']}\n")


async def run_agent4_tests():
    print("========================================")
    print("          AGENT 4 TESTS")
    print("========================================")
    # Use a valid Gemini API key (replace with your own for testing)
    api_key = os.environ.get("GEMINI_API_KEY", "YOUR_GEMINI_API_KEY_HERE")
    agent = Agent4Governance(api_key=api_key)

    # Test 1: Hard Limit
    print("[The Hard Limit] proposed=100")
    res1 = await agent.review({"proposed_replicas": 100})
    print(f" -> Outcome: {res1['outcome']}, Reason: '{res1['reason']}'\n")

    # Test 2: Fast Track
    print("[The Fast-Track] proposed=5, requires_llm=False")
    res2 = await agent.review({"proposed_replicas": 5, "requires_llm": False})
    print(f" -> Outcome: {res2['outcome']}, Reason: '{res2['reason']}'\n")

    # Test 3: LLM Approval
    print("[The LLM Approval] proposed=5, requires_llm=True, context='marketing email'")
    res3 = await agent.review({"proposed_replicas": 5, "requires_llm": True}, "A marketing email just went out, traffic is rising")
    print(f" -> Outcome: {res3['outcome']}, Reason: '{res3['reason']}'\n")

    # Test 4: LLM Fallback (Safety net) - simulate invalid key
    print("[LLM Fallback (Safety Net)] Bad API Key")
    bad_agent = Agent4Governance(api_key="gsk_fake_key_123456789")  # Will cause exception, fallback triggers
    res4 = await bad_agent.review({"proposed_replicas": 5, "requires_llm": True})
    print(f" -> Outcome: {res4['outcome']}, Reason: '{res4['reason']}'\n")


if __name__ == "__main__":
    run_agent3_tests()
    asyncio.run(run_agent4_tests())