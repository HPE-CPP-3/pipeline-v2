"""
Agent 4: Governance and LLM Reasoning
======================================
Standalone agent. Feed it dummy data from dummy_payload.json and run.

Architecture:
  dummy_payload.json
        │
  Rule-based Engine   ← checks thresholds
        │
   ┌────┴────┐
  PASS     FLAGGED
   │          │
auto-approve  LLM Reasoning (mocked - swap in real API key if needed)
   │          │
   └────┬─────┘
    Final Decision (printed to terminal + saved to decision_agents/agent4/decision_log.json)

Run:
    python decision_agents/agent4/agent4_governance.py
    python decision_agents/agent4/agent4_governance.py --payload decision_agents/agent4/dummy_payload.json
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import asyncio
import socket
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path

# ─────────────────────────────────────────────
# Logging setup
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("agent4.governance")


def _add_file_logger(log_path: str, encoding: str = "utf-8") -> None:
    """Mirror console logs into a file using the same format."""
    root = logging.getLogger()
    abs_path = os.path.abspath(log_path)
    if any(isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", "") == abs_path for h in root.handlers):
        return

    file_handler = logging.FileHandler(abs_path, encoding=encoding)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
    )
    root.addHandler(file_handler)


class _RedisSingleInstanceLock:
    """Simple best-effort Redis lock so only one Agent 4 consumes the stream."""

    def __init__(
        self,
        redis_host: str,
        redis_port: int,
        key: str,
        ttl_seconds: int = 30,
    ):
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.key = key
        self.ttl_seconds = ttl_seconds
        self.token = f"{socket.gethostname()}:{os.getpid()}"
        self._redis = None

    def acquire_or_exit(self) -> None:
        import redis

        self._redis = redis.Redis(host=self.redis_host, port=self.redis_port, decode_responses=True)
        ok = self._redis.set(self.key, self.token, nx=True, ex=self.ttl_seconds)
        if not ok:
            current = self._redis.get(self.key)
            raise SystemExit(
                f"Another Agent 4 instance appears to be running (lock={self.key}, holder={current}). "
                f"Stop the other instance or delete the key to proceed."
            )

    def refresh(self) -> None:
        if not self._redis:
            return
        try:
            val = self._redis.get(self.key)
            if val == self.token:
                self._redis.expire(self.key, self.ttl_seconds)
        except Exception:
            pass

    def release(self) -> None:
        if not self._redis:
            return
        try:
            val = self._redis.get(self.key)
            if val == self.token:
                self._redis.delete(self.key)
        except Exception:
            pass


# ─────────────────────────────────────────────
# Enums and Data Models
# ─────────────────────────────────────────────

class GovernanceOutcome(str, Enum):
    APPROVED           = "APPROVED"
    APPROVED_WITH_CAP  = "APPROVED_WITH_CAP"
    ESCALATED_TO_LLM   = "ESCALATED_TO_LLM"
    REJECTED           = "REJECTED"


class FlagReason(str, Enum):
    LOW_CONFIDENCE     = "low_confidence"
    AGGRESSIVE_SCALE   = "aggressive_scale"
    HIGH_CPU_RISK      = "high_cpu_risk"
    HIGH_MEMORY_RISK   = "high_memory_risk"
    SCALE_DOWN_RISK    = "scale_down_risk"


@dataclass
class GovernanceDecision:
    outcome:            GovernanceOutcome
    approved_replicas:  int
    flags:              list[str]
    rule_explanation:   str
    llm_reasoning:      str | None
    final_explanation:  str
    timestamp:          str


# ─────────────────────────────────────────────
# Governance Configuration (tune thresholds here)
# ─────────────────────────────────────────────

class GovernanceConfig:
    CONFIDENCE_LOW_THRESHOLD       = 0.50   # below this → send to LLM
    SCALE_JUMP_AGGRESSIVE_RATIO    = 2.0    # e.g. 2→6 = 3x → send to LLM
    CPU_FORECAST_HIGH_THRESHOLD    = 0.90   # p90 above this = high pressure
    MEMORY_FORECAST_HIGH_THRESHOLD = 0.85   # p90 above this = high memory risk
    SCALE_DOWN_MIN_CONFIDENCE      = 0.65   # scale-down needs higher confidence
    MAX_REPLICAS_ABSOLUTE          = 20     # Never allow more than 20 pods in a demo/test


# ─────────────────────────────────────────────
# Rule-Based Engine
# ─────────────────────────────────────────────

class RuleEngine:
    """
    Evaluates the incoming scaling recommendation against governance rules.
    Returns a list of flags if the decision needs LLM review.
    """

    def __init__(self, config: GovernanceConfig = GovernanceConfig()):
        self.cfg = config

    def evaluate(self, payload: dict) -> list[FlagReason]:
        flags: list[FlagReason] = []

        confidence        = payload.get("confidence", 1.0)
        current_replicas  = payload.get("current_replicas", 1)
        recommended       = payload.get("recommended_replicas", current_replicas)
        action            = payload.get("recommended_action", "none")
        cpu_p90_15m       = float(payload.get("cpu_forecast_p90_15m", 0.0) or 0.0)
        memory_p90_15m_raw = float(payload.get("memory_forecast_p90_15m", 0.0) or 0.0)
        memory_limit      = float(payload.get("memory_limit", 0.0) or 0.0)

        # Memory forecast should normally be a ratio (~0..2). If it's huge, it's likely bytes.
        if memory_p90_15m_raw > 10.0:
            memory_p90_15m = (memory_p90_15m_raw / memory_limit) if memory_limit > 0 else None
            if memory_p90_15m is None:
                logger.info(
                    "  [INFO] memory_p90_15m appears to be bytes but memory_limit is missing; "
                    "skipping ratio-based memory governance rule"
                )
        else:
            memory_p90_15m = memory_p90_15m_raw

        # Rule 1 — Low confidence flag
        if confidence < self.cfg.CONFIDENCE_LOW_THRESHOLD:
            flags.append(FlagReason.LOW_CONFIDENCE)
            logger.info(
                f"  [FLAG] {FlagReason.LOW_CONFIDENCE}: "
                f"confidence={confidence:.2f} < threshold={self.cfg.CONFIDENCE_LOW_THRESHOLD}"
            )

        # Rule 2 — Aggressive scale-up
        if action == "scale_up" and current_replicas > 0:
            ratio = recommended / current_replicas
            if ratio > self.cfg.SCALE_JUMP_AGGRESSIVE_RATIO:
                flags.append(FlagReason.AGGRESSIVE_SCALE)
                logger.info(
                    f"  [FLAG] {FlagReason.AGGRESSIVE_SCALE}: "
                    f"{current_replicas} → {recommended} = {ratio:.1f}x "
                    f"(threshold={self.cfg.SCALE_JUMP_AGGRESSIVE_RATIO}x)"
                )

        # Rule 3 — High CPU pressure (even if memory is okay)
        if cpu_p90_15m > self.cfg.CPU_FORECAST_HIGH_THRESHOLD:
            flags.append(FlagReason.HIGH_CPU_RISK)
            logger.info(
                f"  [FLAG] {FlagReason.HIGH_CPU_RISK}: "
                f"cpu_p90_15m={cpu_p90_15m:.2f} > threshold={self.cfg.CPU_FORECAST_HIGH_THRESHOLD}"
            )

        # Rule 4 — High memory pressure (even if CPU is okay)
        if memory_p90_15m is not None and memory_p90_15m > self.cfg.MEMORY_FORECAST_HIGH_THRESHOLD:
            flags.append(FlagReason.HIGH_MEMORY_RISK)
            logger.info(
                f"  [FLAG] {FlagReason.HIGH_MEMORY_RISK}: "
                f"memory_p90_15m={memory_p90_15m:.2f} > threshold={self.cfg.MEMORY_FORECAST_HIGH_THRESHOLD}"
            )

        # Rule 5 — Scale-down with low confidence is dangerous
        if action == "scale_down" and confidence < self.cfg.SCALE_DOWN_MIN_CONFIDENCE:
            flags.append(FlagReason.SCALE_DOWN_RISK)
            logger.info(
                f"  [FLAG] {FlagReason.SCALE_DOWN_RISK}: "
                f"scale_down with confidence={confidence:.2f} < {self.cfg.SCALE_DOWN_MIN_CONFIDENCE}"
            )

        # Sanity Check — Absolute Maximum Replicas
        if recommended > self.cfg.MAX_REPLICAS_ABSOLUTE:
            flags.append(FlagReason.AGGRESSIVE_SCALE)
            logger.info(f"  [FLAG] {FlagReason.AGGRESSIVE_SCALE}: Sanity check triggered: {recommended} replicas is insane.")

        return flags


# ─────────────────────────────────────────────
# LLM Reasoning (Mocked)
# ─────────────────────────────────────────────
# To use a real LLM, replace the body of `reason()` with an API call.
# Example for OpenAI:
#   import openai
#   client = openai.OpenAI(api_key="sk-...")
#   resp = client.chat.completions.create(model="gpt-4o", messages=[...])
#   return resp.choices[0].message.content
#
# Example for Gemini:
#   import google.generativeai as genai
#   genai.configure(api_key="YOUR_KEY")
#   model = genai.GenerativeModel("gemini-1.5-flash")
#   return model.generate_content(prompt).text

from pydantic import BaseModel, Field, ValidationError

class LLMResponseSchema(BaseModel):
    should_approve: bool
    final_replicas: int = Field(gt=0, le=20) # Must be greater than 0, less than 20
    reasoning: str

class LLMReasoner:
    """
    Receives the full context (payload + flags) and returns a
    plain-English reasoning string plus whether to approve or reject.
    Currently MOCKED — replace reason() with a real API call.
    """

    def __init__(self, api_key: str = None, model_type="local", local_model_path="models/governance_qwen_3b_q4_k_m.gguf"):
        self.mode = model_type # "local" or "groq"
        self.api_key = api_key or os.environ.get("GROQ_API_KEY")
        
        if self.mode == "groq" and self.api_key:
            from groq import AsyncGroq
            self.client = AsyncGroq(api_key=self.api_key)
            self.local_llm = None
        else:
            self.client = None
            try:
                from llama_cpp import Llama
                logger.info(f"Loading local LLM from {local_model_path}...")
                self.local_llm = Llama(
                    model_path=local_model_path,
                    n_ctx=2048,
                    n_gpu_layers=-1, # GPU acceleration if available
                    verbose=False
                )
                logger.info("Local LLM loaded successfully.")
            except ImportError:
                logger.warning("llama-cpp-python not installed. Falling back to mock.")
                self.local_llm = None
            except ValueError as e:
                logger.warning(f"Could not load local model: {e}. Falling back to mock.")
                self.local_llm = None

    def _build_prompt(self, payload: dict, flags: list[FlagReason]) -> str:
        """
        THIS IS THE CORE LOGIC. 
        It turns raw numbers into a narrative for the LLM.
        """
        flag_text = ", ".join(f.value for f in flags) if flags else "none"
        
        # We add "Human Labels" to the numbers to help a small 3B model understand
        cpu_label = "CRITICAL" if payload.get('cpu_forecast_p90_15m', 0) > 0.9 else "NORMAL"
        conf_label = "UNRELIABLE" if payload.get('confidence', 1.0) < 0.4 else "TRUSTED"

        current = payload.get('current_replicas', 1)
        recommended = payload.get('recommended_replicas', current)

        return f"""Review this Kubernetes scaling decision:

Target Pod: {payload.get('pod')} (QoS: {payload.get('qos_class', 'Burstable')})
Proposed Action: {payload.get('recommended_action')}
Scale Delta: {current} -> {recommended}
Model Confidence: {payload.get('confidence')} ({conf_label})
Resource Pressure: CPU {payload.get('cpu_forecast_p90_15m')} ({cpu_label}), Mem {payload.get('memory_forecast_p90_15m')}
Flags Raised: {flag_text}

STRICT OUTPUT RULES:
- If you APPROVE: set final_replicas to {recommended}
- If you REJECT: set final_replicas to {current} (the current safe count — NEVER 0)
- final_replicas must be a positive integer between 1 and 20

Analyze if this is safe. Output JSON: {{"should_approve": bool, "final_replicas": int, "reasoning": "string"}}"""

    async def reason(
        self,
        payload: dict,
        flags: list[FlagReason],
    ) -> tuple[str, bool, int]:
        """
        Returns:
            reasoning      (str)  — LLM's explanation
            should_approve (bool) — True = approve, False = reject
            final_replicas (int)  — LLM's recommended replica count
        """

        # 1. Build the prompt
        prompt = self._build_prompt(payload, flags)
        logger.debug("LLM Prompt:\n" + prompt)

        # 2. Call the Model
        if self.mode == "groq" and self.client:
            try:
                response = await self.client.chat.completions.create(
                    messages=[{"role": "user", "content": prompt}],
                    model="llama-3.3-70b-versatile",
                    response_format={"type": "json_object"},
                    max_tokens=200,
                    timeout=10.0
                )
                res_text = response.choices[0].message.content
                import json
                try:
                    data = json.loads(res_text)
                    validated_data = LLMResponseSchema(**data)
                    should_approve = validated_data.should_approve
                    final_replicas = validated_data.final_replicas
                    
                    if not should_approve:
                        final_replicas = payload.get("current_replicas", 1) # Force safety hold
                        
                    return validated_data.reasoning, should_approve, final_replicas
                except json.JSONDecodeError:
                    return f"Failed to parse LLM JSON: {res_text}", False, payload.get('current_replicas', 1)
                except ValidationError as e:
                    logger.error(f"LLM output failed schema validation: {e}")
                    return "LLM returned malformed data.", False, payload.get('current_replicas', 1)
            except Exception as e:
                logger.error(f"Groq API Error: {e}")
                return "LLM unreachable, defaulting to safe hold.", False, payload.get('current_replicas', 1)
        elif self.local_llm:
            try:
                logger.info("Running local LLM inference...")
                response = self.local_llm.create_chat_completion(
                    messages=[
                        {"role": "system", "content": "You are a Kubernetes autoscaling governance agent. Always respond with a JSON object containing: should_approve (boolean), final_replicas (integer), reasoning (string)."},
                        {"role": "user", "content": prompt}
                    ],
                    response_format={"type": "json_object"},
                    max_tokens=200,
                    temperature=0.1
                )
                res_text = response["choices"][0]["message"]["content"]
                import json
                try:
                    data = json.loads(res_text)
                    validated_data = LLMResponseSchema(**data)
                    should_approve = validated_data.should_approve
                    final_replicas = validated_data.final_replicas
                    
                    if not should_approve:
                        final_replicas = payload.get("current_replicas", 1) # Force safety hold
                        
                    return validated_data.reasoning, should_approve, final_replicas
                except json.JSONDecodeError:
                    return f"Failed to parse LLM JSON: {res_text}", False, payload.get('current_replicas', 1)
                except ValidationError as e:
                    logger.error(f"LLM output failed schema validation: {e}")
                    return "LLM returned malformed data.", False, payload.get('current_replicas', 1)
            except Exception as e:
                logger.error(f"Local LLM Error: {e}")
                return "Local LLM failed, defaulting to safe hold.", False, payload.get('current_replicas', 1)
        else:
             logger.warning("No GROQ_API_KEY and no local model loaded, using mock LLM response.")
             return self._mock_llm_response(payload, flags)

    def _mock_llm_response(
        self,
        payload: dict,
        flags: list[FlagReason],
    ) -> tuple[str, bool, int]:
        """
        Simulates LLM judgment based on flag combinations.
        Real-world LLM would do far richer analysis.
        """
        current   = payload.get("current_replicas", 1)
        recommend = payload.get("recommended_replicas", current)
        confidence = payload.get("confidence", 0.5)
        action    = payload.get("recommended_action", "none")

        # If both aggressive scale AND low confidence → approve with a cap at 2x
        if FlagReason.AGGRESSIVE_SCALE in flags and FlagReason.LOW_CONFIDENCE in flags:
            capped = current * 2
            return (
                f"The model confidence ({confidence:.2f}) is below threshold and the "
                f"scale jump ({current}→{recommend}) is aggressive. Approving a "
                f"conservative cap of {capped} replicas instead to avoid over-provisioning "
                f"while still responding to the elevated CPU and memory signals.",
                True,
                capped,
            )

        # If only low confidence but action is safe (≤2x) → approve with a warning
        if FlagReason.LOW_CONFIDENCE in flags and FlagReason.AGGRESSIVE_SCALE not in flags:
            return (
                f"Confidence is low ({confidence:.2f}), but the scale delta is modest "
                f"({current}→{recommend}). Approving the action with a note to monitor "
                f"closely for the next 15 minutes and revert if CPU drops below 60%.",
                True,
                recommend,
            )

        # If scale-down risk → reject and hold current replica count
        if FlagReason.SCALE_DOWN_RISK in flags:
            return (
                f"A scale-down action with confidence {confidence:.2f} is risky. "
                f"The historical distribution suggests the workload may rebound. "
                f"Rejecting the scale-down. Maintaining {current} replicas for now.",
                False,
                current,
            )

        # High CPU risk alone → approve to prevent throttling
        if FlagReason.HIGH_CPU_RISK in flags and FlagReason.HIGH_MEMORY_RISK not in flags:
            return (
                f"CPU forecast p90 is critically elevated ({payload.get('cpu_forecast_p90_15m', 0):.2f}). "
                f"The scale recommendation of {recommend} replicas is appropriate to "
                f"prevent CPU throttling. Approving with monitoring advisory.",
                True,
                recommend,
            )

        # High memory risk (with or without CPU risk) → approve to distribute pressure
        if FlagReason.HIGH_MEMORY_RISK in flags:
            cpu_also = " CPU and" if FlagReason.HIGH_CPU_RISK in flags else ""
            return (
                f"{cpu_also} Memory forecast p90 is elevated. The scale recommendation of "
                f"{recommend} replicas is appropriate to distribute resource pressure. "
                f"Approving with 15-minute cooldown enforced.",
                True,
                recommend,
            )

        # Default fallback → approve
        return (
            f"After reviewing all signals, the scaling decision appears reasonable. "
            f"Approving {recommend} replicas as recommended.",
            True,
            recommend,
        )


# ─────────────────────────────────────────────
# Main Governance Agent
# ─────────────────────────────────────────────

class GovernanceAgent:

    def __init__(self, groq_api_key: str = None):
        self.rule_engine  = RuleEngine(GovernanceConfig())
        model_type = "groq" if groq_api_key else "local"
        self.llm_reasoner = LLMReasoner(api_key=groq_api_key, model_type=model_type)

    async def run(self, payload: dict) -> GovernanceDecision:
        logger.info("=" * 60)
        logger.info("Agent 4: Governance and LLM Reasoning — Starting")
        logger.info("=" * 60)
        logger.info(
            f"Input: action={payload.get('recommended_action')} | "
            f"replicas {payload.get('current_replicas')} → {payload.get('recommended_replicas')} | "
            f"confidence={payload.get('confidence')}"
        )
        logger.info("")

        # ── Step 1: Rule Engine ───────────────────────────────────────────
        logger.info("Step 1: Running Rule-Based Engine...")
        flags = self.rule_engine.evaluate(payload)

        current_replicas = payload.get("current_replicas", 1)
        recommended      = payload.get("recommended_replicas", current_replicas)

        # [NEW] CIRCUIT BREAKER 1: The "OOM Panic" Fast-Track
        memory_p90 = payload.get("memory_forecast_p90_15m", 0.0)
        if memory_p90 > 0.95 and payload.get("recommended_action") == "scale_up":
            logger.warning("CIRCUIT BREAKER: OOM Panic Fast-Track triggered.")
            return GovernanceDecision(
                outcome=GovernanceOutcome.APPROVED,
                approved_replicas=recommended,
                flags=[f.value for f in flags] + ["OOM_PANIC_OVERRIDE"],
                rule_explanation="CRITICAL MEMORY: Bypassed LLM to prevent imminent OOM crash.",
                llm_reasoning=None,
                final_explanation="Emergency fast-track approval executed due to >95% memory pressure.",
                timestamp=datetime.now(timezone.utc).isoformat()
            )

        # [NEW] CIRCUIT BREAKER 2: The "Insanity" Hard-Reject
        if recommended > self.rule_engine.cfg.MAX_REPLICAS_ABSOLUTE:
            logger.warning(f"CIRCUIT BREAKER: Insanity Hard-Reject triggered. {recommended} > {self.rule_engine.cfg.MAX_REPLICAS_ABSOLUTE}")
            return GovernanceDecision(
                outcome=GovernanceOutcome.REJECTED,
                approved_replicas=current_replicas,
                flags=[f.value for f in flags] + ["INSANITY_HARD_REJECT"],
                rule_explanation=f"Requested {recommended} > Absolute Max ({self.rule_engine.cfg.MAX_REPLICAS_ABSOLUTE}).",
                llm_reasoning=None,
                final_explanation="Hard rejected by rule engine due to mathematically impossible recommendation.",
                timestamp=datetime.now(timezone.utc).isoformat()
            )

        if not flags:
            logger.info("  ✓ No flags raised — decision auto-approved")

        # ── Step 2: Branch Logic ──────────────────────────────────────────
        logger.info("")
        logger.info("Step 2: Applying Governance Branch Logic...")

        llm_reasoning   = None
        final_replicas  = recommended
        outcome         = GovernanceOutcome.APPROVED
        rule_explanation = f"Rule engine evaluated {len(flags)} flag(s): {[f.value for f in flags]}"

        if not flags:
            # Branch 1: Clean pass, auto-approve
            outcome           = GovernanceOutcome.APPROVED
            final_replicas    = recommended
            final_explanation = (
                f"All governance rules passed. Scaling from "
                f"{current_replicas} → {final_replicas} replicas approved automatically."
            )

        else:
            # Branch 2/3: Escalate to LLM
            logger.info(f"  → Escalating to LLM Reasoning ({len(flags)} flag(s)): {[f.value for f in flags]}")
            logger.info("")
            logger.info("Step 3: LLM Reasoning...")

            import time
            start_time = time.time()
            reasoning, should_approve, llm_replicas = await self.llm_reasoner.reason(payload, flags)
            latency = time.time() - start_time
            
            logger.info(f"LLM Inference Latency: {latency:.2f} seconds")
            if latency > 5.0:
                logger.warning("LLM inference is degrading. Consider falling back to Rules-Only mode.")

            llm_reasoning  = reasoning
            final_replicas = llm_replicas

            if should_approve:
                if final_replicas < recommended:
                    outcome = GovernanceOutcome.APPROVED_WITH_CAP
                    final_explanation = (
                        f"LLM approved the scaling action but capped replicas at {final_replicas} "
                        f"(down from recommended {recommended})."
                    )
                else:
                    outcome = GovernanceOutcome.ESCALATED_TO_LLM
                    final_explanation = (
                        f"LLM reviewed and approved scaling to {final_replicas} replicas."
                    )
            else:
                outcome        = GovernanceOutcome.REJECTED
                final_replicas = current_replicas
                final_explanation = (
                    f"LLM rejected the scaling action. Maintaining current replica "
                    f"count of {current_replicas}."
                )

        # ── Step 3: Build final decision ──────────────────────────────────
        decision = GovernanceDecision(
            outcome            = outcome,
            approved_replicas  = final_replicas,
            flags              = [f.value for f in flags],
            rule_explanation   = rule_explanation,
            llm_reasoning      = llm_reasoning,
            final_explanation  = final_explanation,
            timestamp          = datetime.now(timezone.utc).isoformat(),
        )

        return decision


# ─────────────────────────────────────────────
# Pretty Print
# ─────────────────────────────────────────────

def print_decision(decision: GovernanceDecision, payload: dict):
    outcome_colors = {
        GovernanceOutcome.APPROVED:          "✅ APPROVED",
        GovernanceOutcome.APPROVED_WITH_CAP: "⚠️  APPROVED WITH CAP",
        GovernanceOutcome.ESCALATED_TO_LLM:  "🤖 APPROVED (via LLM)",
        GovernanceOutcome.REJECTED:          "❌ REJECTED",
    }

    print("\n" + "=" * 60)
    print("  GOVERNANCE DECISION")
    print("=" * 60)
    print(f"  Outcome:           {outcome_colors[decision.outcome]}")
    print(f"  Current replicas:  {payload.get('current_replicas')}")
    print(f"  Recommended:       {payload.get('recommended_replicas')}")
    print(f"  Final approved:    {decision.approved_replicas}")
    print(f"  Flags raised:      {decision.flags if decision.flags else 'None'}")
    print("-" * 60)
    print(f"  Rule engine:  {decision.rule_explanation}")
    if decision.llm_reasoning:
        print(f"\n  LLM Reasoning:\n  \"{decision.llm_reasoning}\"")
    print("-" * 60)
    print(f"  Summary: {decision.final_explanation}")
    print("=" * 60)
    print(f"  Timestamp: {decision.timestamp}")
    print("=" * 60 + "\n")


# ─────────────────────────────────────────────
# Entry Point
# ─────────────────────────────────────────────

async def run_redis_mode(args):
    import sys
    from dataclasses import asdict
    # Add repo root to path to import src
    sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    from src.storage.redis_store import RedisStore

    logger.info(f"Connecting to Redis at {args.redis_host}:{args.redis_port}...")
    redis_store = RedisStore(host=args.redis_host, port=args.redis_port)
    agent = GovernanceAgent(groq_api_key=args.groq_key)

    logger.info("Agent 4: Listening on stream:optimization:complete...")
    last_id = "$"  # Read only new messages

    lock = _RedisSingleInstanceLock(
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        key=os.environ.get("PIPELINE_AGENT4_LOCK_KEY", "lock:agent4:governance"),
        ttl_seconds=int(os.environ.get("PIPELINE_AGENT4_LOCK_TTL", "30")),
    )
    lock.acquire_or_exit()
    logger.info("Agent 4 lock acquired.")

    try:
        while True:
            lock.refresh()
            messages = await redis_store.read_stream_messages(
                stream_name="stream:optimization:complete",
                last_id=last_id,
                block_ms=5000,
                count=10,
            )
            for msg_id, raw_payload in messages:
                last_id = msg_id

                logger.info(f"--- Received Optimization Action {msg_id} ---")

                # ── Cast Redis string values back to correct Python types ────────
                # Agent 3 must stringify everything before writing to the stream;
                # Agent 4's RuleEngine needs floats and ints, not strings.
                payload = dict(raw_payload)
                for key in (
                    "confidence",
                    "cpu_forecast_p50_5m", "cpu_forecast_p90_5m", "cpu_forecast_p90_15m",
                    "memory_forecast_p50_5m", "memory_forecast_p90_5m", "memory_forecast_p90_15m",
                    "throttle_prob", "oom_prob",
                    "cpu_limit", "memory_limit",
                ):
                    if key in payload:
                        try:
                            payload[key] = float(payload[key])
                        except (ValueError, TypeError):
                            pass
                for key in ("current_replicas", "recommended_replicas"):
                    if key in payload:
                        try:
                            payload[key] = int(payload[key])
                        except (ValueError, TypeError):
                            pass

                # Run governance
                ns = payload.get('namespace', 'default')
                pod = payload.get('pod', 'unknown')
                action = payload.get('recommended_action')
                cooldown_key = f"cooldown:{ns}:{pod}"
                
                last_action = await redis_store.client.get(cooldown_key)
                if last_action == "scale_up" and action == "scale_down":
                    logger.warning("Anti-flapping triggered. Rejecting scale-down during cooldown window.")
                    decision = GovernanceDecision(
                        outcome=GovernanceOutcome.REJECTED,
                        approved_replicas=payload.get("current_replicas", 1),
                        flags=["ANTI_FLAPPING_COOLDOWN"],
                        rule_explanation="Anti-flapping triggered by Redis cooldown key.",
                        llm_reasoning=None,
                        final_explanation="Rejected scale-down to prevent cluster thrashing.",
                        timestamp=datetime.now(timezone.utc).isoformat()
                    )
                else:
                    decision = await agent.run(payload)
                    if decision.outcome in (GovernanceOutcome.APPROVED, GovernanceOutcome.APPROVED_WITH_CAP, GovernanceOutcome.ESCALATED_TO_LLM):
                        await redis_store.client.setex(cooldown_key, 300, action)


                # Print to terminal
                print_decision(decision, payload)

                # ── Publish final governance decision to next stream ──────────
                # Redis Streams require string values; lists must be JSON-encoded.
                gov_dict = asdict(decision)
                gov_dict["flags"] = json.dumps(gov_dict["flags"])  # list → JSON string
                gov_dict["outcome"] = str(gov_dict["outcome"])
                # Copy pod identity fields from incoming payload for traceability
                for field in ("namespace", "pod", "container"):
                    if field in payload:
                        gov_dict[field] = payload[field]

                out_id = await redis_store.write_stream_message(
                    stream_name="stream:governance:complete",
                    payload={k: str(v) for k, v in gov_dict.items() if v is not None}
                )
                logger.info(
                    f"Published governance decision to stream:governance:complete "
                    f"(outcome={decision.outcome}, replicas={decision.approved_replicas}, "
                    f"ID={out_id})\n"
                )

    except KeyboardInterrupt:
        logger.info("Stopping Agent 4 Redis loop.")
    finally:
        lock.release()
        await redis_store.close()

async def async_main(args):
    if args.mode == "redis":
        await run_redis_mode(args)
    else:
        # Load payload
        payload_path = Path(args.payload)
        if not payload_path.exists():
            print(f"Error: Payload file not found: {payload_path}")
            exit(1)

        with open(payload_path) as f:
            payload = json.load(f)

        # Run governance
        agent    = GovernanceAgent(groq_api_key=args.groq_key)
        decision = await agent.run(payload)

        # Print to terminal
        print_decision(decision, payload)

        # Save decision log
        output_path = Path(args.output)
        with open(output_path, "w") as f:
            json.dump(asdict(decision), f, indent=2)

        logger.info(f"Decision saved to: {output_path}")

def main():
    import asyncio
    parser = argparse.ArgumentParser(
        description="Agent 4: Governance and LLM Reasoning"
    )
    parser.add_argument(
        "--log-file",
        type=str,
        default=os.environ.get(
            "PIPELINE_AGENT4_LOG_FILE",
            str(Path(__file__).resolve().parent / "agent4_log.txt"),
        ),
        help="Path to write Agent 4 logs (default: agent4_log.txt)",
    )
    parser.add_argument(
        "--log-encoding",
        type=str,
        default=os.environ.get("PIPELINE_AGENT4_LOG_ENCODING", "utf-8"),
        help="Log file encoding (default: utf-8; use utf-16 to mimic PowerShell-style logs)",
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=["file", "redis"],
        default="file",
        help="Run mode: 'file' for standalone testing, 'redis' for live stream listening",
    )
    parser.add_argument(
        "--payload",
        type=str,
        default="dummy_payload.json",
        help="(File mode) Path to the JSON payload file (default: dummy_payload.json)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=str(Path(__file__).resolve().parent / "decision_log.json"),
        help="(File mode) Path to write the governance decision JSON (default: decision_log.json)",
    )
    parser.add_argument(
        "--redis-host",
        type=str,
        default="localhost",
        help="Redis host",
    )
    parser.add_argument(
        "--redis-port",
        type=int,
        default=6380,  # Based on mock_ingestion.py default
        help="Redis port",
    )
    parser.add_argument(
        "--groq-key",
        type=str,
        default=os.environ.get("GROQ_API_KEY"),
        help="Groq API Key (default: $GROQ_API_KEY)",
    )
    args = parser.parse_args()

    _add_file_logger(args.log_file, encoding=args.log_encoding)
    
    asyncio.run(async_main(args))

if __name__ == "__main__":
    main()
