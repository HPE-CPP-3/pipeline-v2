# Agent 4 — Governance and LLM Reasoning

## Overview

Agent 4 is the final gate before any scaling action reaches Kubernetes. It consumes `stream:optimization:complete` from Agent 3 and produces a governance decision on `stream:governance:complete`.

---

## Execution Flow

```
Payload received (from Agent 3)
  │
  ├─► Anti-flapping check (Redis cooldown key)
  │     scale_up → scale_down within cooldown? → REJECTED (ANTI_FLAPPING_COOLDOWN)
  │
  ├─► Circuit Breaker 1: OOM Panic Fast-Track
  │     memory_p90_5m > 95% AND action=scale_up → APPROVED (bypass LLM)
  │
  ├─► Circuit Breaker 2: Absolute Max Cap
  │     recommended > 20 → cap to 20 → APPROVED_WITH_CAP
  │     already at 20 → APPROVED (hold at max)
  │
  ├─► Rule Engine: raise flags
  │     - LOW_CONFIDENCE (confidence < threshold)
  │     - AGGRESSIVE_SCALE (replica ratio > threshold)
  │     - HIGH_CPU_RISK (SUPPRESSED for scale_up — high CPU confirms scale-up is correct)
  │     - HIGH_MEMORY_RISK (SUPPRESSED for scale_up — same rationale)
  │     - SCALE_DOWN_RISK (scale-down with low confidence)
  │
  ├─► No flags → auto-APPROVED
  └─► Flags raised → LLM Reasoning → APPROVED / APPROVED_WITH_CAP / REJECTED
```

---

## Flag Suppression for Scale-Up

`HIGH_CPU_RISK` and `HIGH_MEMORY_RISK` are intentionally **not raised** when the action is `scale_up`:

> High CPU/memory is the *reason* to scale up — adding replicas distributes load. Flagging these would cause the LLM to hallucinate backwards physics ("CPU is high, don't scale up") and reject valid decisions.

These flags **are** raised for `scale_down` and `hold`, where high resource pressure would make a scale-down dangerous.

Log signature when suppressed:
```
[OK] HIGH_CPU_RISK suppressed for scale_up action (cpu_p90_5m=0.92): high CPU confirms scale-up is correct.
```

---

## Anti-Flapping (Redis Cooldown)

After any approved scaling action, writes:
```
cooldown:<namespace>:<deployment> = "<action>"   TTL: 300s
```

If the next action is `scale_down` and the key still holds `scale_up`, the decision is rejected immediately without calling the LLM.

**To force-clear for testing:**
```bash
redis-cli -p 6380 DEL cooldown:test-workload:stress-test-app
```

---

## LLM Reasoning

The LLM is only invoked when the rule engine raises flags. The prompt explicitly grounds the model:

> **KEY PRINCIPLE:** In Kubernetes, adding more replicas DISTRIBUTES CPU and memory load across more pods. Scaling up when CPU or memory is high is ALWAYS the correct response — it reduces per-pod pressure. NEVER reject a scale_up action solely because CPU or memory is high.

The LLM's scope is limited to:
- Is the scale jump suspiciously large?
- Is the model confidence too low to trust?
- Are there signs of a flapping loop?

**LLM backends (select via `--llm-provider`):**

| Provider | Flag | Notes |
|---------|------|-------|
| `local` | `--llm-provider local` | Local GGUF via llama-cpp-python. Default model: `data/models/governance_qwen_3b_q4_k_m.gguf`. Slow (~15-20s on CPU). |
| `gemini` | `--llm-provider gemini` | Google Gemini API. Fast. Requires `GEMINI_API_KEY`. |
| `hosted` | `--llm-provider hosted` | OpenAI-compatible endpoint. Requires `HOSTED_LLM_URL` + `HOSTED_LLM_MODEL`. |

If LLM inference takes >5s, a degradation warning is logged. If unavailable, falls back to the `_mock_llm_response()` rule-based fallback.

---

## Governance Outcomes

| Outcome | Meaning |
|---------|---------|
| `APPROVED` | Auto-approved (no flags) or at absolute max |
| `APPROVED_WITH_CAP` | Approved but replicas capped (LLM reduced, or absolute max cap applied) |
| `ESCALATED_TO_LLM` | LLM reviewed and approved at recommended count |
| `REJECTED` | LLM rejected, or anti-flapping triggered, or hard rule rejected |

---

## Governance Config

```python
class GovernanceConfig:
    CONFIDENCE_LOW_THRESHOLD       = 0.50   # flag if confidence below this
    SCALE_JUMP_AGGRESSIVE_RATIO    = 2.0    # flag if replica ratio > 2x
    CPU_FORECAST_HIGH_THRESHOLD    = 0.90   # flag if cpu_p90 > 90% (scale_down/hold only)
    MEMORY_FORECAST_HIGH_THRESHOLD = 0.85   # flag if mem_p90 > 85% (scale_down/hold only)
    SCALE_DOWN_MIN_CONFIDENCE      = 0.65   # flag scale_down if below this
    MAX_REPLICAS_ABSOLUTE          = 20     # hard cap — never exceed 20 in demo/test
```

---

## CLI Usage

```bash
# Production (Redis mode)
python decision_agents/agent4/agent4_governance.py \
  --mode redis \
  --redis-host localhost \
  --redis-port 6380 \
  --llm-provider local

# File mode (test with a JSON payload)
python decision_agents/agent4/agent4_governance.py \
  --payload decision_agents/agent4/dummy_payload.json \
  --llm-provider local
```

---

## Output

- Publishes to `stream:governance:complete`
- Logs to `decision_agents/agent4/agent4_log.txt`
- Single-instance lock: `lock:agent4:governance` (30s TTL, auto-refreshed)

---

## Sample Decision Output

```
============================================================
  GOVERNANCE DECISION
============================================================
  Outcome:           ✅ APPROVED
  Current replicas:  1
  Recommended:       2
  Final approved:    2
  Flags raised:      None
------------------------------------------------------------
  Rule engine:  Rule engine evaluated 0 flag(s): []
------------------------------------------------------------
  Summary: All governance rules passed. Scaling from 1 → 2 replicas approved automatically.
============================================================
  Timestamp: 2026-06-10T07:19:27.972122+00:00
============================================================
```