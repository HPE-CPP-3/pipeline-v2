# Agent 4 — Governance Update Specification
## What Needs to Change · Why · Exactly How to Implement It

> **Context:** Agent 3 now sends three new fields (`qos_class`, `cpu_request`, `memory_request`) in every payload.  
> Agent 4 currently ignores them. This document tells you exactly what to change so Agent 4 uses them.

---

## 1. Background — What Agent 4 Does Today

Agent 4 is the **final decision gate** before any scaling action reaches Kubernetes. It receives Agent 3's recommendation and either approves, modifies, or rejects it.

Current flow:

```
Agent 3 payload
      ↓
RuleEngine.evaluate()     ← checks four rules, raises flags
      ↓
   No flags?              ← auto-approve
      ↓
   Flags raised?          ← escalate to LLM (Groq / LLaMA)
      ↓
GovernanceDecision        ← APPROVED / APPROVED_WITH_CAP / REJECTED
      ↓
stream:governance:complete
```

This works — but it applies **identical governance rules to every pod**. A critical `Guaranteed` pod and a throwaway `BestEffort` pod get the same confidence thresholds, the same jump-ratio checks, and the LLM gets no context about pod criticality.

---

## 2. What Is Wrong Right Now

### 2.1 The Rule Engine ignores QoS

```python
# Current GovernanceConfig — flat values, no QoS awareness
CONFIDENCE_LOW_THRESHOLD       = 0.50
SCALE_JUMP_AGGRESSIVE_RATIO    = 2.0
SCALE_DOWN_MIN_CONFIDENCE      = 0.65
```

These are applied identically to every pod. A `Guaranteed` pod (critical, never evicted) should have **stricter** thresholds — flag it earlier, require more confidence. A `BestEffort` pod (first to be evicted, throwaway) should have **looser** thresholds — less aggressive governance.

### 2.2 The LLM prompt has no pod criticality context

```python
# Current LLM prompt — qos_class is never mentioned
prompt = f"""
  Pod:               {payload.get('pod')}
  Action:            {payload.get('recommended_action')}
  Current replicas:  {payload.get('current_replicas')}
  Recommended:       {payload.get('recommended_replicas')}
  Confidence:        {payload.get('confidence')}
  CPU  p90 (15m):    {payload.get('cpu_forecast_p90_15m')}
  Mem  p90 (15m):    {payload.get('memory_forecast_p90_15m')}
  Flags raised:      {flag_text}
  # qos_class, cpu_request, memory_request are NEVER sent to the LLM
"""
```

The LLM is being asked to reason about a scaling decision without knowing whether the pod is mission-critical or expendable. That is not enough context for good decisions.

### 2.3 Concrete example of the problem

```
Payments service — QoS: Guaranteed
  Agent 3 recommends: scale_up, 2 → 4 replicas, confidence = 0.48

Current Agent 4 Rule Engine:
  confidence 0.48 < 0.50 threshold → FLAG: low_confidence
  scale ratio 4/2 = 2.0x → within 2.0 threshold → no flag

LLM receives the low_confidence flag and responds:
  "Confidence is low but scale delta is modest. Approving with monitoring warning."

  ← LLM approved a critical payments pod with 48% model confidence
  ← It would give the EXACT same response for a throwaway batch job
  ← The correct answer for a Guaranteed pod at 48% confidence is HOLD
```

---

## 3. What Needs to Change

There are **three changes** required. They are isolated and do not affect anything outside Agent 4.

---

## 4. Change 1 — Add QoS-Aware Thresholds to `GovernanceConfig`

### What to do

Replace the flat threshold constants with a QoS policy table, exactly like Agent 3 did.

### Where

In the `GovernanceConfig` class.

### Code

```python
class GovernanceConfig:

    # ── Existing flat thresholds — keep these as Burstable defaults ──────
    # These are used as fallback when qos_class is missing from the payload.
    CONFIDENCE_LOW_THRESHOLD       = 0.50
    SCALE_JUMP_AGGRESSIVE_RATIO    = 2.0
    CPU_FORECAST_HIGH_THRESHOLD    = 0.90
    MEMORY_FORECAST_HIGH_THRESHOLD = 0.85
    SCALE_DOWN_MIN_CONFIDENCE      = 0.65

    # ── NEW: QoS-aware governance policy table ────────────────────────────
    #
    # Guaranteed: critical pod — flag it earlier, demand more confidence,
    #             never let a low-confidence model scale it down.
    #
    # Burstable:  standard pod — original thresholds preserved exactly.
    #             This is the fallback when qos_class is missing.
    #
    # BestEffort: throwaway pod — looser governance, bigger jumps allowed,
    #             less confidence needed to act.
    #
    QOS_GOVERNANCE_POLICY = {
        "Guaranteed": {
            "confidence_low_threshold":    0.70,   # flag if below 70% — stricter
            "scale_jump_aggressive_ratio": 1.5,    # flag jumps > 1.5x — earlier warning
            "scale_down_min_confidence":   0.90,   # almost never approve scale down
        },
        "Burstable": {
            "confidence_low_threshold":    0.50,   # original value — unchanged
            "scale_jump_aggressive_ratio": 2.0,    # original value — unchanged
            "scale_down_min_confidence":   0.65,   # original value — unchanged
        },
        "BestEffort": {
            "confidence_low_threshold":    0.35,   # very lenient — act even with low confidence
            "scale_jump_aggressive_ratio": 2.5,    # allow bigger jumps
            "scale_down_min_confidence":   0.50,   # scale down easily
        },
    }
```

---

## 5. Change 2 — Update `RuleEngine.evaluate()` to Use QoS Thresholds

### What to do

At the start of `evaluate()`, read `qos_class` from the payload and look up the appropriate thresholds. Then replace the three hardcoded threshold values with the QoS-specific ones.

### Where

In `RuleEngine.evaluate()`. Only three lines change — the rule logic itself stays identical.

### Code

```python
def evaluate(self, payload: dict) -> list[FlagReason]:

    # ── NEW: read QoS class and resolve thresholds ────────────────────
    qos_class  = payload.get("qos_class", "Burstable")   # default to Burstable if missing
    qos_policy = self.cfg.QOS_GOVERNANCE_POLICY.get(
        qos_class,
        self.cfg.QOS_GOVERNANCE_POLICY["Burstable"]       # safe fallback
    )

    # Pull QoS-specific threshold values
    # These replace the hardcoded self.cfg.CONFIDENCE_LOW_THRESHOLD etc.
    conf_threshold      = qos_policy["confidence_low_threshold"]
    aggressive_ratio    = qos_policy["scale_jump_aggressive_ratio"]
    scale_down_min_conf = qos_policy["scale_down_min_confidence"]
    # ─────────────────────────────────────────────────────────────────

    flags: list[FlagReason] = []

    confidence        = payload.get("confidence", 1.0)
    current_replicas  = payload.get("current_replicas", 1)
    recommended       = payload.get("recommended_replicas", current_replicas)
    action            = payload.get("recommended_action", "none")
    cpu_p90_15m       = float(payload.get("cpu_forecast_p90_15m", 0.0) or 0.0)

    # Memory normalisation — unchanged from original
    memory_p90_15m_raw = float(payload.get("memory_forecast_p90_15m", 0.0) or 0.0)
    memory_limit       = float(payload.get("memory_limit", 0.0) or 0.0)
    if memory_p90_15m_raw > 10.0:
        memory_p90_15m = (memory_p90_15m_raw / memory_limit) if memory_limit > 0 else None
    else:
        memory_p90_15m = memory_p90_15m_raw

    # Rule 1 — Low confidence
    # CHANGED: uses conf_threshold (QoS-aware) instead of self.cfg.CONFIDENCE_LOW_THRESHOLD
    if confidence < conf_threshold:
        flags.append(FlagReason.LOW_CONFIDENCE)
        logger.info(
            f"  [FLAG] {FlagReason.LOW_CONFIDENCE}: "
            f"confidence={confidence:.2f} < threshold={conf_threshold} "
            f"(QoS={qos_class})"
        )

    # Rule 2 — Aggressive scale-up
    # CHANGED: uses aggressive_ratio (QoS-aware) instead of self.cfg.SCALE_JUMP_AGGRESSIVE_RATIO
    if action == "scale_up" and current_replicas > 0:
        ratio = recommended / current_replicas
        if ratio > aggressive_ratio:
            flags.append(FlagReason.AGGRESSIVE_SCALE)
            logger.info(
                f"  [FLAG] {FlagReason.AGGRESSIVE_SCALE}: "
                f"{current_replicas} → {recommended} = {ratio:.1f}x "
                f"(threshold={aggressive_ratio}x, QoS={qos_class})"
            )

    # Rule 3 — High memory pressure
    # UNCHANGED — memory risk threshold is not QoS-specific
    if memory_p90_15m is not None and memory_p90_15m > self.cfg.MEMORY_FORECAST_HIGH_THRESHOLD:
        flags.append(FlagReason.HIGH_MEMORY_RISK)
        logger.info(
            f"  [FLAG] {FlagReason.HIGH_MEMORY_RISK}: "
            f"memory_p90_15m={memory_p90_15m:.2f} > {self.cfg.MEMORY_FORECAST_HIGH_THRESHOLD}"
        )

    # Rule 4 — Scale-down with low confidence
    # CHANGED: uses scale_down_min_conf (QoS-aware) instead of self.cfg.SCALE_DOWN_MIN_CONFIDENCE
    if action == "scale_down" and confidence < scale_down_min_conf:
        flags.append(FlagReason.SCALE_DOWN_RISK)
        logger.info(
            f"  [FLAG] {FlagReason.SCALE_DOWN_RISK}: "
            f"scale_down with confidence={confidence:.2f} < {scale_down_min_conf} "
            f"(QoS={qos_class})"
        )

    return flags
```

### What changed vs original

| Rule | Before | After |
|---|---|---|
| Rule 1 — confidence | `self.cfg.CONFIDENCE_LOW_THRESHOLD` (0.50 always) | `conf_threshold` from QoS policy |
| Rule 2 — jump ratio | `self.cfg.SCALE_JUMP_AGGRESSIVE_RATIO` (2.0 always) | `aggressive_ratio` from QoS policy |
| Rule 3 — memory | `self.cfg.MEMORY_FORECAST_HIGH_THRESHOLD` (0.85) | **Unchanged** — memory risk is not QoS-specific |
| Rule 4 — scale down | `self.cfg.SCALE_DOWN_MIN_CONFIDENCE` (0.65 always) | `scale_down_min_conf` from QoS policy |

---

## 6. Change 3 — Add QoS Context to the LLM Prompt

### What to do

Add `qos_class`, `cpu_request`, and `memory_request` to the prompt string inside `LLMReasoner.reason()`. Also add a plain-English explanation of what each QoS class means so the LLM can use it correctly.

### Where

In `LLMReasoner.reason()`, in the `prompt` f-string.

### Code

```python
async def reason(self, payload: dict, flags: list[FlagReason]) -> tuple[str, bool, int]:

    flag_text = ", ".join(f.value for f in flags) if flags else "none"

    # ── NEW: read QoS fields from payload ─────────────────────────────
    qos_class      = payload.get("qos_class",      "Burstable")
    cpu_request    = payload.get("cpu_request",    "unknown")
    memory_request = payload.get("memory_request", "unknown")

    # ── QoS description for the LLM ───────────────────────────────────
    qos_descriptions = {
        "Guaranteed":  "CRITICAL pod — cpu_request equals cpu_limit and mem_request equals mem_limit. This pod is never evicted under memory pressure. Be conservative: require higher confidence, avoid aggressive scale-down.",
        "Burstable":   "STANDARD pod — requests are lower than limits. Normal risk tolerance applies.",
        "BestEffort":  "THROWAWAY pod — no resource requests or limits are set. First to be evicted under cluster pressure. Aggressive scaling is acceptable.",
    }
    qos_description = qos_descriptions.get(qos_class, "Unknown QoS class — treat as standard.")

    prompt = f"""
You are a Kubernetes autoscaling governance agent.
Review the following scaling decision, and output a JSON object containing EXACTLY these keys:
{{"should_approve": true/false, "final_replicas": integer, "reasoning": "brief explanation"}}

  Pod:               {payload.get('pod')}
  Namespace:         {payload.get('namespace')}
  Action:            {payload.get('recommended_action')}
  Current replicas:  {payload.get('current_replicas')}
  Recommended:       {payload.get('recommended_replicas')}
  Confidence score:  {payload.get('confidence')} (0.0 = unreliable, 1.0 = fully reliable)
  CPU  p90 (15m):    {payload.get('cpu_forecast_p90_15m')}
  Mem  p90 (15m):    {payload.get('memory_forecast_p90_15m')}
  Reason from Agent 3: {payload.get('reason', 'N/A')}
  Flags raised:      {flag_text}

  QoS Class:         {qos_class}
  CPU  request:      {cpu_request} cores
  Memory request:    {memory_request} bytes
  QoS meaning:       {qos_description}
"""
```

### Why this matters

The LLM now knows:
- Whether this is a critical pod or a throwaway
- What the actual resource requests are (not just limits)
- A plain-English guide on how to weight its decision accordingly

---

## 7. Impact — Before vs After

### Scenario: Guaranteed pod, confidence = 0.48, scale_up 2 → 4

```
BEFORE:
  Rule 1: 0.48 < 0.50 → FLAG low_confidence
  Rule 2: 4/2 = 2.0x, not > 2.0 → no flag
  LLM: "Confidence low but delta modest. Approving with monitoring warning."
  Outcome: APPROVED  ← wrong for a critical pod

AFTER:
  Rule 1: 0.48 < 0.70 (Guaranteed threshold) → FLAG low_confidence
  Rule 2: 4/2 = 2.0x > 1.5 (Guaranteed threshold) → FLAG aggressive_scale
  LLM receives both flags + "CRITICAL pod — be conservative"
  LLM: "Two flags on a critical pod with low confidence. Rejecting. Hold at 2."
  Outcome: REJECTED or APPROVED_WITH_CAP  ← correct
```

### Scenario: BestEffort pod, confidence = 0.40, scale_up 3 → 6

```
BEFORE:
  Rule 1: 0.40 < 0.50 → FLAG low_confidence
  LLM review triggered even for a throwaway pod
  Outcome: unnecessary LLM overhead

AFTER:
  Rule 1: 0.40 > 0.35 (BestEffort threshold) → no flag
  Rule 2: 6/3 = 2.0x, not > 2.5 (BestEffort threshold) → no flag
  Outcome: APPROVED automatically  ← correct, no LLM call needed
```

### Full comparison table

| Scenario | Before | After |
|---|---|---|
| Guaranteed pod, `conf=0.48` | Approved with warning | Flagged (threshold=0.70) → LLM rejects |
| Guaranteed pod, `2 → 3` replicas | Auto-approved | Flagged (ratio > 1.5x) → LLM review |
| Guaranteed pod, scale down | Approved at `conf=0.65` | Rejected unless `conf >= 0.90` |
| **Burstable pod (any scenario)** | **Original behaviour** | **Identical to original** |
| BestEffort pod, `conf=0.40` | Flagged → LLM review | Auto-approved (threshold=0.35) |
| BestEffort pod, `3 → 7` replicas | Flagged (ratio 2.3x > 2.0) | Auto-approved (threshold=2.5x) |

---

## 8. What Does NOT Change

| Component | Status |
|---|---|
| `GovernanceOutcome` enum | Unchanged |
| `FlagReason` enum | Unchanged |
| `GovernanceDecision` dataclass | Unchanged |
| `GovernanceAgent.run()` orchestration | Unchanged |
| Rule 3 — HIGH_MEMORY_RISK | Unchanged — memory risk threshold is not QoS-specific |
| `_mock_llm_response()` logic | Unchanged — mock still handles same flag combinations |
| Redis stream names | Unchanged |
| `_RedisSingleInstanceLock` | Unchanged |
| Output to `stream:governance:complete` | Unchanged |

---

## 9. Implementation Checklist

```
[ ] 1. Add QOS_GOVERNANCE_POLICY table to GovernanceConfig
        — keep existing flat constants as-is (used as Burstable defaults)

[ ] 2. In RuleEngine.evaluate():
        — read qos_class from payload at the top of the method
        — look up QOS_GOVERNANCE_POLICY with "Burstable" as fallback
        — replace conf threshold in Rule 1 with conf_threshold from policy
        — replace jump ratio in Rule 2 with aggressive_ratio from policy
        — replace scale_down confidence in Rule 4 with scale_down_min_conf from policy
        — Rule 3 (memory) stays unchanged

[ ] 3. In LLMReasoner.reason():
        — read qos_class, cpu_request, memory_request from payload
        — add QoS fields + description to the prompt f-string

[ ] 4. Test with three payloads:
        — payload with qos_class = "Guaranteed", low confidence → expect stricter flagging
        — payload with qos_class = "Burstable"  → expect identical behaviour to current
        — payload with qos_class missing entirely → expect identical behaviour to current (fallback)
```

---

## 10. Test Payloads

Use these in file mode (`python agent4_governance.py --payload test_X.json`) to verify each change.

### test_guaranteed.json — should be flagged more aggressively

```json
{
    "namespace": "test-workload",
    "pod": "stress-test-app",
    "container": "stress-test-app",
    "recommended_action": "scale_up",
    "current_replicas": 2,
    "recommended_replicas": 4,
    "confidence": 0.48,
    "cpu_forecast_p90_15m": 0.82,
    "memory_forecast_p90_15m": 0.60,
    "cpu_limit": 0.5,
    "memory_limit": 536870912,
    "cpu_request": 0.5,
    "memory_request": 536870912,
    "qos_class": "Guaranteed",
    "reason": "CPU throttle risk is HIGH. QoS=Guaranteed. Scaling 2 -> 4."
}
```

Expected: `conf=0.48 < 0.70 → FLAG low_confidence`, `4/2=2.0x > 1.5 → FLAG aggressive_scale`, LLM reviews and rejects or caps.

---

### test_burstable.json — should behave identically to current Agent 4

```json
{
    "namespace": "test-workload",
    "pod": "stress-test-app",
    "container": "stress-test-app",
    "recommended_action": "scale_up",
    "current_replicas": 2,
    "recommended_replicas": 4,
    "confidence": 0.48,
    "cpu_forecast_p90_15m": 0.82,
    "memory_forecast_p90_15m": 0.60,
    "cpu_limit": 0.5,
    "memory_limit": 536870912,
    "cpu_request": 0.25,
    "memory_request": 268435456,
    "qos_class": "Burstable",
    "reason": "CPU throttle risk is HIGH. QoS=Burstable. Scaling 2 -> 4."
}
```

Expected: `conf=0.48 < 0.50 → FLAG low_confidence`, `4/2=2.0x not > 2.0 → no flag`. Same as current behaviour.

---

### test_no_qos.json — should fall back gracefully to Burstable behaviour

```json
{
    "namespace": "test-workload",
    "pod": "stress-test-app",
    "container": "stress-test-app",
    "recommended_action": "scale_up",
    "current_replicas": 2,
    "recommended_replicas": 4,
    "confidence": 0.48,
    "cpu_forecast_p90_15m": 0.82,
    "memory_forecast_p90_15m": 0.60,
    "cpu_limit": 0.5,
    "memory_limit": 536870912,
    "reason": "CPU throttle risk is HIGH. Scaling 2 -> 4."
}
```

Expected: `qos_class` missing → falls back to Burstable thresholds → identical to current behaviour. Agent 4 must not crash.

---

> **Summary:** Three changes, all isolated.  
> `GovernanceConfig` gets a policy table.  
> `RuleEngine.evaluate()` reads QoS at the top and uses three QoS-aware values.  
> `LLMReasoner.reason()` adds four lines to the prompt.  
> Everything else stays the same.