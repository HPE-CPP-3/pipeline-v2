# Implementation Plan: Unified 4-Agent Pipeline

## Goal
Combine Agent 1 (Ingestion), Agent 2 (Prediction), Agent 3 (Optimization), and Agent 4 (Governance) into a single, unified execution runtime. Update the project requirements, and provide a comprehensive Walkthrough on how to run this unified project natively.

## User Review Required
> [!IMPORTANT]
> The current setup runs Agent 1 and Agent 2 via an imported `asyncio.gather` loop. Agent 3 and Agent 4 are separate modules designed to be run as standalone standalone script files. 
> To run all 4 powerfully and safely in a single terminal process without thread-blocking issues, I propose using Python's `asyncio.gather` combined with the imported modules of all agents. Is a new CLI flag in `runtime.py` (like `--agent all`) acceptable to you?

## Proposed Changes

---

### Pipeline Orchestration

#### [MODIFY] `src/agents/runtime.py`
We will update the main CLI entry point of the pipeline to support a new agent mode: `--agent all`.
- **Imports:** Import the `run_redis_mode` or equivalent async logic from Agent 3 and Agent 4.
- **Argparse:** Add `"all"` to the `--agent` choices.
- **Execution Logic:** When `--agent all` is selected, `asyncio.gather` will launch four concurrent tasks:
  1. `run_ingestion_only`
  2. `run_prediction_only`
  3. `agent3.run_redis_mode`
  4. `agent4.run_redis_mode`

#### [MODIFY] `decision_agents/agent3/agent3_optimization.py`
#### [MODIFY] `decision_agents/agent4/agent4_governance.py`
- Expose their `run_redis_mode` functions purely instead of requiring a namespace of string arguments, or build a tiny config dataclass to pass the redis host/port/keys gracefully when invoked from `runtime.py`.

---

### Dependency Management

#### [MODIFY] `requirements.txt`
Add the external dependencies necessary for Agent 3 and Agent 4 to function if they aren't already grouped in the main requirements file.
```text
redis>=5.0.0
groq>=0.4.0
```

---

### Documentation

#### [NEW] `artifacts/walkthrough.md`
I will replace the old walkthrough with a completely updated guide that explains exactly how this unified architecture runs under the hood, and provides the single simple Bash command needed to fire off the full 4-stage pipeline.

## Verification Plan

1. Verify `requirements.txt` installation completes correctly via `run_command`.
2. Inspect the modified `runtime.py` to ensure all 4 tasks are awaited safely without syntax errors.
3. Provide the full instructions so the user can test the `python -m src.agents.runtime --agent all` command and watch the logs locally.
