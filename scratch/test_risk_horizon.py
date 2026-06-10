import sys
import os

sys.path.append(os.path.abspath("."))
from src.prediction.throttle_risk import ThrottleRiskCalculator
from src.prediction.oom_risk import OOMRiskCalculator

def test_throttle_risk():
    # 5m horizon config
    config_5m = {
        "critical_ratio": 0.95,
        "high_ratio": 0.85,
        "current_throttle_ratio_high": 0.1,
        "horizon": 5
    }
    calc_5m = ThrottleRiskCalculator(config_5m)
    assert calc_5m.horizon == 5

    # 15m horizon config
    config_15m = {
        "critical_ratio": 0.95,
        "high_ratio": 0.85,
        "current_throttle_ratio_high": 0.1,
        "horizon": 15
    }
    calc_15m = ThrottleRiskCalculator(config_15m)
    assert calc_15m.horizon == 15

    # Let's say CPU limit is 2.0 cores
    # cpu_forecast has p90 at 5m = 2.0 (100% limit) and p90 at 15m = 0.5 (25% limit)
    cpu_forecast = {
        5: {0.9: 2.0, 0.5: 1.0},
        15: {0.9: 0.5, 0.5: 0.2}
    }

    # Under 5m calculator, the risk should be CRITICAL (2.0/2.0 >= 0.95)
    res_5m = calc_5m.calculate_risk(cpu_forecast, cpu_limit=2.0)
    print("5m Calc Risk Level:", res_5m["risk_level"])
    assert res_5m["risk_level"] == "CRITICAL"

    # Under 15m calculator, the risk should be LOW (0.5/2.0 = 25% limit)
    res_15m = calc_15m.calculate_risk(cpu_forecast, cpu_limit=2.0)
    print("15m Calc Risk Level:", res_15m["risk_level"])
    assert res_15m["risk_level"] == "LOW"

def test_oom_risk():
    config_5m = {
        "critical_ratio": 0.95,
        "high_ratio": 0.85,
        "horizon": 5
    }
    calc_5m = OOMRiskCalculator(config_5m)

    config_15m = {
        "critical_ratio": 0.95,
        "high_ratio": 0.85,
        "horizon": 15
    }
    calc_15m = OOMRiskCalculator(config_15m)

    # Let's say Memory limit is 1000 bytes
    memory_forecast = {
        5: {0.9: 980.0, 0.5: 800.0},
        15: {0.9: 400.0, 0.5: 300.0}
    }

    res_5m = calc_5m.calculate_risk(memory_forecast, memory_limit=1000.0)
    print("5m Memory Risk Level:", res_5m["oom_risk"])
    assert res_5m["oom_risk"] == "CRITICAL"

    res_15m = calc_15m.calculate_risk(memory_forecast, memory_limit=1000.0)
    print("15m Memory Risk Level:", res_15m["oom_risk"])
    assert res_15m["oom_risk"] == "LOW"

if __name__ == "__main__":
    test_throttle_risk()
    test_oom_risk()
    print("ALL RISK HORIZON TESTS PASSED!")
