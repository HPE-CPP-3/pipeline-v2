"""
Prometheus writer for predictions
"""

from datetime import datetime
from typing import Any, Optional
import logging

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

logger = logging.getLogger(__name__)


class PrometheusWriter:
    """
    Write predictions to Prometheus

    Can use either:
    - Pushgateway (for ephemeral jobs)
    - Remote write API (for direct ingestion)
    """

    def __init__(
        self,
        pushgateway_url: Optional[str] = None,
        remote_write_url: Optional[str] = None,
        prefix: str = "pipeline_prediction",
        labels: Optional[dict[str, str]] = None,
    ):
        self.pushgateway_url = pushgateway_url
        self.remote_write_url = remote_write_url
        self.prefix = prefix
        self.labels = labels or {}

        self.registry = CollectorRegistry()

        # Define metrics
        self._create_metrics()

        logger.info(f"Initialized PrometheusWriter (prefix={prefix})")

    def _create_metrics(self):
        """Create Prometheus metrics"""
        # CPU predictions
        self.cpu_p50 = Gauge(
            f"{self.prefix}_cpu_p50",
            "CPU usage forecast (p50)",
            ["pod", "namespace", "container", "horizon"],
            registry=self.registry,
        )

        self.cpu_p70 = Gauge(
            f"{self.prefix}_cpu_p70",
            "CPU usage forecast (p70)",
            ["pod", "namespace", "container", "horizon"],
            registry=self.registry,
        )

        self.cpu_p90 = Gauge(
            f"{self.prefix}_cpu_p90",
            "CPU usage forecast (p90)",
            ["pod", "namespace", "container", "horizon"],
            registry=self.registry,
        )

        # Memory predictions
        self.memory_p50 = Gauge(
            f"{self.prefix}_memory_p50",
            "Memory usage forecast (p50)",
            ["pod", "namespace", "container", "horizon"],
            registry=self.registry,
        )

        self.memory_p70 = Gauge(
            f"{self.prefix}_memory_p70",
            "Memory usage forecast (p70)",
            ["pod", "namespace", "container", "horizon"],
            registry=self.registry,
        )

        self.memory_p90 = Gauge(
            f"{self.prefix}_memory_p90",
            "Memory usage forecast (p90)",
            ["pod", "namespace", "container", "horizon"],
            registry=self.registry,
        )

        # Risk metrics
        self.cpu_throttle_risk = Gauge(
            f"{self.prefix}_cpu_throttle_risk",
            "CPU throttle risk (0-1)",
            ["pod", "namespace", "container"],
            registry=self.registry,
        )

        self.memory_oom_risk = Gauge(
            f"{self.prefix}_memory_oom_risk",
            "Memory OOM risk (0-1)",
            ["pod", "namespace", "container"],
            registry=self.registry,
        )

        # Drift metrics
        self.drift_ratio = Gauge(
            f"{self.prefix}_drift_ratio",
            "Fraction of features with drift",
            ["pod", "namespace"],
            registry=self.registry,
        )

        self.prediction_mae_ratio = Gauge(
            f"{self.prefix}_mae_ratio",
            "Prediction error ratio (current/baseline)",
            ["pod", "namespace"],
            registry=self.registry,
        )

    def write_predictions(
        self,
        pod: str,
        namespace: str,
        container: str,
        predictions: dict[str, Any],
    ):
        """
        Write predictions to Prometheus

        Args:
            pod: Pod name
            namespace: Namespace
            container: Container name
            predictions: Prediction dict with cpu_forecast, memory_forecast, risks
        """
        labels = {
            "pod": pod,
            "namespace": namespace,
            "container": container,
            **self.labels,
        }

        # CPU forecasts
        if "cpu_forecast" in predictions:
            for horizon, quantiles in predictions["cpu_forecast"].items():
                horizon_str = str(horizon).replace("min", "")

                if "p50" in quantiles:
                    self.cpu_p50.labels(**labels, horizon=horizon_str).set(
                        quantiles["p50"]
                    )
                if "p70" in quantiles:
                    self.cpu_p70.labels(**labels, horizon=horizon_str).set(
                        quantiles["p70"]
                    )
                if "p90" in quantiles:
                    self.cpu_p90.labels(**labels, horizon=horizon_str).set(
                        quantiles["p90"]
                    )

        # Memory forecasts
        if "memory_forecast" in predictions:
            for horizon, quantiles in predictions["memory_forecast"].items():
                horizon_str = str(horizon).replace("min", "")

                if "p50" in quantiles:
                    self.memory_p50.labels(**labels, horizon=horizon_str).set(
                        quantiles["p50"]
                    )
                if "p70" in quantiles:
                    self.memory_p70.labels(**labels, horizon=horizon_str).set(
                        quantiles["p70"]
                    )
                if "p90" in quantiles:
                    self.memory_p90.labels(**labels, horizon=horizon_str).set(
                        quantiles["p90"]
                    )

        # Risk metrics
        if "risks" in predictions:
            if "cpu_throttle" in predictions["risks"]:
                risk = predictions["risks"]["cpu_throttle"]
                if "probability" in risk:
                    self.cpu_throttle_risk.labels(**labels).set(risk["probability"])

            if "memory_oom" in predictions["risks"]:
                risk = predictions["risks"]["memory_oom"]
                if "probability" in risk:
                    self.memory_oom_risk.labels(**labels).set(risk["probability"])

        # Push to gateway
        if self.pushgateway_url:
            self._push_metrics()

    def write_drift_metrics(
        self,
        pod: str,
        namespace: str,
        drift_ratio: float,
        mae_ratio: Optional[float] = None,
    ):
        """
        Write drift detection metrics

        Args:
            pod: Pod name
            namespace: Namespace
            drift_ratio: Fraction of features drifting
            mae_ratio: Current MAE / baseline MAE
        """
        labels = {"pod": pod, "namespace": namespace, **self.labels}

        self.drift_ratio.labels(**labels).set(drift_ratio)

        if mae_ratio is not None:
            self.prediction_mae_ratio.labels(**labels).set(mae_ratio)

        # Push to gateway
        if self.pushgateway_url:
            self._push_metrics()

    def _push_metrics(self):
        """Push metrics to Pushgateway"""
        if not self.pushgateway_url:
            return

        try:
            push_to_gateway(
                self.pushgateway_url,
                job="pipeline-v2",
                registry=self.registry,
                grouping_key=self.labels,
            )
            logger.debug("Pushed metrics to Pushgateway")
        except Exception as e:
            logger.error(f"Failed to push metrics: {e}")

    def clear_metrics(self, pod: str, namespace: str, container: str):
        """
        Clear metrics for a specific pod (cleanup)
        """
        labels = {"pod": pod, "namespace": namespace, "container": container}

        # Remove all metrics for this pod
        self.cpu_p50.remove(*labels.values())
        self.cpu_p70.remove(*labels.values())
        self.cpu_p90.remove(*labels.values())
        self.memory_p50.remove(*labels.values())
        self.memory_p70.remove(*labels.values())
        self.memory_p90.remove(*labels.values())
        self.cpu_throttle_risk.remove(*labels.values())
        self.memory_oom_risk.remove(*labels.values())
