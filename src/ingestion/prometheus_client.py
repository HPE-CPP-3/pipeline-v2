"""
Prometheus client for querying metrics
"""

from typing import Any, Optional
from datetime import datetime, timedelta, timezone
import logging

from prometheus_api_client import PrometheusConnect
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


class PrometheusClient:
    """
    Client for querying Prometheus server

    Uses pre-computed recording rules for efficiency:
    - pipeline:container_cpu_usage_rate:5m
    - pipeline:container_memory_usage_bytes
    - etc.
    """

    def __init__(
        self,
        url: str,
        timeout_seconds: int = 30,
        max_retries: int = 3,
        retry_delay_seconds: int = 2,
    ):
        self.url = url
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries

        self.client = PrometheusConnect(
            url=url,
            disable_ssl=True,
            retry=max_retries,
        )

        logger.info(f"Initialized Prometheus client at {url}")

    def _selector(
        self,
        namespace: str,
        pod_name: str,
        container_name: Optional[str] = None,
    ) -> str:
        selector = f'namespace="{namespace}", pod="{pod_name}"'
        if container_name:
            selector += f', container="{container_name}"'
        return selector

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
    )
    def query(
        self, query: str, timestamp: Optional[datetime] = None
    ) -> list[dict[str, Any]]:
        """
        Execute instant query

        Args:
            query: PromQL query string
            timestamp: Optional timestamp (defaults to now)

        Returns:
            List of metric dictionaries with labels and values
        """
        try:
            result = self.client.custom_query(
                query=query,
                timeout=self.timeout_seconds,
            )
            logger.debug(f"Query executed: {query[:100]}...")
            return result
        except Exception as e:
            logger.error(f"Query failed: {e}")
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
    )
    def query_range(
        self,
        query: str,
        start: datetime,
        end: datetime,
        step: int = 60,
    ) -> list[dict[str, Any]]:
        """
        Execute range query

        Args:
            query: PromQL query string
            start: Start timestamp
            end: End timestamp
            step: Step size in seconds (default: 60)

        Returns:
            List of metric dictionaries with time series data
        """
        try:
            result = self.client.custom_query_range(
                query=query,
                start_time=start,
                end_time=end,
                step=f"{step}s",
                timeout=self.timeout_seconds,
            )
            logger.debug(f"Range query executed: {query[:100]}...")
            return result
        except Exception as e:
            logger.error(f"Range query failed: {e}")
            raise

    async def get_active_pods(
        self, namespace: Optional[str] = None
    ) -> list[dict[str, str]]:
        """
        Get list of active pods

        Args:
            namespace: Optional namespace filter

        Returns:
            List of dicts with pod name, namespace, container
        """
        if namespace:
            query = f'kube_pod_status_phase{{namespace="{namespace}", phase="Running"}}'
        else:
            query = 'kube_pod_status_phase{phase="Running"}'

        result = self.query(query)

        pods = []
        for metric in result:
            labels = metric.get("metric", {})
            pods.append(
                {
                    "pod": labels.get("pod", ""),
                    "namespace": labels.get("namespace", ""),
                    "container": labels.get("container", ""),
                }
            )

        logger.info(f"Found {len(pods)} active pods")
        return pods

    async def get_container_cpu_usage(
        self,
        pod: str,
        namespace: str,
        container: Optional[str] = None,
        window_minutes: int = 60,
    ) -> list[tuple[datetime, float]]:
        """
        Get CPU usage rate for container

        Uses pre-computed recording rule: pipeline:container_cpu_usage_rate:5m
        """
        if container:
            query = f'pipeline:container_cpu_usage_rate:5m{{pod="{pod}", namespace="{namespace}", container="{container}"}}'
        else:
            query = f'pipeline:container_cpu_usage_rate:5m{{pod="{pod}", namespace="{namespace}"}}'

        end = datetime.now()
        start = end - timedelta(minutes=window_minutes)

        result = self.query_range(query, start, end, step=60)

        if not result:
            return []

        # Parse time series
        values = result[0].get("values", [])
        return [(datetime.fromtimestamp(ts), float(val)) for ts, val in values]

    async def get_container_cpu_usage_seconds_total(
        self,
        namespace: str,
        pod_name: str,
        container_name: Optional[str] = None,
        window_minutes: int = 60,
    ) -> list[tuple[datetime, float]]:
        selector = self._selector(namespace, pod_name, container_name)
        query = f'rate(container_cpu_usage_seconds_total{{{selector}, container!="", image!=""}}[5m])'
        end = datetime.now()
        start = end - timedelta(minutes=window_minutes)
        result = self.query_range(query, start, end, step=60)
        if not result:
            return []
        values = result[0].get("values", [])
        return [
            (datetime.fromtimestamp(ts, tz=timezone.utc), float(val))
            for ts, val in values
        ]

    async def get_container_cpu_throttled_seconds_total(
        self,
        namespace: str,
        pod_name: str,
        container_name: Optional[str] = None,
        window_minutes: int = 60,
    ) -> list[tuple[datetime, float]]:
        selector = self._selector(namespace, pod_name, container_name)
        query = f'rate(container_cpu_cfs_throttled_seconds_total{{{selector}, container!="", image!=""}}[5m])'
        end = datetime.now()
        start = end - timedelta(minutes=window_minutes)
        result = self.query_range(query, start, end, step=60)
        if not result:
            return []
        values = result[0].get("values", [])
        return [
            (datetime.fromtimestamp(ts, tz=timezone.utc), float(val))
            for ts, val in values
        ]

    async def get_container_memory_usage(
        self,
        pod: str,
        namespace: str,
        container: Optional[str] = None,
        window_minutes: int = 60,
    ) -> list[tuple[datetime, float]]:
        """
        Get memory usage for container

        Uses pre-computed recording rule: pipeline:container_memory_usage_bytes
        """
        if container:
            query = f'pipeline:container_memory_usage_bytes{{pod="{pod}", namespace="{namespace}", container="{container}"}}'
        else:
            query = f'pipeline:container_memory_usage_bytes{{pod="{pod}", namespace="{namespace}"}}'

        end = datetime.now()
        start = end - timedelta(minutes=window_minutes)

        result = self.query_range(query, start, end, step=60)

        if not result:
            return []

        values = result[0].get("values", [])
        return [(datetime.fromtimestamp(ts), float(val)) for ts, val in values]

    async def get_container_memory_working_set(
        self,
        pod: str,
        namespace: str,
        container: Optional[str] = None,
        window_minutes: int = 60,
    ) -> list[tuple[datetime, float]]:
        if container:
            query = f'pipeline:container_memory_working_set_bytes{{pod="{pod}", namespace="{namespace}", container="{container}"}}'
        else:
            query = f'pipeline:container_memory_working_set_bytes{{pod="{pod}", namespace="{namespace}"}}'

        end = datetime.now()
        start = end - timedelta(minutes=window_minutes)

        result = self.query_range(query, start, end, step=60)
        if not result:
            return []

        values = result[0].get("values", [])
        return [(datetime.fromtimestamp(ts), float(val)) for ts, val in values]

    async def get_container_memory_working_set_bytes(
        self,
        namespace: str,
        pod_name: str,
        container_name: Optional[str] = None,
        window_minutes: int = 60,
    ) -> list[tuple[datetime, float]]:
        selector = self._selector(namespace, pod_name, container_name)
        query = f'container_memory_working_set_bytes{{{selector}, container!="", image!=""}}'
        end = datetime.now()
        start = end - timedelta(minutes=window_minutes)
        result = self.query_range(query, start, end, step=60)
        if not result:
            return []
        values = result[0].get("values", [])
        return [
            (datetime.fromtimestamp(ts, tz=timezone.utc), float(val))
            for ts, val in values
        ]

    async def get_container_memory_failures_total(
        self,
        namespace: str,
        pod_name: str,
        container_name: Optional[str] = None,
        window_minutes: int = 60,
    ) -> list[tuple[datetime, float]]:
        selector = self._selector(namespace, pod_name, container_name)
        query = f'rate(container_memory_failures_total{{{selector}, container!="", image!=""}}[5m])'
        end = datetime.now()
        start = end - timedelta(minutes=window_minutes)
        result = self.query_range(query, start, end, step=60)
        if not result:
            return []
        values = result[0].get("values", [])
        return [
            (datetime.fromtimestamp(ts, tz=timezone.utc), float(val))
            for ts, val in values
        ]

    async def get_container_memory_cache(
        self,
        pod: str,
        namespace: str,
        container: Optional[str] = None,
        window_minutes: int = 60,
    ) -> list[tuple[datetime, float]]:
        if container:
            query = f'pipeline:container_memory_cache_bytes{{pod="{pod}", namespace="{namespace}", container="{container}"}}'
        else:
            query = f'pipeline:container_memory_cache_bytes{{pod="{pod}", namespace="{namespace}"}}'

        end = datetime.now()
        start = end - timedelta(minutes=window_minutes)

        result = self.query_range(query, start, end, step=60)
        if not result:
            return []

        values = result[0].get("values", [])
        return [(datetime.fromtimestamp(ts), float(val)) for ts, val in values]

    async def get_container_memory_failcount(
        self,
        pod: str,
        namespace: str,
        container: Optional[str] = None,
        window_minutes: int = 60,
    ) -> list[tuple[datetime, float]]:
        end = datetime.now()
        start = end - timedelta(minutes=window_minutes)

        if container:
            primary_query = f'container_memory_failcnt{{pod="{pod}", namespace="{namespace}", container="{container}"}}'
            fallback_query = f'pipeline:container_memory_failcnt:rate1h{{pod="{pod}", namespace="{namespace}", container="{container}"}}'
        else:
            primary_query = (
                f'container_memory_failcnt{{pod="{pod}", namespace="{namespace}"}}'
            )
            fallback_query = f'pipeline:container_memory_failcnt:rate1h{{pod="{pod}", namespace="{namespace}"}}'

        result = self.query_range(primary_query, start, end, step=60)
        if not result:
            result = self.query_range(fallback_query, start, end, step=60)
        if not result:
            return []

        values = result[0].get("values", [])
        return [(datetime.fromtimestamp(ts), float(val)) for ts, val in values]

    async def get_cpu_throttle_ratio(
        self,
        pod: str,
        namespace: str,
        container: Optional[str] = None,
        window_minutes: int = 60,
    ) -> list[tuple[datetime, float]]:
        """
        Get CPU throttling ratio

        Uses pre-computed recording rule: pipeline:container_cpu_throttle_ratio:5m
        """
        if container:
            query = f'pipeline:container_cpu_throttle_ratio:5m{{pod="{pod}", namespace="{namespace}", container="{container}"}}'
        else:
            query = f'pipeline:container_cpu_throttle_ratio:5m{{pod="{pod}", namespace="{namespace}"}}'

        end = datetime.now()
        start = end - timedelta(minutes=window_minutes)

        result = self.query_range(query, start, end, step=60)

        if not result:
            return []

        values = result[0].get("values", [])
        return [(datetime.fromtimestamp(ts), float(val)) for ts, val in values]

    async def get_node_metrics(
        self,
        node: str,
        window_minutes: int = 60,
    ) -> dict[str, list[tuple[datetime, float]]]:
        """
        Get node-level metrics

        Returns dict with:
        - cpu_usage_rate
        - load_ratio
        - memory_available_ratio
        - disk_io_time
        - network_drop_ratio
        """
        end = datetime.now()
        start = end - timedelta(minutes=window_minutes)

        queries = {
            "cpu_usage_rate": f'pipeline:node_cpu_usage_rate:5m{{instance="{node}"}}',
            "load_ratio": f'pipeline:node_load_ratio{{instance="{node}"}}',
            "memory_available_ratio": f'pipeline:node_memory_available_ratio{{instance="{node}"}}',
            "disk_io_time": f'pipeline:node_disk_io_time:rate5m{{instance="{node}"}}',
            "network_drop_ratio": f'pipeline:node_network_drop_ratio{{instance="{node}"}}',
        }

        metrics = {}
        for name, query in queries.items():
            result = self.query_range(query, start, end, step=60)
            if result:
                values = result[0].get("values", [])
                metrics[name] = [
                    (datetime.fromtimestamp(ts), float(val)) for ts, val in values
                ]
            else:
                metrics[name] = []

        return metrics

    async def get_resource_limits(
        self,
        pod: str,
        namespace: str,
        container: Optional[str] = None,
    ) -> dict[str, float]:
        """
        Get CPU and memory limits for container
        """
        if container:
            cpu_query = f'kube_pod_container_resource_limits{{resource="cpu", pod="{pod}", namespace="{namespace}", container="{container}"}}'
            mem_query = f'kube_pod_container_resource_limits{{resource="memory", pod="{pod}", namespace="{namespace}", container="{container}"}}'
        else:
            cpu_query = f'kube_pod_container_resource_limits{{resource="cpu", pod="{pod}", namespace="{namespace}"}}'
            mem_query = f'kube_pod_container_resource_limits{{resource="memory", pod="{pod}", namespace="{namespace}"}}'

        limits = {"cpu_cores": 0.0, "memory_bytes": 0.0}

        cpu_result = self.query(cpu_query)
        if cpu_result:
            limits["cpu_cores"] = float(cpu_result[0].get("value", [0, 0])[1])

        mem_result = self.query(mem_query)
        if mem_result:
            limits["memory_bytes"] = float(mem_result[0].get("value", [0, 0])[1])

        return limits

    async def get_resource_requests(
        self,
        pod: str,
        namespace: str,
        container: Optional[str] = None,
    ) -> dict[str, float]:
        if container:
            cpu_query = f'kube_pod_container_resource_requests{{resource="cpu", pod="{pod}", namespace="{namespace}", container="{container}"}}'
            mem_query = f'kube_pod_container_resource_requests{{resource="memory", pod="{pod}", namespace="{namespace}", container="{container}"}}'
        else:
            cpu_query = f'kube_pod_container_resource_requests{{resource="cpu", pod="{pod}", namespace="{namespace}"}}'
            mem_query = f'kube_pod_container_resource_requests{{resource="memory", pod="{pod}", namespace="{namespace}"}}'

        requests = {"cpu_cores": 0.0, "memory_bytes": 0.0}

        cpu_result = self.query(cpu_query)
        if cpu_result:
            requests["cpu_cores"] = float(cpu_result[0].get("value", [0, 0])[1])

        mem_result = self.query(mem_query)
        if mem_result:
            requests["memory_bytes"] = float(mem_result[0].get("value", [0, 0])[1])

        return requests

    async def get_pod_restart_count(
        self,
        pod: str,
        namespace: str,
    ) -> float:
        query = f'sum(kube_pod_container_status_restarts_total{{pod="{pod}", namespace="{namespace}"}})'
        result = self.query(query)
        if result:
            return float(result[0].get("value", [0, 0])[1])
        return 0.0

    async def get_pod_status_phase(self, pod: str, namespace: str) -> str:
        query = f'pipeline:pod_status_phase{{pod="{pod}", namespace="{namespace}"}}'
        result = self.query(query)
        if not result:
            return "Unknown"

        for sample in result:
            value = float(sample.get("value", [0, 0])[1])
            if value == 1.0:
                metric = sample.get("metric", {})
                return metric.get("phase", "Unknown")

        metric = result[0].get("metric", {})
        return metric.get("phase", "Unknown")

    async def get_pod_start_time_seconds(self, pod: str, namespace: str) -> float:
        query = f'kube_pod_start_time{{pod="{pod}", namespace="{namespace}"}}'
        result = self.query(query)
        if result:
            return float(result[0].get("value", [0, 0])[1])
        return 0.0

    async def get_pod_pending_time_seconds(self, pod: str, namespace: str) -> float:
        query = (
            f'time() - kube_pod_created{{pod="{pod}", namespace="{namespace}"}} '
            f'and on(pod, namespace) kube_pod_status_phase{{pod="{pod}", namespace="{namespace}", phase="Pending"}} == 1'
        )
        result = self.query(query)
        if result:
            return float(result[0].get("value", [0, 0])[1])
        return 0.0

    async def get_pod_eviction_count(self, pod: str, namespace: str) -> float:
        query = f'increase(kube_event_count{{involved_object_kind="Pod", involved_object_name="{pod}", namespace="{namespace}", reason="Evicted"}}[24h])'
        result = self.query(query)
        if result:
            return float(result[0].get("value", [0, 0])[1])
        return 0.0

    async def get_node_disk_metrics(
        self,
        node: str,
        window_minutes: int = 60,
    ) -> dict[str, list[tuple[datetime, float]]]:
        end = datetime.now()
        start = end - timedelta(minutes=window_minutes)
        queries = {
            "node_disk_read_throughput_bytes_per_sec": f'pipeline:node_disk_read_bytes:rate5m{{instance="{node}"}}',
            "node_disk_write_throughput_bytes_per_sec": f'pipeline:node_disk_written_bytes:rate5m{{instance="{node}"}}',
            "node_disk_io_time_ratio": f'pipeline:node_disk_io_time:rate5m{{instance="{node}"}}',
            "node_disk_queue_length": f'pipeline:node_disk_io_queue_length{{instance="{node}"}}',
        }

        metrics: dict[str, list[tuple[datetime, float]]] = {}
        for name, query in queries.items():
            result = self.query_range(query, start, end, step=60)
            if not result:
                metrics[name] = []
                continue
            values = result[0].get("values", [])
            metrics[name] = [
                (datetime.fromtimestamp(ts), float(val)) for ts, val in values
            ]
        return metrics

    async def get_node_network_metrics(
        self,
        node: str,
        window_minutes: int = 60,
    ) -> dict[str, list[tuple[datetime, float]]]:
        end = datetime.now()
        start = end - timedelta(minutes=window_minutes)
        queries = {
            "node_network_bytes_in_per_sec": f'pipeline:node_network_receive_bytes:rate5m{{instance="{node}"}}',
            "node_network_bytes_out_per_sec": f'pipeline:node_network_transmit_bytes:rate5m{{instance="{node}"}}',
            "node_network_packet_drops_ratio": f'pipeline:node_network_drop_ratio{{instance="{node}"}}',
            "node_network_errors_ratio": f'pipeline:node_network_error_ratio{{instance="{node}"}}',
        }

        metrics: dict[str, list[tuple[datetime, float]]] = {}
        for name, query in queries.items():
            result = self.query_range(query, start, end, step=60)
            if not result:
                metrics[name] = []
                continue
            values = result[0].get("values", [])
            metrics[name] = [
                (datetime.fromtimestamp(ts), float(val)) for ts, val in values
            ]
        return metrics

    async def get_node_memory_metrics(
        self,
        node: str,
        window_minutes: int = 60,
    ) -> dict[str, list[tuple[datetime, float]]]:
        end = datetime.now()
        start = end - timedelta(minutes=window_minutes)
        queries = {
            "node_memory_available_bytes": f'node_memory_MemAvailable_bytes{{instance="{node}"}}',
            "node_memory_free_bytes": f'node_memory_MemFree_bytes{{instance="{node}"}}',
            "node_memory_cache_bytes": f'node_memory_Cached_bytes{{instance="{node}"}}',
            "node_memory_buffers_bytes": f'node_memory_Buffers_bytes{{instance="{node}"}}',
            "node_memory_cache_buffers_bytes": f'(node_memory_Cached_bytes{{instance="{node}"}} + node_memory_Buffers_bytes{{instance="{node}"}})',
        }

        metrics: dict[str, list[tuple[datetime, float]]] = {}
        for name, query in queries.items():
            result = self.query_range(query, start, end, step=60)
            if not result:
                metrics[name] = []
                continue
            values = result[0].get("values", [])
            metrics[name] = [
                (datetime.fromtimestamp(ts), float(val)) for ts, val in values
            ]
        return metrics

    async def get_node_cpu_modes(
        self,
        node: str,
        window_minutes: int = 60,
    ) -> dict[str, list[tuple[datetime, float]]]:
        end = datetime.now()
        start = end - timedelta(minutes=window_minutes)
        queries = {
            "node_cpu_usage_mode_user": f'rate(node_cpu_seconds_total{{instance="{node}", mode="user"}}[5m])',
            "node_cpu_usage_mode_system": f'rate(node_cpu_seconds_total{{instance="{node}", mode="system"}}[5m])',
            "node_cpu_usage_mode_idle": f'rate(node_cpu_seconds_total{{instance="{node}", mode="idle"}}[5m])',
            "node_cpu_usage_mode_iowait": f'rate(node_cpu_seconds_total{{instance="{node}", mode="iowait"}}[5m])',
        }
        metrics: dict[str, list[tuple[datetime, float]]] = {}
        for name, query in queries.items():
            result = self.query_range(query, start, end, step=60)
            if not result:
                metrics[name] = []
                continue
            values = result[0].get("values", [])
            metrics[name] = [
                (datetime.fromtimestamp(ts), float(val)) for ts, val in values
            ]
        return metrics

    async def get_node_load_averages(
        self,
        node: str,
        window_minutes: int = 60,
    ) -> dict[str, list[tuple[datetime, float]]]:
        end = datetime.now()
        start = end - timedelta(minutes=window_minutes)
        queries = {
            "node_load_average_1m": f'node_load1{{instance="{node}"}}',
            "node_load_average_5m": f'node_load5{{instance="{node}"}}',
            "node_load_average_15m": f'node_load15{{instance="{node}"}}',
        }
        metrics: dict[str, list[tuple[datetime, float]]] = {}
        for name, query in queries.items():
            result = self.query_range(query, start, end, step=60)
            if not result:
                metrics[name] = []
                continue
            values = result[0].get("values", [])
            metrics[name] = [
                (datetime.fromtimestamp(ts), float(val)) for ts, val in values
            ]
        return metrics

    async def get_node_level_feature_matrix(
        self,
        node: str,
        window_minutes: int = 60,
    ) -> dict[str, list[tuple[datetime, float]]]:
        end = datetime.now()
        start = end - timedelta(minutes=window_minutes)
        queries = {
            "node_load1": f'node_load1{{instance="{node}"}}',
            "node_load5": f'node_load5{{instance="{node}"}}',
            "node_load15": f'node_load15{{instance="{node}"}}',
            "node_memory_MemAvailable_bytes": f'node_memory_MemAvailable_bytes{{instance="{node}"}}',
            "node_disk_read_bytes_total": f'rate(node_disk_read_bytes_total{{instance="{node}"}}[5m])',
            "node_network_transmit_bytes_total": f'rate(node_network_transmit_bytes_total{{instance="{node}"}}[5m])',
        }

        metrics: dict[str, list[tuple[datetime, float]]] = {}
        for name, query in queries.items():
            result = self.query_range(query, start, end, step=60)
            if not result:
                metrics[name] = []
                continue
            values = result[0].get("values", [])
            metrics[name] = [
                (datetime.fromtimestamp(ts, tz=timezone.utc), float(val))
                for ts, val in values
            ]
        return metrics

    async def get_k8s_control_plane_snapshot(
        self,
        namespace: str,
        pod_name: str,
        container_name: Optional[str] = None,
    ) -> dict[str, float]:
        selector = self._selector(namespace, pod_name, container_name)
        cpu_req = self.query(
            f'kube_pod_container_resource_requests{{resource="cpu", {selector}}}'
        )
        mem_req = self.query(
            f'kube_pod_container_resource_requests{{resource="memory", {selector}}}'
        )
        cpu_lim = self.query(
            f'kube_pod_container_resource_limits{{resource="cpu", {selector}}}'
        )
        mem_lim = self.query(
            f'kube_pod_container_resource_limits{{resource="memory", {selector}}}'
        )
        pod_phase = self.query(
            f'kube_pod_status_phase{{namespace="{namespace}", pod="{pod_name}"}}'
        )
        restarts = self.query(
            f'sum(kube_pod_container_status_restarts_total{{namespace="{namespace}", pod="{pod_name}"}})'
        )

        phase_value = 0.0
        for sample in pod_phase:
            val = float(sample.get("value", [0, 0])[1])
            if val == 1.0:
                phase = sample.get("metric", {}).get("phase", "Unknown")
                phase_map = {
                    "Pending": 0.0,
                    "Running": 1.0,
                    "Succeeded": 2.0,
                    "Failed": 3.0,
                    "Unknown": 4.0,
                }
                phase_value = phase_map.get(phase, 4.0)
                break

        return {
            "kube_pod_container_resource_requests_cpu": float(
                cpu_req[0].get("value", [0, 0])[1]
            )
            if cpu_req
            else 0.0,
            "kube_pod_container_resource_requests_memory": float(
                mem_req[0].get("value", [0, 0])[1]
            )
            if mem_req
            else 0.0,
            "kube_pod_container_resource_limits_cpu": float(
                cpu_lim[0].get("value", [0, 0])[1]
            )
            if cpu_lim
            else 0.0,
            "kube_pod_container_resource_limits_memory": float(
                mem_lim[0].get("value", [0, 0])[1]
            )
            if mem_lim
            else 0.0,
            "kube_pod_status_phase": phase_value,
            "kube_pod_container_status_restarts_total": float(
                restarts[0].get("value", [0, 0])[1]
            )
            if restarts
            else 0.0,
        }

    async def get_node_cpu_capacity_cores(self, node: str) -> float:
        query = f'count(count(node_cpu_seconds_total{{instance="{node}"}}) by (cpu))'
        result = self.query(query)
        if not result:
            return 1.0
        return max(float(result[0].get("value", [0, 1])[1]), 1.0)

    async def get_pods_per_node(self, node: str) -> float:
        query = f'count(kube_pod_info{{node="{node}"}})'
        result = self.query(query)
        if not result:
            return 1.0
        return max(float(result[0].get("value", [0, 1])[1]), 1.0)

    async def get_prediction_errors(
        self,
        window_hours: float = 1.0,
    ) -> float:
        """
        Get current MAE of p50 forecast vs actual
        """
        query = f"avg_over_time(abs(pipeline_prediction_cpu_p50 - pipeline:container_cpu_usage_rate:5m)[{window_hours}h])"
        result = self.query(query)

        if result and result[0].get("value"):
            return float(result[0]["value"][1])
        return 0.0

    async def get_baseline_error_rate(
        self,
        window_hours: float = 24.0,
    ) -> float:
        """
        Get baseline MAE (24h rolling average)
        """
        query = f"avg_over_time(abs(pipeline_prediction_cpu_p50 - pipeline:container_cpu_usage_rate:5m)[{window_hours}h])"
        result = self.query(query)

        if result and result[0].get("value"):
            return float(result[0]["value"][1])
        return 0.0
