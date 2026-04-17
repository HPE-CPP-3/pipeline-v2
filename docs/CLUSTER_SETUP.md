# Kubernetes Cluster Setup for Metrics Logging

This guide explains how to prepare a Kubernetes cluster so `pipeline-v2` can ingest metrics from Prometheus.

The pipeline **does not scrape Kubernetes directly**. It queries a Prometheus endpoint. Your cluster must expose the metrics and recording rules expected by the ingestion/training agents.

## 1) What the pipeline expects

`pipeline-v2` expects Prometheus to provide:

- cAdvisor-backed container metrics (CPU, memory, throttling, failcnt)
- node-exporter metrics (CPU modes, load, memory, disk, network)
- kube-state-metrics (pod lifecycle, requests/limits, scheduling metadata)
- recording rules with `pipeline:*` names used by the ingestion client

## 2) Install monitoring stack

Recommended: `kube-prometheus-stack`.

```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update

kubectl create namespace monitoring --dry-run=client -o yaml | kubectl apply -f -

helm upgrade --install monitoring prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  --set kubeStateMetrics.enabled=true \
  --set nodeExporter.enabled=true \
  --set prometheus.prometheusSpec.retention=7d \
  --set prometheus.prometheusSpec.scrapeInterval=30s
```

## 3) Verify exporters and targets

```bash
kubectl get pods -n monitoring
kubectl get servicemonitors -n monitoring
```

Open Prometheus (dev):

```bash
kubectl -n monitoring port-forward svc/monitoring-kube-prometheus-prometheus 9090:9090
```

Then confirm these return data in Prometheus UI (`/graph`):

- `container_cpu_usage_seconds_total`
- `container_cpu_cfs_throttled_periods_total`
- `container_memory_usage_bytes`
- `container_memory_working_set_bytes`
- `container_memory_failcnt`
- `node_cpu_seconds_total`
- `node_load1`
- `node_memory_MemAvailable_bytes`
- `node_disk_read_bytes_total`
- `node_network_receive_bytes_total`
- `kube_pod_status_phase`
- `kube_pod_container_resource_limits`

## 4) Add required recording rules

Apply this `PrometheusRule` in `monitoring` namespace.

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: pipeline-recording-rules
  namespace: monitoring
spec:
  groups:
    - name: pipeline.container
      interval: 30s
      rules:
        - record: pipeline:container_cpu_usage_rate:5m
          expr: |
            sum by (namespace, pod, container) (
              rate(container_cpu_usage_seconds_total{container!="",image!=""}[5m])
            )

        - record: pipeline:container_cpu_throttle_ratio:5m
          expr: |
            sum by (namespace, pod, container) (
              rate(container_cpu_cfs_throttled_periods_total{container!="",image!=""}[5m])
            )
            /
            clamp_min(
              sum by (namespace, pod, container) (
                rate(container_cpu_cfs_periods_total{container!="",image!=""}[5m])
              ),
              1
            )

        - record: pipeline:container_memory_usage_bytes
          expr: |
            max by (namespace, pod, container) (
              container_memory_usage_bytes{container!="",image!=""}
            )

        - record: pipeline:container_memory_working_set_bytes
          expr: |
            max by (namespace, pod, container) (
              container_memory_working_set_bytes{container!="",image!=""}
            )

        - record: pipeline:container_memory_cache_bytes
          expr: |
            max by (namespace, pod, container) (
              container_memory_cache{container!="",image!=""}
            )

        - record: pipeline:container_memory_failcnt:rate1h
          expr: |
            sum by (namespace, pod, container) (
              increase(container_memory_failcnt{container!="",image!=""}[1h])
            )

    - name: pipeline.node
      interval: 30s
      rules:
        - record: pipeline:node_cpu_usage_rate:5m
          expr: |
            1 - avg by (instance) (rate(node_cpu_seconds_total{mode="idle"}[5m]))

        - record: pipeline:node_load_ratio
          expr: |
            node_load1
            /
            clamp_min(count by (instance) (count by (instance, cpu) (node_cpu_seconds_total)), 1)

        - record: pipeline:node_memory_available_ratio
          expr: |
            node_memory_MemAvailable_bytes
            /
            clamp_min(node_memory_MemTotal_bytes, 1)

        - record: pipeline:node_disk_read_bytes:rate5m
          expr: |
            sum by (instance) (rate(node_disk_read_bytes_total[5m]))

        - record: pipeline:node_disk_written_bytes:rate5m
          expr: |
            sum by (instance) (rate(node_disk_written_bytes_total[5m]))

        - record: pipeline:node_disk_io_time:rate5m
          expr: |
            sum by (instance) (rate(node_disk_io_time_seconds_total[5m]))

        - record: pipeline:node_disk_io_queue_length
          expr: |
            sum by (instance) (node_disk_io_now)

        - record: pipeline:node_network_receive_bytes:rate5m
          expr: |
            sum by (instance) (rate(node_network_receive_bytes_total[5m]))

        - record: pipeline:node_network_transmit_bytes:rate5m
          expr: |
            sum by (instance) (rate(node_network_transmit_bytes_total[5m]))

        - record: pipeline:node_network_drop_ratio
          expr: |
            (
              sum by (instance) (rate(node_network_receive_drop_total[5m]))
              +
              sum by (instance) (rate(node_network_transmit_drop_total[5m]))
            )
            /
            clamp_min(
              (
                sum by (instance) (rate(node_network_receive_packets_total[5m]))
                +
                sum by (instance) (rate(node_network_transmit_packets_total[5m]))
              ),
              1
            )

        - record: pipeline:node_network_error_ratio
          expr: |
            (
              sum by (instance) (rate(node_network_receive_errs_total[5m]))
              +
              sum by (instance) (rate(node_network_transmit_errs_total[5m]))
            )
            /
            clamp_min(
              (
                sum by (instance) (rate(node_network_receive_packets_total[5m]))
                +
                sum by (instance) (rate(node_network_transmit_packets_total[5m]))
              ),
              1
            )

    - name: pipeline.k8s
      interval: 30s
      rules:
        - record: pipeline:pod_status_phase
          expr: |
            max by (namespace, pod, phase) (kube_pod_status_phase == 1)
```

Apply:

```bash
kubectl apply -f pipeline-recording-rules.yaml
```

## 5) Expose Prometheus to the VM running pipeline-v2

Set a reachable URL in your VM:

```bash
export PIPELINE_PROMETHEUS_URL="http://<prometheus-host>:9090"
```

Or put the same value in `configs/ingestion.yaml` under `prometheus.url`.

For production, prefer private networking (VPN/VPC peering/internal load balancer) and authentication.

## 6) Validate required pipeline series

In Prometheus, verify these queries return values:

- `pipeline:container_cpu_usage_rate:5m`
- `pipeline:container_cpu_throttle_ratio:5m`
- `pipeline:container_memory_usage_bytes`
- `pipeline:container_memory_working_set_bytes`
- `pipeline:container_memory_cache_bytes`
- `pipeline:container_memory_failcnt:rate1h`
- `pipeline:node_disk_read_bytes:rate5m`
- `pipeline:node_disk_written_bytes:rate5m`
- `pipeline:node_network_drop_ratio`
- `pipeline:pod_status_phase`

If these are present, agentic ingestion/training/prediction can run end-to-end.

## 7) Troubleshooting

- Missing container metrics: ensure kubelet/cAdvisor scraping is enabled.
- Missing kube metrics: ensure `kube-state-metrics` is running and scraped.
- Empty `pipeline:*` series: recording rules not loaded or wrong namespace.
- Node metrics missing: ensure node-exporter DaemonSet is healthy.
- Label mismatch (`instance`, `pod`, `namespace`): inspect your metric labels in Prometheus and adjust recording-rule expressions to match your environment.
