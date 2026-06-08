#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== Pipeline V2 Test Cluster Setup ===${NC}"
echo ""

# Check prerequisites
echo -e "${YELLOW}Step 1: Checking prerequisites...${NC}"

# Check Docker
if ! command -v docker &> /dev/null; then
    echo -e "${RED}✗ Docker is not installed. Please install Docker first.${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Docker is installed${NC}"

# Check kind
if ! command -v kind &> /dev/null; then
    echo -e "${YELLOW}Installing kind...${NC}"
    curl -Lo ./kind https://kind.sigs.k8s.io/dl/v0.20.0/kind-linux-amd64
    chmod +x ./kind
    sudo mv ./kind /usr/local/bin/kind
    echo -e "${GREEN}✓ kind installed${NC}"
else
    echo -e "${GREEN}✓ kind is installed${NC}"
fi

# Check kubectl
if ! command -v kubectl &> /dev/null; then
    echo -e "${YELLOW}Installing kubectl...${NC}"
    curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
    chmod +x kubectl
    sudo mv ./kubectl /usr/local/bin/kubectl
    echo -e "${GREEN}✓ kubectl installed${NC}"
else
    echo -e "${GREEN}✓ kubectl is installed${NC}"
fi

# Check helm
if ! command -v helm &> /dev/null; then
    echo -e "${YELLOW}Installing helm...${NC}"
    curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
    echo -e "${GREEN}✓ helm installed${NC}"
else
    echo -e "${GREEN}✓ helm is installed${NC}"
fi

echo ""
echo -e "${YELLOW}Step 2: Creating kind cluster...${NC}"

# Delete existing cluster if exists
if kind get clusters 2>/dev/null | grep -q "pipeline-test"; then
    echo -e "${YELLOW}Deleting existing pipeline-test cluster...${NC}"
    kind delete cluster --name pipeline-test
fi

# Create kind cluster with extra port mappings for Prometheus
cat <<EOF | kind create cluster --name pipeline-test --config=-
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    kubeadmConfigPatches:
      - |
        kind: InitConfiguration
        nodeRegistration:
          kubeletExtraArgs:
            node-labels: "ingress-ready=true"
    extraPortMappings:
      - containerPort: 30000
        hostPort: 30000
        protocol: TCP
      - containerPort: 30001
        hostPort: 30001
        protocol: TCP
      - containerPort: 30002
        hostPort: 30002
        protocol: TCP
  - role: worker
  - role: worker
EOF

echo -e "${GREEN}✓ kind cluster 'pipeline-test' created${NC}"

echo ""
echo -e "${YELLOW}Step 3: Installing kube-prometheus-stack...${NC}"

# Add Prometheus community helm repo
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts 2>/dev/null || true
helm repo update

# Create monitoring namespace
kubectl create namespace monitoring --dry-run=client -o yaml | kubectl apply -f -

# Install kube-prometheus-stack with custom values
helm upgrade --install monitoring prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  --set kubeStateMetrics.enabled=true \
  --set nodeExporter.enabled=true \
  --set prometheus.prometheusSpec.retention=7d \
  --set prometheus.prometheusSpec.scrapeInterval=30s \
  --set prometheus.prometheusSpec.serviceMonitorSelectorNilUsesHelmValues=false \
  --set prometheus.prometheusSpec.podMonitorSelectorNilUsesHelmValues=false \
  --set prometheus.service.type=NodePort \
  --set prometheus.service.nodePort=30000 \
  --set grafana.service.type=NodePort \
  --set grafana.service.nodePort=30001 \
  --set alertmanager.service.type=NodePort \
  --set alertmanager.service.nodePort=30002 \
  --wait \
  --timeout 10m

echo -e "${GREEN}✓ kube-prometheus-stack installed${NC}"

echo ""
echo -e "${YELLOW}Step 4: Deploying pipeline recording rules...${NC}"

kubectl apply -f - <<EOF
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: pipeline-recording-rules
  namespace: monitoring
  labels:
    prometheus: monitoring
    role: alert-rules
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
EOF

echo -e "${GREEN}✓ Recording rules deployed${NC}"

echo ""
echo -e "${YELLOW}Step 5: Deploying sample workload to generate metrics...${NC}"

# Create test namespace
kubectl create namespace test-workload --dry-run=client -o yaml | kubectl apply -f -

# Deploy a sample application that generates variable CPU/memory load
kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: stress-test-app
  namespace: test-workload
  labels:
    app: stress-test
spec:
  replicas: 1
  selector:
    matchLabels:
      app: stress-test
  template:
    metadata:
      labels:
        app: stress-test
    spec:
      containers:
      - name: stress-container
        image: polinux/stress:latest
        command: ["sh", "-c"]
        args:
          - |
            while true; do
              # Run CPU and Memory stress concurrently for 6 minutes
              stress --cpu 3 --vm 1 --vm-bytes 420M --timeout 360s || true
              # Rest for 15 seconds
              sleep 15
            done
        resources:
          requests:
            cpu: "0.5"
            memory: "256Mi"
          limits:
            cpu: "2.0"
            memory: "512Mi"
        securityContext:
          allowPrivilegeEscalation: false
          capabilities:
            drop:
              - ALL
---
apiVersion: v1
kind: Service
metadata:
  name: stress-test-service
  namespace: test-workload
spec:
  selector:
    app: stress-test
  ports:
    - protocol: TCP
      port: 80
      targetPort: 80
EOF

echo -e "${GREEN}✓ Sample workload deployed${NC}"

echo ""
echo -e "${YELLOW}Step 6: Waiting for metrics to be collected (60 seconds)...${NC}"
sleep 60

echo ""
echo -e "${YELLOW}Step 7: Verifying metrics availability...${NC}"

# Check if Prometheus is running
if kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=prometheus -n monitoring --timeout=120s; then
    echo -e "${GREEN}✓ Prometheus is ready${NC}"
else
    echo -e "${RED}✗ Prometheus is not ready${NC}"
    exit 1
fi

# Verify recording rules are working
echo ""
echo -e "${YELLOW}Checking recording rules...${NC}"

# Give it a moment for data to accumulate
sleep 10

# Check for pipeline:* metrics
if kubectl exec -n monitoring svc/monitoring-kube-prometheus-prometheus -- \
    promtool query instant 'pipeline:container_cpu_usage_rate:5m' 2>/dev/null | grep -q "container_cpu_usage_rate"; then
    echo -e "${GREEN}✓ Recording rules are working${NC}"
else
    echo -e "${YELLOW}⚠ Recording rules may need more time to populate (wait 5-10 minutes)${NC}"
fi

echo ""
echo -e "${GREEN}=== Setup Complete! ===${NC}"
echo ""
echo -e "${YELLOW}Access URLs:${NC}"
echo -e "  Prometheus:  ${GREEN}http://localhost:30000${NC}"
echo -e "  Grafana:     ${GREEN}http://localhost:30001${NC}"
echo -e "  Alertmanager:${GREEN}http://localhost:30002${NC}"
echo ""
echo -e "${YELLOW}Grafana credentials:${NC}"
echo -e "  Username: ${GREEN}admin${NC}"
echo -e "  Password: ${GREEN}$(kubectl get secret -n monitoring monitoring-grafana -o jsonpath="{.data.admin-password}" | base64 --decode)${NC}"
echo ""
echo -e "${YELLOW}Next steps:${NC}"
echo "  1. Start local infrastructure: docker-compose -f docker-compose.dev.yaml up -d"
echo "  2. Set environment variables (see configs/.env.example)"
echo "  3. Run the agentic pipeline: pipeline-agentic --pod <pod-name> --namespace test-workload"
echo ""
echo -e "${YELLOW}To verify metrics manually:${NC}"
echo "  kubectl -n monitoring port-forward svc/monitoring-kube-prometheus-prometheus 9090:9090"
echo "  Then visit: http://localhost:9090/graph"
echo ""
echo -e "${YELLOW}Sample queries to try in Prometheus:${NC}"
echo "  - pipeline:container_cpu_usage_rate:5m"
echo "  - pipeline:container_memory_usage_bytes"
echo "  - pipeline:node_cpu_usage_rate:5m"
echo ""
