# CeleRS Deployment Guide

This guide covers deploying CeleRS in production environments using Docker, Kubernetes, and cloud platforms.

## Table of Contents

- [Quick Start](#quick-start)
- [Docker Deployment](#docker-deployment)
- [Kubernetes Deployment](#kubernetes-deployment)
- [Cloud Platforms](#cloud-platforms)
- [Monitoring & Observability](#monitoring--observability)
- [Performance Tuning](#performance-tuning)
- [Security Best Practices](#security-best-practices)

## Quick Start

### Prerequisites

- Docker 24.0+ or Kubernetes 1.28+
- Redis 7.0+ or PostgreSQL 16+ or RabbitMQ 3.12+
- 2GB+ RAM per worker instance
- Network connectivity between workers and brokers

### Local Development

Use docker-compose for local development:

```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f worker

# Scale workers
docker-compose up -d --scale worker=4

# Stop all services
docker-compose down
```

## Docker Deployment

### Single Worker Instance

```bash
# Pull the latest image
docker pull cooljapan/celers:latest

# Run worker
docker run -d \
  --name celers-worker \
  -e RUST_LOG=info \
  -e REDIS_URL=redis://redis:6379 \
  cooljapan/celers:latest \
  worker start --concurrency 8
```

### Multi-Worker with Docker Compose

Create `docker-compose.prod.yml`:

```yaml
version: '3.8'

services:
  redis:
    image: redis:7-alpine
    volumes:
      - redis-data:/data
    command: redis-server --appendonly yes
    deploy:
      resources:
        limits:
          memory: 512M

  worker-high-priority:
    image: cooljapan/celers:latest
    environment:
      RUST_LOG: info
      REDIS_URL: redis://redis:6379
    command: worker start --concurrency 16 --queue high_priority
    deploy:
      replicas: 2
      resources:
        limits:
          memory: 2G
          cpus: '2'

  worker-normal:
    image: cooljapan/celers:latest
    environment:
      RUST_LOG: info
      REDIS_URL: redis://redis:6379
    command: worker start --concurrency 8 --queue celery
    deploy:
      replicas: 4
      resources:
        limits:
          memory: 1G
          cpus: '1'

volumes:
  redis-data:
```

Deploy:

```bash
docker-compose -f docker-compose.prod.yml up -d
```

## Kubernetes Deployment

### Redis StatefulSet

Create `k8s/redis-statefulset.yaml`:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: redis
spec:
  ports:
    - port: 6379
      targetPort: 6379
  clusterIP: None
  selector:
    app: redis
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: redis
spec:
  serviceName: redis
  replicas: 1
  selector:
    matchLabels:
      app: redis
  template:
    metadata:
      labels:
        app: redis
    spec:
      containers:
        - name: redis
          image: redis:7-alpine
          ports:
            - containerPort: 6379
          volumeMounts:
            - name: redis-data
              mountPath: /data
          resources:
            requests:
              memory: "256Mi"
              cpu: "250m"
            limits:
              memory: "512Mi"
              cpu: "500m"
  volumeClaimTemplates:
    - metadata:
        name: redis-data
      spec:
        accessModes: [ "ReadWriteOnce" ]
        resources:
          requests:
            storage: 10Gi
```

### Worker Deployment

Create `k8s/worker-deployment.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: celers-worker
  labels:
    app: celers-worker
spec:
  replicas: 4
  selector:
    matchLabels:
      app: celers-worker
  template:
    metadata:
      labels:
        app: celers-worker
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9090"
        prometheus.io/path: "/metrics"
    spec:
      containers:
        - name: worker
          image: cooljapan/celers:latest
          command: ["celers", "worker", "start"]
          args:
            - "--concurrency=8"
            - "--max-retries=3"
            - "--enable-metrics"
          env:
            - name: RUST_LOG
              value: "info"
            - name: REDIS_URL
              valueFrom:
                secretKeyRef:
                  name: celers-secrets
                  key: redis-url
          resources:
            requests:
              memory: "1Gi"
              cpu: "1000m"
            limits:
              memory: "2Gi"
              cpu: "2000m"
          livenessProbe:
            httpGet:
              path: /health
              port: 9090
            initialDelaySeconds: 30
            periodSeconds: 10
          readinessProbe:
            httpGet:
              path: /ready
              port: 9090
            initialDelaySeconds: 10
            periodSeconds: 5
---
apiVersion: v1
kind: Secret
metadata:
  name: celers-secrets
type: Opaque
stringData:
  redis-url: "redis://redis:6379"
```

### Horizontal Pod Autoscaler

Create `k8s/hpa.yaml`:

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: celers-worker-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: celers-worker
  minReplicas: 2
  maxReplicas: 20
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
    - type: Resource
      resource:
        name: memory
        target:
          type: Utilization
          averageUtilization: 80
    - type: Pods
      pods:
        metric:
          name: queue_size
        target:
          type: AverageValue
          averageValue: "100"
```

Deploy to Kubernetes:

```bash
# Create namespace
kubectl create namespace celers

# Apply configurations
kubectl apply -f k8s/ -n celers

# Verify deployment
kubectl get pods -n celers
kubectl get svc -n celers

# Scale workers
kubectl scale deployment celers-worker --replicas=10 -n celers
```

## Cloud Platforms

### AWS ECS

Use AWS ECS with Fargate for serverless deployment:

```json
{
  "family": "celers-worker",
  "networkMode": "awsvpc",
  "requiresCompatibilities": ["FARGATE"],
  "cpu": "2048",
  "memory": "4096",
  "containerDefinitions": [
    {
      "name": "worker",
      "image": "cooljapan/celers:latest",
      "command": ["celers", "worker", "start"],
      "environment": [
        {"name": "RUST_LOG", "value": "info"},
        {"name": "REDIS_URL", "value": "redis://cache.xxxxx.0001.use1.cache.amazonaws.com:6379"}
      ],
      "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "/ecs/celers-worker",
          "awslogs-region": "us-east-1",
          "awslogs-stream-prefix": "worker"
        }
      }
    }
  ]
}
```

### Google Cloud Run

Deploy to Cloud Run (note: requires HTTP endpoint for health checks):

```bash
gcloud run deploy celers-worker \
  --image gcr.io/PROJECT_ID/celers:latest \
  --platform managed \
  --region us-central1 \
  --memory 2Gi \
  --cpu 2 \
  --min-instances 2 \
  --max-instances 10 \
  --set-env-vars REDIS_URL=redis://REDIS_IP:6379
```

### Azure Container Instances

```bash
az container create \
  --resource-group celers-rg \
  --name celers-worker \
  --image cooljapan/celers:latest \
  --cpu 2 \
  --memory 4 \
  --environment-variables \
    RUST_LOG=info \
    REDIS_URL=redis://cache.redis.cache.windows.net:6380 \
  --restart-policy Always
```

## Monitoring & Observability

### Prometheus Integration

Configure Prometheus scraping in `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'celers-workers'
    static_configs:
      - targets: ['worker:9090']
    metrics_path: '/metrics'
    scrape_interval: 15s
```

### Grafana Dashboards

Import pre-built dashboards:
- Queue metrics: `docs/grafana/queue-dashboard.json`
- Worker metrics: `docs/grafana/worker-dashboard.json`
- Performance metrics: `docs/grafana/performance-dashboard.json`

### Logging

Configure structured logging:

```bash
# JSON logging for production
RUST_LOG=info,celers=debug RUST_LOG_FORMAT=json celers worker start

# Send logs to CloudWatch/Stackdriver
celers worker start 2>&1 | /opt/aws/cloudwatch/bin/cloudwatch-logs-agent
```

## Performance Tuning

### Worker Configuration

Optimize for throughput:

```bash
celers worker start \
  --concurrency 16 \
  --enable-batch-dequeue \
  --batch-size 20 \
  --poll-interval 100
```

Optimize for latency:

```bash
celers worker start \
  --concurrency 8 \
  --poll-interval 50 \
  --max-retries 5
```

### Redis Tuning

Configure Redis for high throughput:

```bash
# redis.conf
maxmemory 4gb
maxmemory-policy allkeys-lru
save 900 1
save 300 10
save 60 10000
appendonly yes
appendfsync everysec
```

### PostgreSQL Tuning

Optimize PostgreSQL for queue operations:

```sql
-- Increase connection pool
ALTER SYSTEM SET max_connections = 200;

-- Tune for writes
ALTER SYSTEM SET shared_buffers = '2GB';
ALTER SYSTEM SET effective_cache_size = '6GB';
ALTER SYSTEM SET maintenance_work_mem = '512MB';
ALTER SYSTEM SET checkpoint_completion_target = 0.9;
```

## Security Best Practices

### Network Security

1. **Use VPCs**: Isolate workers and brokers in private subnets
2. **Enable TLS**: Use `rediss://` and SSL connections
3. **Firewall rules**: Restrict access to broker ports

### Authentication

Configure Redis authentication:

```bash
# redis.conf
requirepass your_strong_password

# Worker configuration
REDIS_URL=redis://:your_strong_password@redis:6379
```

### Secrets Management

Use Kubernetes secrets or cloud secret managers:

```bash
# AWS Secrets Manager
aws secretsmanager create-secret \
  --name celers/redis-url \
  --secret-string "redis://password@redis:6379"

# Reference in deployment
kubectl create secret generic celers-secrets \
  --from-literal=redis-url="$(aws secretsmanager get-secret-value --secret-id celers/redis-url --query SecretString --output text)"
```

### Resource Limits

Set appropriate limits to prevent resource exhaustion:

```yaml
resources:
  requests:
    memory: "1Gi"
    cpu: "1000m"
  limits:
    memory: "2Gi"
    cpu: "2000m"
```

## Troubleshooting

### Common Issues

**Workers not processing tasks:**
- Check Redis connectivity: `redis-cli ping`
- Verify queue has tasks: `celers queue status`
- Check worker logs: `docker logs celers-worker`

**High memory usage:**
- Enable batch processing: `--enable-batch-dequeue`
- Reduce concurrency: `--concurrency 4`
- Set max result size: `--max-result-size 1048576`

**Slow task processing:**
- Increase concurrency: `--concurrency 16`
- Enable circuit breaker: `--enable-circuit-breaker`
- Check broker latency in metrics

For more help, see the [Performance Guide](../PERFORMANCE.md) or [open an issue](https://github.com/cool-japan/celers/issues).
