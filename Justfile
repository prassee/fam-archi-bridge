# Justfile for managing WAL Writer services
# Aligned with docker-compose.yml services
# ---------------------------------------------------------------------------
# Quick Start Targets
# ---------------------------------------------------------------------------

# Start all services (complete stack)
start-all:
    docker-compose up -d
    @echo "All services started"
    @echo "Grafana: http://localhost:3000 (admin/admin)"
    @echo "Prometheus: http://localhost:9091"
    @echo "WAL Writer Rust metrics: http://localhost:9095/metrics"
    @echo "Kafka: localhost:9092"

# Stop all services
stop-all:
    docker-compose stop
    @echo "All services stopped"

# Start CDC core stack only (postgres + kafka + data-pump-go + wal-writer-rust + wal-consumer)
start-cdc:
    docker-compose up -d postgres
    @echo "Waiting for PostgreSQL to be healthy..."
    @timeout 60 bash -c 'until docker-compose exec -T postgres pg_isready -U postgres > /dev/null 2>&1; do sleep 2; done' || true
    docker-compose up -d db-init data-pump-go kafka
    @echo "Waiting for Kafka to be healthy..."
    @timeout 60 bash -c 'until docker-compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; do sleep 2; done' || true
    docker-compose up -d wal-writer-rust wal-consumer
    @echo "CDC stack started"
    @echo "  PostgreSQL: localhost:5432"
    @echo "  Kafka: localhost:9092"
    @echo "  WAL Writer Rust metrics: http://localhost:9095/metrics"

# Start CDC stack in strict ordered startup for maximum stability
start-stable:
    docker-compose up -d postgres
    @echo "Waiting for PostgreSQL to be healthy..."
    @timeout 60 bash -c 'until docker-compose exec -T postgres pg_isready -U postgres > /dev/null 2>&1; do sleep 2; done' || true
    docker-compose up -d kafka
    @echo "Waiting for Kafka to be healthy..."
    @timeout 60 bash -c 'until docker-compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; do sleep 2; done' || true
    docker-compose up db-init
    docker-compose up -d wal-writer-rust
    @echo "Waiting for WAL Writer Rust to be healthy..."
    @timeout 60 bash -c 'until docker-compose exec -T wal-writer-rust curl -s http://localhost:9095/health > /dev/null 2>&1; do sleep 2; done' || true
    docker-compose up -d data-pump-go wal-consumer
    @echo "Stable CDC stack started"
    @echo "  PostgreSQL: localhost:5432"
    @echo "  Kafka: localhost:9092"
    @echo "  WAL Writer Rust metrics: http://localhost:9095/metrics"

# Stop CDC core stack
stop-cdc:
    docker-compose stop wal-writer-rust wal-consumer data-pump-go kafka db-init postgres
    @echo "CDC stack stopped"

# Start monitoring only (prometheus + grafana + exporters)
start-monitoring:
    docker-compose up -d postgres-exporter kafka-exporter prometheus grafana
    @echo "Monitoring services started"
    @echo "  Grafana: http://localhost:3000 (admin/admin)"
    @echo "  Prometheus: http://localhost:9091"
    @echo "  Kafka Exporter: http://localhost:9308/metrics"
    @echo "  PostgreSQL Exporter: http://localhost:9187/metrics"

# Start dashboards and exporters only
start-dashboards:
    docker-compose up -d prometheus grafana postgres-exporter kafka-exporter
    @echo "Dashboards and exporters started"
    @echo "  Grafana: http://localhost:3000 (admin/admin)"
    @echo "  Prometheus: http://localhost:9091"
    @echo "  Kafka Exporter: http://localhost:9308/metrics"
    @echo "  PostgreSQL Exporter: http://localhost:9187/metrics"
    @echo "  WAL Writer Rust metrics (scraped via Prometheus)"

# Stop monitoring
stop-monitoring:
    docker-compose stop postgres-exporter kafka-exporter prometheus grafana
    @echo "Monitoring services stopped"

# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Individual Service Targets
# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# PostgreSQL Database
start-postgres:
    docker-compose up -d postgres
    @echo "Waiting for PostgreSQL to be healthy..."
    @timeout 60 bash -c 'until docker-compose exec -T postgres pg_isready -U postgres > /dev/null 2>&1; do sleep 2; done' || true
    @echo "PostgreSQL started on localhost:5432"

stop-postgres:
    docker-compose stop postgres
    @echo "PostgreSQL stopped"

# Database Initialization (creates replication slot + publication)
start-db-init:
    docker-compose up db-init
    @echo "Database initialization completed"

# Kafka Broker
start-kafka:
    docker-compose up -d kafka
    @echo "Waiting for Kafka to be healthy..."
    @timeout 60 bash -c 'until docker-compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; do sleep 2; done' || true
    @echo "Kafka started on localhost:9092"

stop-kafka:
    docker-compose stop kafka
    @echo "Kafka stopped"

# Data Pump Go (generates UPI transactions)
start-data-pump:
    docker-compose up -d data-pump-go
    @echo "Data Pump started (generating ~10k txns/sec)"

stop-data-pump:
    docker-compose stop data-pump-go
    @echo "Data Pump stopped"

# WAL Writer Rust (active CDC agent)
start-wal-writer:
    docker-compose up -d wal-writer-rust
    @echo "Starting WAL Writer Rust..."
    @timeout 30 bash -c 'until docker-compose exec -T wal-writer-rust curl -s http://localhost:9095/health > /dev/null 2>&1; do sleep 2; done' || true
    @echo "WAL Writer Rust started"
    @echo "  Metrics: http://localhost:9095/metrics"
    @echo "  Health: http://localhost:9095/health"

stop-wal-writer:
    docker-compose stop wal-writer-rust
    @echo "WAL Writer Rust stopped"

# Recompile, rebuild image, and redeploy WAL Writer Rust
redeploy-wal-writer:
    @echo "Recompiling and rebuilding WAL Writer Rust image..."
    docker-compose build --no-cache wal-writer-rust
    @echo "Re-deploying WAL Writer Rust container..."
    docker-compose up -d --force-recreate wal-writer-rust
    @echo "Waiting for WAL Writer Rust to be healthy..."
    @timeout 60 bash -c 'until docker-compose exec -T wal-writer-rust curl -s http://localhost:9095/health > /dev/null 2>&1; do sleep 2; done' || true
    @echo "WAL Writer Rust recompiled, rebuilt, and redeployed"
    @echo "  Metrics: http://localhost:9095/metrics"
    @echo "  Health: http://localhost:9095/health"

# WAL Consumer (Rust consumer with parallel workers)
start-wal-consumer:
    docker-compose up -d wal-consumer
    @echo "WAL Consumer started"
    @echo "  Workers: 8 (partition-based routing)"
    @echo "  Commit threshold: 5000 messages"

stop-wal-consumer:
    docker-compose stop wal-consumer
    @echo "WAL Consumer stopped"

# Prometheus (metrics collection)
start-prometheus:
    docker-compose up -d prometheus
    @echo "Prometheus started on http://localhost:9091"

stop-prometheus:
    docker-compose stop prometheus
    @echo "Prometheus stopped"

# Grafana (metrics visualization)
start-grafana:
    docker-compose up -d grafana
    @echo "Grafana started on http://localhost:3000 (admin/admin)"

stop-grafana:
    docker-compose stop grafana
    @echo "Grafana stopped"

# PostgreSQL Exporter (metrics from PostgreSQL)
start-postgres-exporter:
    docker-compose up -d postgres-exporter
    @echo "PostgreSQL Exporter started on http://localhost:9187/metrics"

stop-postgres-exporter:
    docker-compose stop postgres-exporter
    @echo "PostgreSQL Exporter stopped"

# Kafka Exporter (metrics from Kafka)
start-kafka-exporter:
    docker-compose up -d kafka-exporter
    @echo "Kafka Exporter started on http://localhost:9308/metrics"

stop-kafka-exporter:
    docker-compose stop kafka-exporter
    @echo "Kafka Exporter stopped"

# ---------------------------------------------------------------------------
# Utility Targets
# ---------------------------------------------------------------------------

# View live logs from all running services
logs:
    docker-compose logs -f

# View logs from specific service (usage: just logs-service wal-writer)
logs-service service:
    docker-compose logs -f {{ service }}

# View status of all services
status:
    docker-compose ps

# Restart all services
restart:
    @just stop-all
    @just start-all

# Restart CDC stack
restart-cdc:
    @just stop-cdc
    @just start-cdc

# Clean up all containers and volumes
clean:
    docker-compose down -v
    @echo "All containers and volumes removed"
    @echo "Note: This includes rust writer rollback service if present."

# Clean up containers only (keep volumes)
clean-containers:
    docker-compose down
    @echo "All containers removed"

# Show configuration for a service (usage: just config wal-writer)
config service:
    docker-compose config --services | grep {{ service }}
    @echo "Service: {{ service }}"
    docker-compose ps {{ service }} || echo "Service not running"

# ---------------------------------------------------------------------------
# Kubernetes Targets
# ---------------------------------------------------------------------------

# Deploy full stack to Kubernetes via Kustomize
k8s-deploy:
    kubectl apply -k k8s/
    @echo "Full stack deployed to Kubernetes (namespace: cdc)"

# Tear down all Kubernetes resources (namespace included)
k8s-destroy:
    kubectl delete -k k8s/ --ignore-not-found
    @echo "Kubernetes resources removed"

# Show status of all pods in cdc namespace
k8s-status:
    kubectl get pods,svc,pvc -n cdc

# Deploy infrastructure only (postgres + kafka + db-init)
k8s-deploy-infra:
    kubectl apply -f k8s/namespace.yaml
    kubectl apply -f k8s/rbac.yaml
    kubectl apply -f k8s/configmap.yaml
    kubectl apply -f k8s/postgres.yaml
    kubectl apply -f k8s/kafka.yaml
    kubectl apply -f k8s/db-init.yaml
    @echo "Infra deployed (postgres, kafka, db-init)"

# Deploy WAL Writer only
k8s-deploy-wal-writer:
    kubectl apply -f k8s/deployment.yaml
    @echo "WAL Writer deployed"
    kubectl rollout status deployment/wal-writer -n cdc

# Deploy data pump only
k8s-deploy-data-pump:
    kubectl apply -f k8s/data-pump-go.yaml
    @echo "Data Pump deployed"

# Restart WAL Writer deployment (picks up new image)
k8s-restart-wal-writer:
    kubectl rollout restart deployment/wal-writer -n cdc
    kubectl rollout status deployment/wal-writer -n cdc
    @echo "WAL Writer restarted"

# View live logs from WAL Writer pods
k8s-logs-wal-writer:
    kubectl logs -n cdc -l app=wal-writer -f --tail=100

# View live logs from data-pump pods
k8s-logs-data-pump:
    kubectl logs -n cdc -l app=data-pump-go -f --tail=100

# Run db-init Job again (re-apply manifest; delete existing job first if needed)
k8s-run-db-init:
    kubectl delete job db-init -n cdc --ignore-not-found
    kubectl apply -f k8s/db-init.yaml
    kubectl wait --for=condition=complete job/db-init -n cdc --timeout=120s
    @echo "db-init job completed"

# Deploy monitoring stack (prometheus + grafana + exporters)
k8s-deploy-monitoring:
    kubectl apply -f k8s/monitoring.yaml
    @echo "Monitoring stack deployed (prometheus, grafana, postgres-exporter, kafka-exporter)"

# Port-forward Prometheus to localhost:9091 and open in browser
k8s-open-prometheus:
    @echo "Port-forwarding Prometheus -> http://localhost:9091"
    @kubectl port-forward -n cdc svc/prometheus 9091:9090 &
    @sleep 2 && open http://localhost:9091 || xdg-open http://localhost:9091

# Port-forward Grafana to localhost:3000 and open in browser (admin/admin)
k8s-open-grafana:
    @echo "Port-forwarding Grafana -> http://localhost:3000 (admin/admin)"
    @kubectl port-forward -n cdc svc/grafana 3000:3000 &
    @sleep 2 && open http://localhost:3000 || xdg-open http://localhost:3000

# Stop all active port-forwards (prometheus + grafana + minio + polaris)
k8s-stop-portforwards:
    @pkill -f 'kubectl port-forward.*prometheus' || true
    @pkill -f 'kubectl port-forward.*grafana' || true
    @pkill -f 'kubectl port-forward.*minio' || true
    @pkill -f 'kubectl port-forward.*polaris' || true
    @echo "Port-forwards stopped"

# Deploy MinIO + Polaris (Iceberg REST catalog) storage stack
k8s-deploy-storage:
    kubectl apply -f k8s/storage.yaml
    @echo "MinIO storage stack deployed (minio, minio-init job)"

# Deploy Polaris REST catalog only
k8s-deploy-polaris:
    kubectl apply -f k8s/polaris.yaml
    @echo "Polaris REST catalog deployed"

# Re-run polaris-init catalog bootstrap job
k8s-run-polaris-init:
    kubectl delete job polaris-init -n cdc --ignore-not-found
    kubectl apply -f k8s/polaris.yaml
    kubectl wait --for=condition=complete job/polaris-init -n cdc --timeout=180s
    @echo "polaris-init job completed"

# Port-forward MinIO console to localhost:9001 and open in browser (minioadmin/minioadmin)
k8s-open-minio:
    @echo "Port-forwarding MinIO console -> http://localhost:9001 (minioadmin/minioadmin)"
    @kubectl port-forward -n cdc svc/minio 9001:9001 &
    @sleep 2 && open http://localhost:9001 || xdg-open http://localhost:9001

# Port-forward Polaris catalog API to localhost:8181
k8s-open-polaris:
    @echo "Port-forwarding Polaris catalog -> http://localhost:8181/api/catalog"
    @kubectl port-forward -n cdc svc/polaris 8181:8181 8182:8182 &
    @sleep 2 && open http://localhost:8182/q/health || xdg-open http://localhost:8182/q/health

# Re-run minio-init bucket creation job
k8s-run-minio-init:
    kubectl delete job minio-init -n cdc --ignore-not-found
    kubectl apply -f k8s/storage.yaml
    kubectl wait --for=condition=complete job/minio-init -n cdc --timeout=120s
    @echo "minio-init job completed"

# ---------------------------------------------------------------------------
# Utility Targets
# ---------------------------------------------------------------------------

# Test connectivity to key endpoints
test:
    @echo "Testing PostgreSQL..."
    @docker-compose exec -T postgres pg_isready -U postgres || echo "PostgreSQL not responding"
    @echo "PostgreSQL responding"
    @echo "Testing Kafka..."
    @docker-compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null || echo "Kafka not responding"
    @echo "Kafka responding"
    @echo "Testing WAL Writer Rust..."
    @curl -s http://localhost:9095/health > /dev/null || echo "WAL Writer Rust not responding"
    @echo "WAL Writer Rust responding"
    @echo "All systems operational"
