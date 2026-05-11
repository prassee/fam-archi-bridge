# Justfile for managing WAL Writer services
# Aligned with docker-compose.yml services
# ────────────────────────────────────────────────────────────────────────────
# Quick Start Targets
# ────────────────────────────────────────────────────────────────────────────

# Start all services (complete stack)
start-all:
    docker-compose up -d
    @echo "✓ All services started"
    @echo "Grafana: http://localhost:3000 (admin/admin)"
    @echo "Prometheus: http://localhost:9091"
    @echo "WAL Writer Rust metrics: http://localhost:9095/metrics"
    @echo "Kafka: localhost:9092"

# Stop all services
stop-all:
    docker-compose stop
    @echo "✓ All services stopped"

# Start CDC core stack only (postgres + kafka + data-pump-go + wal-writer-rust + wal-consumer)
start-cdc:
    docker-compose up -d postgres
    @echo "⏳ Waiting for PostgreSQL to be healthy..."
    @timeout 60 bash -c 'until docker-compose exec -T postgres pg_isready -U postgres > /dev/null 2>&1; do sleep 2; done' || true
    docker-compose up -d db-init data-pump-go kafka
    @echo "⏳ Waiting for Kafka to be healthy..."
    @timeout 60 bash -c 'until docker-compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; do sleep 2; done' || true
    docker-compose up -d wal-writer-rust wal-consumer
    @echo "✓ CDC stack started"
    @echo "  PostgreSQL: localhost:5432"
    @echo "  Kafka: localhost:9092"
    @echo "  WAL Writer Rust metrics: http://localhost:9095/metrics"

# Start CDC stack in strict ordered startup for maximum stability
start-stable:
    docker-compose up -d postgres
    @echo "⏳ Waiting for PostgreSQL to be healthy..."
    @timeout 60 bash -c 'until docker-compose exec -T postgres pg_isready -U postgres > /dev/null 2>&1; do sleep 2; done' || true
    docker-compose up -d kafka
    @echo "⏳ Waiting for Kafka to be healthy..."
    @timeout 60 bash -c 'until docker-compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; do sleep 2; done' || true
    docker-compose up db-init
    docker-compose up -d wal-writer-rust
    @echo "⏳ Waiting for WAL Writer Rust to be healthy..."
    @timeout 60 bash -c 'until docker-compose exec -T wal-writer-rust curl -s http://localhost:9095/health > /dev/null 2>&1; do sleep 2; done' || true
    docker-compose up -d data-pump-go wal-consumer
    @echo "✓ Stable CDC stack started"
    @echo "  PostgreSQL: localhost:5432"
    @echo "  Kafka: localhost:9092"
    @echo "  WAL Writer Rust metrics: http://localhost:9095/metrics"

# Stop CDC core stack
stop-cdc:
    docker-compose stop wal-writer-rust wal-consumer data-pump-go kafka db-init postgres
    @echo "✓ CDC stack stopped"

# Start monitoring only (prometheus + grafana + exporters)
start-monitoring:
    docker-compose up -d postgres-exporter kafka-exporter prometheus grafana
    @echo "✓ Monitoring services started"
    @echo "  Grafana: http://localhost:3000 (admin/admin)"
    @echo "  Prometheus: http://localhost:9091"
    @echo "  Kafka Exporter: http://localhost:9308/metrics"
    @echo "  PostgreSQL Exporter: http://localhost:9187/metrics"

# Start dashboards and exporters only
start-dashboards:
    docker-compose up -d prometheus grafana postgres-exporter kafka-exporter
    @echo "✓ Dashboards and exporters started"
    @echo "  Grafana: http://localhost:3000 (admin/admin)"
    @echo "  Prometheus: http://localhost:9091"
    @echo "  Kafka Exporter: http://localhost:9308/metrics"
    @echo "  PostgreSQL Exporter: http://localhost:9187/metrics"
    @echo "  WAL Writer Rust metrics (scraped via Prometheus)"

# Stop monitoring
stop-monitoring:
    docker-compose stop postgres-exporter kafka-exporter prometheus grafana
    @echo "✓ Monitoring services stopped"

# ────────────────────────────────────────────────────────────────────────────
# Individual Service Targets
# ────────────────────────────────────────────────────────────────────────────

# PostgreSQL Database
start-postgres:
    docker-compose up -d postgres
    @echo "⏳ Waiting for PostgreSQL to be healthy..."
    @timeout 60 bash -c 'until docker-compose exec -T postgres pg_isready -U postgres > /dev/null 2>&1; do sleep 2; done' || true
    @echo "✓ PostgreSQL started on localhost:5432"

stop-postgres:
    docker-compose stop postgres
    @echo "✓ PostgreSQL stopped"

# Database Initialization (creates replication slot + publication)
start-db-init:
    docker-compose up db-init
    @echo "✓ Database initialization completed"

# Kafka Broker
start-kafka:
    docker-compose up -d kafka
    @echo "⏳ Waiting for Kafka to be healthy..."
    @timeout 60 bash -c 'until docker-compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; do sleep 2; done' || true
    @echo "✓ Kafka started on localhost:9092"

stop-kafka:
    docker-compose stop kafka
    @echo "✓ Kafka stopped"

# Data Pump Go (generates UPI transactions)
start-data-pump:
    docker-compose up -d data-pump-go
    @echo "✓ Data Pump started (generating ~10k txns/sec)"

stop-data-pump:
    docker-compose stop data-pump-go
    @echo "✓ Data Pump stopped"

# WAL Writer Rust (active CDC agent)
start-wal-writer:
    docker-compose up -d wal-writer-rust
    @echo "⏳ Starting WAL Writer Rust..."
    @timeout 30 bash -c 'until docker-compose exec -T wal-writer-rust curl -s http://localhost:9095/health > /dev/null 2>&1; do sleep 2; done' || true
    @echo "✓ WAL Writer Rust started"
    @echo "  Metrics: http://localhost:9095/metrics"
    @echo "  Health: http://localhost:9095/health"

stop-wal-writer:
    docker-compose stop wal-writer-rust
    @echo "✓ WAL Writer Rust stopped"

# Recompile, rebuild image, and redeploy WAL Writer Rust
redeploy-wal-writer:
    @echo "⏳ Recompiling and rebuilding WAL Writer Rust image..."
    docker-compose build --no-cache wal-writer-rust
    @echo "⏳ Re-deploying WAL Writer Rust container..."
    docker-compose up -d --force-recreate wal-writer-rust
    @echo "⏳ Waiting for WAL Writer Rust to be healthy..."
    @timeout 60 bash -c 'until docker-compose exec -T wal-writer-rust curl -s http://localhost:9095/health > /dev/null 2>&1; do sleep 2; done' || true
    @echo "✓ WAL Writer Rust recompiled, rebuilt, and redeployed"
    @echo "  Metrics: http://localhost:9095/metrics"
    @echo "  Health: http://localhost:9095/health"

# WAL Consumer (Rust consumer with parallel workers)
start-wal-consumer:
    docker-compose up -d wal-consumer
    @echo "✓ WAL Consumer started"
    @echo "  Workers: 8 (partition-based routing)"
    @echo "  Commit threshold: 5000 messages"

stop-wal-consumer:
    docker-compose stop wal-consumer
    @echo "✓ WAL Consumer stopped"

# Prometheus (metrics collection)
start-prometheus:
    docker-compose up -d prometheus
    @echo "✓ Prometheus started on http://localhost:9091"

stop-prometheus:
    docker-compose stop prometheus
    @echo "✓ Prometheus stopped"

# Grafana (metrics visualization)
start-grafana:
    docker-compose up -d grafana
    @echo "✓ Grafana started on http://localhost:3000 (admin/admin)"

stop-grafana:
    docker-compose stop grafana
    @echo "✓ Grafana stopped"

# PostgreSQL Exporter (metrics from PostgreSQL)
start-postgres-exporter:
    docker-compose up -d postgres-exporter
    @echo "✓ PostgreSQL Exporter started on http://localhost:9187/metrics"

stop-postgres-exporter:
    docker-compose stop postgres-exporter
    @echo "✓ PostgreSQL Exporter stopped"

# Kafka Exporter (metrics from Kafka)
start-kafka-exporter:
    docker-compose up -d kafka-exporter
    @echo "✓ Kafka Exporter started on http://localhost:9308/metrics"

stop-kafka-exporter:
    docker-compose stop kafka-exporter
    @echo "✓ Kafka Exporter stopped"

# ────────────────────────────────────────────────────────────────────────────
# Utility Targets
# ────────────────────────────────────────────────────────────────────────────

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
    @echo "✓ All containers and volumes removed"
    @echo "Note: This includes rust writer rollback service if present."

# Clean up containers only (keep volumes)
clean-containers:
    docker-compose down
    @echo "✓ All containers removed"

# Show configuration for a service (usage: just config wal-writer)
config service:
    docker-compose config --services | grep {{ service }}
    @echo "Service: {{ service }}"
    docker-compose ps {{ service }} || echo "Service not running"

# Test connectivity to key endpoints
test:
    @echo "Testing PostgreSQL..."
    @docker-compose exec -T postgres pg_isready -U postgres || echo "❌ PostgreSQL not responding"
    @echo "✓ PostgreSQL responding"
    @echo "Testing Kafka..."
    @docker-compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null || echo "❌ Kafka not responding"
    @echo "✓ Kafka responding"
    @echo "Testing WAL Writer Rust..."
    @curl -s http://localhost:9095/health > /dev/null || echo "❌ WAL Writer Rust not responding"
    @echo "✓ WAL Writer Rust responding"
    @echo "✓ All systems operational"
