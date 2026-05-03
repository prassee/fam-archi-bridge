# Justfile for managing services

# Start all services (core + monitoring)
start:
    docker-compose up -d postgres
    @echo "Waiting for postgres to be healthy (30s timeout)..."
    @timeout 30 bash -c 'until docker-compose ps postgres | grep -q "healthy"; do sleep 2; done' || true
    docker-compose up -d data-pump-go postgres-exporter prometheus grafana
    @echo "All services started!"
    @echo "Grafana: http://localhost:3000 (admin/admin)"
    @echo "Prometheus: http://localhost:9091"

# Stop all services
stop:
    docker-compose stop data-pump-go postgres postgres-exporter prometheus grafana
    @echo "All services stopped!"

# Start core services only (postgres + data-pump-go)
start-core:
    docker-compose up -d postgres
    @echo "Waiting for postgres to be healthy (30s timeout)..."
    @timeout 30 bash -c 'until docker-compose ps postgres | grep -q "healthy"; do sleep 2; done' || true
    docker-compose up -d data-pump-go
    @echo "Core services started!"

# Stop core services only
stop-core:
    docker-compose stop data-pump-go postgres
    @echo "Core services stopped!"

# Stop core services only
stop-data-dump:
    docker-compose stop data-pump-go
    @echo "Data Dump Service stopped!"

# Start Kafka broker
start-kafka:
    docker-compose up -d kafka
    @echo "Kafka started on localhost:9092"

# Stop Kafka broker
stop-kafka:
    docker-compose stop kafka
    @echo "Kafka stopped!"

# Start WAL Writer Rust container
start-wal-writer:
    docker-compose up -d wal-writer
    @echo "WAL Writer started on http://localhost:9090"

# Stop WAL Writer Rust container
stop-wal-writer:
    docker-compose stop wal-writer
    @echo "WAL Writer stopped!"


# Start monitoring services (Prometheus + Grafana + exporters)
monitoring-start:
    docker-compose up -d postgres-exporter kafka-exporter prometheus grafana
    @echo "Monitoring services started!"
    @echo "Grafana: http://localhost:3000 (admin/admin)"
    @echo "Prometheus: http://localhost:9091"
    @echo "Kafka Exporter: http://localhost:9308/metrics"

# Stop monitoring services
monitoring-stop:
    docker-compose stop postgres-exporter kafka-exporter prometheus grafana
    @echo "Monitoring services stopped!"

# Stop all services
stop-all:
    docker-compose stop
    @echo "All services stopped!"

# View logs for postgres and data-pump-go
logs:
    docker-compose logs -f postgres data-pump-go

# View logs for monitoring services
logs-monitoring:
    docker-compose logs -f postgres-exporter kafka-exporter prometheus grafana

# Check status of postgres and data-pump-go
status:
    docker-compose ps postgres data-pump-go

# Check status of monitoring services
status-monitoring:
    docker-compose ps postgres-exporter kafka-exporter prometheus grafana

# Restart core services
restart:
    just stop
    just start

# Restart monitoring services
restart-monitoring:
    just monitoring-stop
    just monitoring-start

# Clean up (remove containers and volumes for core services)
clean:
    docker-compose rm -fsv postgres data-pump-go
    docker volume rm rust-wal-cake-writer_postgres-data 2>/dev/null || true
    @echo "Cleanup completed!"

# Clean up monitoring
clean-monitoring:
    docker-compose rm -fsv postgres-exporter kafka-exporter prometheus grafana
    docker volume rm rust-wal-cake-writer_prometheus-data rust-wal-cake-writer_grafana-data 2>/dev/null || true
    @echo "Monitoring cleanup completed!"
