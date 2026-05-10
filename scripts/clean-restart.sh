#!/bin/bash
# Clean restart: remove replication slots, volumes, and start services fresh

set -e

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

echo "=== WAL Writer Clean Restart Script ==="
echo

# 1. Stop all services
echo "[1/6] Stopping all docker-compose services..."
docker compose down --remove-orphans 2>&1 | grep -E '(Stopping|Removed|network)' || true
echo "✓ Services stopped"
echo

# Drop replication slots if PostgreSQL is still running
echo "[2/6] Dropping replication slots (if any)..."
if docker compose ps postgres 2>/dev/null | grep -q 'postgres'; then
  echo "  Attempting to drop slots from running PostgreSQL..."
  # Try to drop the slot if it exists (may fail if not superuser or slot doesn't exist)
  docker compose exec -T postgres psql -U postgres -d postgres << 'EOF' 2>/dev/null || true
SELECT pg_drop_replication_slot(slot_name) 
FROM pg_replication_slots 
WHERE slot_name IN ('wal_writer_slot', 'wal_writer_slot_v2');
EOF
  echo "  ✓ Slots dropped (or didn't exist)"
else
  echo "  PostgreSQL not running; slots will be recreated from scratch"
fi
echo

# 3. Remove volumes (clean slate)
echo "[3/6] Removing data volumes..."
docker volume rm wal-cake-writer_postgres-data wal-cake-writer_kafka-data 2>/dev/null || true
docker volume rm postgres-data kafka-data 2>/dev/null || true
# Also try without prefix in case docker-compose didn't create them with a prefix
docker volume rm postgres-data kafka-data prometheus-data grafana-data logs-data 2>/dev/null || true
echo "✓ Volumes removed"
echo

# 4. Start services
echo "[4/6] Starting all services..."
docker compose up -d --build 2>&1 | grep -E '(Creating|Started|Building)' || true
echo "✓ Services started"
echo

# 5. Wait for PostgreSQL to be healthy
echo "[5/6] Waiting for PostgreSQL to be ready..."
for i in {1..60}; do
  if docker compose exec -T postgres pg_isready -U postgres > /dev/null 2>&1; then
    echo "✓ PostgreSQL is healthy"
    break
  fi
  if [ $i -eq 60 ]; then
    echo "✗ PostgreSQL did not become healthy after 60 attempts"
    exit 1
  fi
  sleep 1
done
echo

# 6. Verify replication slot and publication
echo "[6/6] Verifying replication slot and publication..."
docker compose exec -T postgres psql -U postgres -d postgres -c \
  "SELECT slot_name, plugin, slot_type, active, restart_lsn FROM pg_replication_slots WHERE slot_name IN ('wal_writer_slot', 'wal_writer_slot_v2');" || true
echo

docker compose exec -T postgres psql -U postgres -d postgres -c \
  "SELECT pubname, puballtables FROM pg_publication WHERE pubname = 'wal_writer_publication';" || true
echo

echo "Verifying Kafka is healthy..."
for i in {1..30}; do
  if docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; then
    echo "✓ Kafka is healthy"
    break
  fi
  if [ $i -eq 30 ]; then
    echo "✗ Kafka did not become healthy after 30 attempts"
    exit 1
  fi
  sleep 2
done
echo

echo "=== ✓ Clean restart complete ==="
echo
echo "Services running:"
docker compose ps --format "table {{.Names}}\t{{.Status}}"
echo
echo "Next steps:"
echo "  - Monitor WAL writer: docker compose logs -f wal-writer"
echo "  - Monitor data pump: docker compose logs -f data-pump-go"
echo "  - Monitor consumer: docker compose logs -f wal-consumer"
echo "  - Access Grafana: http://localhost:3000 (admin/admin)"
echo
