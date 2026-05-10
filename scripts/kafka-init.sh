#!/bin/sh
set -e

echo "Waiting for Kafka broker to become available..."
until kafka-topics --bootstrap-server kafka:9092 --list > /dev/null 2>&1; do
  sleep 2
  echo "Waiting for Kafka broker..."
done

echo "Kafka broker is available. Creating CDC topics with explicit partition counts..."
for t in cdc.public.upi_transactions cdc.public.user_subscription cdc.public.users; do
  echo "Creating topic $t..."
  kafka-topics --bootstrap-server kafka:9092 --create --topic "$t" --partitions 4 --replication-factor 1 --if-not-exists
  echo "Created or existed: $t"
done

echo "Kafka CDC topics created."
echo "Listing topics after initialization:"
kafka-topics --bootstrap-server kafka:9092 --list
exit 0
