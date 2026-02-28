#!/usr/bin/env bash
set -euo pipefail

topic_name=${1:-events}
partitions=${KAFKA_NUM_PARTITIONS:-6}
replication=${KAFKA_REPLICATION_FACTOR:-1}

kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --create \
  --if-not-exists \
  --topic "$topic_name" \
  --replication-factor "$replication" \
  --partitions "$partitions"
