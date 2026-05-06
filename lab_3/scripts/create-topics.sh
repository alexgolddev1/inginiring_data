#!/bin/sh
set -eu

echo "Waiting for brokers to accept requests..."
sleep 20

kafka-topics --bootstrap-server broker1:29092,broker2:29093 \
  --create \
  --if-not-exists \
  --topic Topic1 \
  --partitions 2 \
  --replication-factor 2

kafka-topics --bootstrap-server broker1:29092,broker2:29093 \
  --create \
  --if-not-exists \
  --topic Topic2 \
  --partitions 2 \
  --replication-factor 2

echo "Topics created."
