#!/bin/sh
set -eu

BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-broker1:29092,broker2:29093}"

echo "Waiting for Kafka brokers on ${BOOTSTRAP_SERVERS}..."
sleep 15

for topic in trip-duration-avg-by-day trip-count-by-day top-start-station-by-day top-3-stations-by-day; do
  kafka-topics --bootstrap-server "$BOOTSTRAP_SERVERS" \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions 2 \
    --replication-factor 2
done

echo "Kafka Streams output topics are ready."
