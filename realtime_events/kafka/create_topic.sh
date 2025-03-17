#!/bin/bash
docker exec kafka kafka-topics.sh --create --topic user_events --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
