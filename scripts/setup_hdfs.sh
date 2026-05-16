#!/bin/bash
echo "Creating HDFS directory structure..."

docker exec namenode hdfs dfs -mkdir -p /data/raw/unsw
docker exec namenode hdfs dfs -mkdir -p /data/raw/cic
docker exec namenode hdfs dfs -mkdir -p /data/processed/unsw
docker exec namenode hdfs dfs -mkdir -p /data/processed/cic
docker exec namenode hdfs dfs -mkdir -p /data/unified
docker exec namenode hdfs dfs -mkdir -p /models
docker exec namenode hdfs dfs -mkdir -p /logs/ingestion

# Set permissions
docker exec namenode hdfs dfs -chmod -R 775 /data
docker exec namenode hdfs dfs -chmod -R 775 /models
docker exec namenode hdfs dfs -chmod -R 775 /logs

# Verify
docker exec namenode hdfs dfs -ls -R /data

echo "Done."