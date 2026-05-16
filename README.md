# Big Data IDS Project 

# — Phase 1: Data Ingestion & Storage

## What this does
Streams UNSW-NB15 and CIC-IDS-2017 network intrusion datasets
through a Kafka pipeline into an HDFS data lake as Parquet files.

## Stack
- Apache Kafka (Confluent 7.5.0)
- Apache Hadoop HDFS (3.2.1)
- Apache Spark (3.5.0)
- Python 3.11 (kafka-python, pandas, pyarrow, hdfs)

## Quick Start
```bash
# 1. Start all services
docker compose up -d

# 2. Create HDFS directories
bash scripts/setup_hdfs.sh

# 3. Install Python deps
docker exec python-runner pip install kafka-python pandas pyarrow hdfs

# 4. Create Kafka topics
docker exec kafka kafka-topics --create --bootstrap-server kafka:29092 \
  --topic unsw-raw --partitions 4 --replication-factor 1
docker exec kafka kafka-topics --create --bootstrap-server kafka:29092 \
  --topic cic-raw --partitions 4 --replication-factor 1

# 5. Start consumer (background), then producer
docker exec python-runner python /kafka/consumer.py 
docker exec python-runner python /kafka/producer.py

# 6. Validate
python scripts/validate.py
```

## Results
| Dataset | Files | HDFS Size | Replicated |
|---|---|---|---|
| UNSW-NB15 | 38 Parquet | 158 MB | 474 MB |
| CIC-IDS-2017 | 57 Parquet | 407 MB | 1.2 GB |

## HDFS Structure
/data/
/raw/
/unsw/   ← 38 Parquet files (Snappy compressed)
/cic/    ← 57 Parquet files (Snappy compressed)
/processed/   ← Phase 2 output
/unified/     ← Phase 2 merged dataset
/models/         ← Phase 3 output

## Handoff to Member 2
- HDFS path: `hdfs://namenode:9000/data/raw/`
- Format: Parquet, Snappy compressed
- CIC columns have leading whitespace → strip on read
- Added metadata cols: `_dataset`, `_source_file`, `_ingest_ts`
- See `docs/schema.md` for full column documentation

# - Phase 2 start
git clone https://github.com/MennaSherieff/Big_Data_Project.git
cd Big_Data_Project

# Start containers (Kafka + HDFS + Spark)
docker compose up -d

# Verify HDFS still has the data
docker exec namenode hdfs dfs -du -h /data/raw/

# Expected output:
407.5 M  1.2 G    /data/raw/cic
158.0 M  474.1 M  /data/raw/unsw

# Step 2 — runs Spark, not Kafka

# Open a Spark shell connected to HDFS
docker exec spark spark-shell \
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000

# Or run a Python script
docker exec spark spark-submit \
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
  /path/to/preprocessing.py
