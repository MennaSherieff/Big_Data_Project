import json
import time
import logging
import os
import subprocess
import tempfile
from collections import defaultdict

from kafka import KafkaConsumer
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

KAFKA_BROKER = "kafka:29092"
TOPICS       = ["unsw-raw", "cic-raw"]

HDFS_PATHS = {
    "unsw-raw": "/data/raw/unsw",
    "cic-raw":  "/data/raw/cic",
}

# Local temp dir inside the container (shared with namenode via docker volume if needed)
LOCAL_TEMP = "/tmp/parquet_staging"

# How many rows to buffer before flushing to HDFS
BATCH_SIZE = 50000

os.makedirs(LOCAL_TEMP, exist_ok=True)


def flush_to_hdfs(rows, topic, part_index):
    """
    Write rows as Parquet locally, then copy to HDFS using
    'docker exec namenode hdfs dfs -put' via subprocess.
    This avoids the datanode redirect issue with the Python hdfs client.
    """
    if not rows:
        return 0

    df         = pd.DataFrame(rows)
    local_path = f"{LOCAL_TEMP}/{topic}_part_{part_index:05d}.parquet"
    hdfs_path  = f"{HDFS_PATHS[topic]}/part_{part_index:05d}.parquet"

    # Write Parquet locally
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, local_path, compression="snappy")
    file_size_mb = os.path.getsize(local_path) / 1024 / 1024
    log.info(f"  Written locally: {local_path} ({file_size_mb:.2f} MB)")

    # Copy local file into the namenode container, then put to HDFS
    # Step 1: docker cp local_path → namenode:/tmp/
    namenode_tmp = f"/tmp/{os.path.basename(local_path)}"
    cp_cmd = ["docker", "cp", local_path, f"namenode:{namenode_tmp}"]
    result = subprocess.run(cp_cmd, capture_output=True, text=True)
    if result.returncode != 0:
        log.error(f"docker cp failed: {result.stderr}")
        return 0

    # Step 2: hdfs dfs -put from inside namenode
    put_cmd = [
        "docker", "exec", "namenode",
        "hdfs", "dfs", "-put", "-f",
        namenode_tmp, hdfs_path
    ]
    result = subprocess.run(put_cmd, capture_output=True, text=True)
    if result.returncode != 0:
        log.error(f"hdfs put failed: {result.stderr}")
        return 0

    # Step 3: cleanup temp files
    os.remove(local_path)
    subprocess.run(
        ["docker", "exec", "namenode", "rm", "-f", namenode_tmp],
        capture_output=True
    )

    log.info(f"  Uploaded to HDFS: {hdfs_path} ({len(df)} rows)")
    return len(df)


def validate_batch(rows, topic):
    """Basic integrity checks before flushing."""
    df       = pd.DataFrame(rows)
    n_rows   = len(df)
    n_cols   = len(df.columns)
    null_pct = df.isnull().mean().max() * 100

    if null_pct > 50:
        log.warning(f"[{topic}] High null rate: {null_pct:.1f}%")
    else:
        log.info(f"[{topic}] Batch OK — {n_rows} rows, {n_cols} cols, max null%={null_pct:.1f}")

    return df


def make_consumer():
    return KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="hdfs-writer",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=60000,  # stop after 60s of no messages
    )


def main():
    consumer   = make_consumer()
    buffers    = defaultdict(list)
    part_idx   = defaultdict(int)
    row_counts = defaultdict(int)

    log.info("Consumer started — listening for messages...")

    for msg in consumer:
        topic = msg.topic
        buffers[topic].append(msg.value)

        if len(buffers[topic]) >= BATCH_SIZE:
            validate_batch(buffers[topic], topic)
            written = flush_to_hdfs(buffers[topic], topic, part_idx[topic])
            row_counts[topic] += written
            part_idx[topic]   += 1
            buffers[topic]     = []

    # Flush any remaining rows
    log.info("No more messages — flushing remaining buffers...")
    for topic, rows in buffers.items():
        if rows:
            validate_batch(rows, topic)
            written = flush_to_hdfs(rows, topic, part_idx[topic])
            row_counts[topic] += written

    log.info("=" * 50)
    log.info("Consumer finished. Final row counts:")
    for topic, count in row_counts.items():
        log.info(f"  {topic}: {count:,} rows written to HDFS")
    log.info("=" * 50)


if __name__ == "__main__":
    main()