# =========================
# CONFIG FILE
# Member 2 Spark Pipeline
# =========================


HDFS_RAW_UNSW = "hdfs://namenode:9000/data/raw/unsw/"
HDFS_RAW_CIC  = "hdfs://namenode:9000/data/raw/cic/"

HDFS_OUTPUT_CLEAN = "hdfs://namenode:9000/data/processed/unified_clean/"

FEATURE_COLS = [
    "flow_duration",
    "fwd_bytes",
    "bwd_bytes",
    "fwd_pkts",
    "bwd_pkts",
    "total_bytes",
    "flow_pkts_per_sec",
    "flow_bytes_per_sec",
    "down_up_ratio"
]