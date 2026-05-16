import subprocess
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)

# Expected approximate row counts (fill in after your first run)
EXPECTED = {
    "/data/raw/unsw": 2_540_000,   # ~2.5M rows across 4 files
    "/data/raw/cic":  2_830_000,   # ~2.8M rows across 8 files
}

def hdfs_ls(path):
    result = subprocess.run(
        ["docker", "exec", "namenode", "hdfs", "dfs", "-ls", path],
        capture_output=True, text=True
    )
    return result.stdout

def count_parquet_files(path):
    out = hdfs_ls(path)
    return sum(1 for line in out.splitlines() if ".parquet" in line)

for path, expected in EXPECTED.items():
    n_files = count_parquet_files(path)
    log.info(f"{path}: {n_files} Parquet files found (expected ~{expected:,} rows total)")

log.info("Tip: open http://localhost:9870 to browse HDFS visually")