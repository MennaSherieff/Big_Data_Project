# Dataset Schema Documentation

## UNSW-NB15 (4 CSV files, ~2.54M rows)

| Column | Type | Description |
|--------|------|-------------|
| srcip | string | Source IP |
| sport | int | Source port |
| dstip | string | Destination IP |
| dsport | int | Destination port |
| proto | string | Protocol (tcp/udp/...) |
| state | string | Connection state |
| dur | float | Duration |
| sbytes | int | Source→dest bytes |
| dbytes | int | Dest→source bytes |
| sttl | int | Source TTL |
| dttl | int | Dest TTL |
| ... | ... | 49 features total |
| label | int | **0 = Normal, 1 = Attack** |
| attack_cat | string | Attack category (Fuzzers, DoS, etc.) |

**Label mapping:** Binary (0/1) — 9 attack categories

---

## CIC-IDS-2017 (8 CSV files, ~2.83M rows)

> ⚠️ CIC-IDS headers have **leading spaces** — strip them before use.

| Column | Type | Description |
|--------|------|-------------|
| Flow ID | string | Unique flow identifier |
| Source IP | string | Source IP |
| Source Port | int | Source port |
| Destination IP | string | Dest IP |
| Destination Port | int | Dest port |
| Protocol | int | Protocol number |
| Flow Duration | int | Duration in microseconds |
| Total Fwd Packets | int | Forward packet count |
| ... | ... | 84 features total |
| Label | string | **"BENIGN" or attack name** |

**Label mapping:** String — BENIGN, DoS Hulk, PortScan, DDoS, etc.

---

## HDFS Storage Format

- Format: **Parquet** (Snappy compressed)
- Partitioning: by source file (`part_XXXXX.parquet`)
- Metadata columns added during ingestion:
  - `_dataset` — `"unsw"` or `"cic"`
  - `_source_file` — original filename
  - `_ingest_ts` — Unix timestamp (ms) of ingestion

## Known Issues to Hand Off to Member 2

1. CIC-IDS column names have leading whitespace → strip before joining
2. UNSW label is int (0/1); CIC label is string → needs unified taxonomy
3. Both datasets have `Inf` and `NaN` in some float columns
4. Class imbalance: ~80% BENIGN in CIC-IDS