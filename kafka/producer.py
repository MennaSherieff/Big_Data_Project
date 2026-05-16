import os
import csv
import json
import time
import logging
from kafka import KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

KAFKA_BROKER = "kafka:29092"
BATCH_DELAY  = 0.001   # seconds between rows (tune as needed)

DATASET_CONFIGS = {
    "unsw": {
        "topic": "unsw-raw",
        "files": [
            "/data/unsw/UNSW-NB15_1.csv",
            "/data/unsw/UNSW-NB15_2.csv",
            "/data/unsw/UNSW-NB15_3.csv",
            "/data/unsw/UNSW-NB15_4.csv",
        ]
    },
    "cic": {
        "topic": "cic-raw",
        "files": [
            "/data/cic/Monday-WorkingHours.pcap_ISCX.csv",
            "/data/cic/Tuesday-WorkingHours.pcap_ISCX.csv",
            "/data/cic/Wednesday-workingHours.pcap_ISCX.csv",
            "/data/cic/Thursday-WorkingHours-Morning-WebAttacks.pcap_ISCX.csv",
            "/data/cic/Thursday-WorkingHours-Afternoon-Infilteration.pcap_ISCX.csv",
            "/data/cic/Friday-WorkingHours-Morning.pcap_ISCX.csv",
            "/data/cic/Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv",
            "/data/cic/Friday-WorkingHours-Afternoon-PortScan.pcap_ISCX.csv",
        ]
    }
}

def make_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",              # wait for broker confirmation
        retries=5,
        batch_size=16384,
        linger_ms=5,
    )

def stream_file(producer, filepath, topic, dataset_name, source_file):
    if not os.path.exists(filepath):
        log.warning(f"File not found: {filepath}")
        return 0

    count = 0
    log.info(f"Streaming {filepath} → topic '{topic}'")

    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Strip whitespace from CIC-IDS headers (they have leading spaces)
            row = {k.strip(): v.strip() for k, v in row.items()}

            # Attach metadata so the consumer knows the origin
            row["_dataset"]     = dataset_name
            row["_source_file"] = source_file
            row["_ingest_ts"]   = int(time.time() * 1000)

            producer.send(
                topic,
                key=f"{dataset_name}_{count}",
                value=row
            )
            count += 1

            if count % 10000 == 0:
                producer.flush()
                log.info(f"  {source_file}: {count} rows sent")

            time.sleep(BATCH_DELAY)

    producer.flush()
    log.info(f"  Finished {source_file}: {count} rows total")
    return count

def main():
    producer = make_producer()
    totals = {}

    for dataset_name, config in DATASET_CONFIGS.items():
        topic     = config["topic"]
        total     = 0
        for filepath in config["files"]:
            source_file = os.path.basename(filepath)
            n = stream_file(producer, filepath, topic, dataset_name, source_file)
            total += n
        totals[dataset_name] = total
        log.info(f"Dataset '{dataset_name}' complete: {total} rows → topic '{topic}'")

    producer.close()
    log.info(f"All done. Summary: {totals}")

if __name__ == "__main__":
    main()