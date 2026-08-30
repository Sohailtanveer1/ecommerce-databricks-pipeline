"""
Minimal CDC sink: drains Debezium topics from Redpanda and writes the change
envelopes as JSON files to a local landing folder, which is then uploaded to ADLS
`landing/cdc/...` (Auto Loader picks them up -> src/ingestion/cdc_to_bronze.py).

Why a sink script (not Kafka Connect ADLS sink): keeps the local stack light and
the bridge to the cloud explicit. In a real deployment you'd use Debezium Server
sinking straight to Azure Event Hubs, or the Kafka Connect ADLS Gen2 sink.

Usage:
  pip install kafka-python
  python docker/cdc_sink.py --out ./_landing/cdc --once
  # then upload:  azcopy copy "./_landing/cdc/*" "https://<acct>.dfs.core.windows.net/landing/cdc/" --recursive
"""

from __future__ import annotations

import argparse
import json
import os
import time


def main():
    from kafka import KafkaConsumer  # kafka-python

    ap = argparse.ArgumentParser()
    # Redpanda's EXTERNAL listener (see docker-compose). Host clients must use
    # this, not 9092 (which advertises the in-network name `redpanda`).
    ap.add_argument("--brokers", default="localhost:19092")
    ap.add_argument("--out", default="./_landing/cdc")
    ap.add_argument("--once", action="store_true", help="drain what's available then exit")
    args = ap.parse_args()

    consumer = KafkaConsumer(
        bootstrap_servers=args.brokers,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="cdc-sink",
        value_deserializer=lambda v: v.decode("utf-8") if v else None,
        consumer_timeout_ms=8000 if args.once else float("inf"),
    )
    consumer.subscribe(pattern="postgres_txn\\.public\\..*")

    counts: dict[str, int] = {}
    for msg in consumer:
        if not msg.value:
            continue
        # topic: postgres_txn.public.orders -> system=postgres_txn table=orders
        parts = msg.topic.split(".")
        system, table = parts[0], parts[-1]
        d = f"{args.out}/{system}/{table}"
        os.makedirs(d, exist_ok=True)
        # Debezium value is {schema, payload}; keep payload (the envelope).
        env = json.loads(msg.value)
        payload = env.get("payload", env)
        with open(f"{d}/{table}-{msg.partition}-{msg.offset}.json", "w") as fh:
            json.dump(payload, fh)
        counts[table] = counts.get(table, 0) + 1

    print("sunk:", counts or "(nothing new)")
    if not args.once:
        while True:
            time.sleep(5)


if __name__ == "__main__":
    main()
