"""
producer.py
Assignment 2 — streams sample banking transaction events to the Kafka
topic 'txn-topic', one row every second, simulating a live feed from
the Core Banking Transaction Processing System (see Assignment 1).
"""

import csv
import time
import json
import sys
from kafka import KafkaProducer, errors

KAFKA_BROKER = 'localhost:9092'
TOPIC = 'txn-topic'
CSV_FILE = 'txn_data.csv'

try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=5
    )
    producer.partitions_for(TOPIC)   # fails fast if broker is unreachable
    print("✅ Connected to Kafka at", KAFKA_BROKER)
except errors.NoBrokersAvailable:
    print("❌ Kafka not running — start it with: docker-compose up -d")
    sys.exit(1)

with open(CSV_FILE, 'r') as f:
    reader = csv.DictReader(f)
    count = 0
    for row in reader:
        # cast numeric fields back from string (CSV stores everything as text)
        row['amount'] = float(row['amount'])
        row['risk_score'] = float(row['risk_score'])
        row['balance_after'] = float(row['balance_after'])

        producer.send(TOPIC, key=row['account_id'].encode('utf-8'), value=row)
        count += 1
        print(f"Produced [{count}] {row['txn_id']} | {row['account_id']} | "
              f"{row['txn_type']} ₹{row['amount']:,.2f} | {row['payment_status']}")
        time.sleep(1)   # 1 event/sec — simulates a live tick feed

producer.flush()
print(f"✅ Done — {count} transaction events sent to '{TOPIC}'")
