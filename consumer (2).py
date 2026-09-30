"""
consumer.py
Assignment 3 — Consumes banking transactions from Kafka and stores them in MongoDB Atlas.
"""

from kafka import KafkaConsumer
import json
from pymongo import MongoClient
import datetime
import sys

# --- MONGODB SETUP ---
# Replace this with your actual MongoDB Atlas Connection String
# Example: "mongodb+srv://<username>:<password>@cluster0.mongodb.net/?retryWrites=true&w=majority"
MONGO_URI = "YOUR_MONGODB_ATLAS_CONNECTION_STRING"

try:
    client = MongoClient(MONGO_URI)
    db = client['sda_assignment']          # Database name
    collection = db['transactions']        # Collection name
    print("✅ Connected to MongoDB Atlas")
except Exception as e:
    print(f"❌ Failed to connect to MongoDB: {e}")
    sys.exit(1)

# --- KAFKA SETUP ---
TOPIC = 'txn-topic'

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f"📡 Listening on '{TOPIC}' and pushing to MongoDB... (Ctrl+C to stop)\n")

total = 0

for msg in consumer:
    data = msg.value
    
    # Add a timestamp for dashboard time-series plotting
    data['timestamp'] = datetime.datetime.utcnow()
    
    # Insert the message into MongoDB
    collection.insert_one(data)
    
    total += 1
    if total % 10 == 0:
        print(f"💾 Inserted {total} transactions into MongoDB Atlas...")