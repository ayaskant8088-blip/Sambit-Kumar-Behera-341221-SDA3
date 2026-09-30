# SDA Assignment 3 - Dashboard for Analysis of Consumed Data

## Overview
This repository contains the data pipeline and dashboard configuration for analyzing real-time banking transactions. 
Data flows from a local CSV through a Kafka producer (`producer.py`), is consumed by `consumer.py`, and is written to a MongoDB Atlas cluster. MongoDB Atlas Charts is used to visualize the streaming data in real-time.

## Files Included
* `docker-compose.yml`: Sets up the Zookeeper and Kafka cluster.
* `producer.py`: Streams data from `txn_data.csv` to the Kafka topic `txn-topic` at 1 event/sec.
* `consumer.py`: Subscribes to `txn-topic`, appends a timestamp, and inserts documents into MongoDB Atlas.
* `requirements.txt`: Python dependencies.

## How to Run
1. Start the Kafka cluster:
   ```bash
   docker-compose up -d