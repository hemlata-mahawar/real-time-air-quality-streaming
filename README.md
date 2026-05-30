# Real-Time Air Quality Analytics Pipeline using Kafka & PySpark

## Overview

This project implements a **real-time air quality analytics pipeline** using **Apache Kafka** and **PySpark Structured Streaming**. The system processes streaming air-quality data, performs window-based aggregations, applies watermarking for late-arriving events, and ranks cities based on Air Quality Index (AQI) metrics.

The pipeline simulates a real-world streaming analytics use case where environmental sensor data is continuously ingested and analyzed in near real time.

---

## Architecture

```text
Air Quality Dataset
        ↓
Kafka Producer
        ↓
Kafka Topic
        ↓
PySpark Structured Streaming Consumer
        ↓
Data Cleaning & Parsing
        ↓
Watermarking + Window Aggregation
        ↓
AQI Ranking by City
        ↓
Streaming Output



        ┌──────────────────────────────┐
        │   Air Quality CSV (GCS)      │
        │   (Raw, Unsorted, Dirty)     │
        └─────────────┬────────────────┘
                      ↓
        ┌──────────────────────────────┐
        │ Dataproc Batch Job           │
        │ (PySpark Cleaning Layer)     │
        │ - Fix missing hours          │
        │ - Remove bad rows            │
        │ - Sort by city & time        │
        └─────────────┬────────────────┘
                      ↓
        ┌──────────────────────────────┐
        │ Kafka Producer (PySpark)     │
        │ (Simulate Streaming)         │
        │ Send records → Kafka Topic   │
        └─────────────┬────────────────┘
                      ↓
────────────────────────────────────────────

        ┌──────────────────────────────┐
        │ Kafka Broker (Compute VM)    │
        │ Topic: air-quality-stream    │
        └─────────────┬────────────────┘
                      ↓
        ┌──────────────────────────────┐
        │ Dataproc Streaming Job       │
        │ (Structured Streaming)       │
        │ Read data from Kafka         │
        └─────────────┬────────────────┘
                      ↓
        ┌──────────────────────────────┐
        │ 8-hour Sliding Window        │
        │ (Slide every 1 hour)         │
        └─────────────┬────────────────┘
                      ↓
        ┌──────────────────────────────┐
        │ Compute Metrics              │
        │ V1: Avg AQI                  │
        │ V2: Max Pollutant Spike      │
        └─────────────┬────────────────┘
                      ↓
        ┌──────────────────────────────┐
        │ Ranking Logic                │
        │ Sort by V1, then V2          │
        └─────────────┬────────────────┘
                      ↓
        ┌──────────────────────────────┐
        │ Output (Console / Sink)      │
        │ Hourly City Rankings         │
        └──────────────────────────────┘
```

---

## Technologies Used

* Python
* Apache Kafka
* PySpark Structured Streaming
* Spark SQL
* JSON Data Processing

---

## Project Features

* Real-time data streaming using Kafka
* Structured Streaming with PySpark
* JSON message parsing from Kafka topics
* Event-time processing
* Watermarking for late data handling
* Sliding window aggregations
* AQI-based city ranking
* Fault tolerance using checkpointing

---

## Project Structure

```text
.
├── kafka_producer.py
├── spark_streaming_pipeline.py
├── dataset/
│   └── air_quality_data.csv
├── system_flow.txt
└── README.md

```

### File Description

#### `kafka_producer.py`

Reads air quality data from a dataset and streams records to a Kafka topic in real time.

#### `spark_streaming_pipeline.py`

Consumes Kafka messages using PySpark Structured Streaming, processes event-time data, performs aggregations, and ranks cities based on AQI metrics.

---

## Data Processing Workflow

### 1. Data Ingestion

Air quality data is streamed to Kafka using a custom producer script.

### 2. Stream Consumption

PySpark Structured Streaming consumes messages from Kafka topics.

### 3. Data Cleaning & Parsing

* JSON messages are parsed
* Invalid or missing records are filtered
* Event timestamps are converted into Spark timestamp format

### 4. Event-Time Processing

The pipeline uses:

* **Watermarking:** `5 minutes`
* **Window Duration:** `8 hours`
* **Sliding Interval:** `1 hour`

This enables handling delayed events while performing time-based aggregations.

### 5. AQI Analytics

The system computes AQI-related metrics and ranks cities according to pollution severity.

---

## How to Run the Project

### Step 1: Start Kafka

Start Kafka locally.

Create a Kafka topic:

```bash
kafka-topics.sh --create \
--topic air-quality-topic \
--bootstrap-server localhost:9092
```

---

### Step 2: Install Dependencies

```bash
pip install pyspark kafka-python
```

---

### Step 3: Run Kafka Producer

```bash
python kafka_producer.py
```

---

### Step 4: Run Streaming Pipeline

```bash
python spark_streaming_pipeline.py
```

---

## Example Concepts Used

* Kafka Producer & Consumer
* Real-Time Data Streaming
* Event-Time Processing
* Watermarking
* Sliding Window Aggregation
* Ranking using Window Functions
* Structured Streaming

---

## Learning Outcomes

Through this project, I learned:

* Building real-time streaming pipelines
* Working with Apache Kafka for event streaming
* Using PySpark Structured Streaming
* Handling late-arriving data using watermarking
* Performing time-window analytics
* Processing streaming JSON data efficiently

---

## Future Improvements

* Store streaming results in databases or cloud storage
* Build a dashboard for AQI monitoring
* Add real-time alerts for high AQI values
* Deploy the pipeline on cloud infrastructure

---

## Author

**Hemlata Mahawar**

Data Engineering & Machine Learning Enthusiast
