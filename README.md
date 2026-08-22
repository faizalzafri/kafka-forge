# Kafka Forge: Production-Grade Kafka Reference Implementation

Kafka Forge is an enterprise-grade reference repository containing patterns and configurations for building highly resilient, high-throughput applications using **Apache Kafka**, **Java 21**, **Jackson Databind**, and **Docker**.

This repository showcases advanced messaging patterns including **Non-blocking Retries & Dead Letter Queues (DLQ)**, and **Multi-threaded flow control with partition Pause/Resume Backpressure using Java 21 Virtual Threads**.

---

## Repository Modules

The project is structured as a Maven multi-module workspace:

| Module | Description | Core Tech Stack |
| :--- | :--- | :--- |
| [`kafka-basic`](file:///e:/Dev/kafka-forge/kafka-basic) | Standard Kafka API showcases (producers, consumers, partition-keyed routing). | Kafka Clients |
| [`kafka-producer-twitter`](file:///e:/Dev/kafka-forge/kafka-producer-twitter) | Resilient Twitter streaming ingestion client utilizing safe producer properties. | Kafka Clients, HBC |
| [`kafka-consumer-elasticsearch`](file:///e:/Dev/kafka-forge/kafka-consumer-elasticsearch) | Elasticsearch ingestion consumer showcasing manual offset control and Bulk API requests. | Elasticsearch client |
| [`kafka-streams-filter-tweets`](file:///e:/Dev/kafka-forge/kafka-streams-filter-tweets) | Real-time streams application mapping, filtering, and routing high-velocity event streams. | Kafka Streams |
| [`kafka-consumer-retry-dlq`](file:///e:/Dev/kafka-forge/kafka-consumer-retry-dlq) | Resilient consumer implementing non-blocking retry topics and a Dead Letter Queue (DLQ). | Kafka Clients, Jackson |
| [`kafka-consumer-backpressure`](file:///e:/Dev/kafka-forge/kafka-consumer-backpressure) | High-performance consumer using Java 21 Virtual Threads, Semaphore gates, and partition pause/resume flow control. | Kafka Clients, Jackson, Java 21 |

---

## Design Patterns

### 1. Resilient Non-blocking Retries & DLQ
To avoid blocking the partition consumption thread when encountering transient network or database failures, this module routes failed messages to separate **Retry Topics** (with exponential backoff) and eventually to a **Dead Letter Queue (DLQ)** for permanent errors. 
- Main Consumer commits the offset immediately after delegating to the retry flow.
- A dedicated Retry Consumer processes retries with a configured backoff delay.
- Non-recoverable/poison pill records are sent directly to the DLQ.

### 2. High-Throughput Pause/Resume Backpressure (Java 21 Virtual Threads)
When processing records concurrently using a thread pool, consuming too fast will saturate memory or downstream systems. This module implements partition-level pause/resume flow control:
- Polls records using an allocation-free iterator.
- Submits processing tasks to lightweight **Java 21 Virtual Threads**.
- Tracks concurrent executions using a `Semaphore` (acting as a watermark).
- If active tasks hit the `highWatermark`, assigned partitions are **paused** and outstanding batch records are **seeked back** to allow the poll loop to continue running (avoiding session timeouts).
- Once active tasks drain below the `lowWatermark`, partitions are **resumed**.

---

## Local Infrastructure Setup

A [`docker-compose.yml`](file:///e:/Dev/kafka-forge/docker-compose.yml) file is provided in the root directory to spin up the local development stack:
- **ZooKeeper**: `localhost:2181`
- **Kafka Broker**: `localhost:9092`
- **Elasticsearch**: `localhost:9200`

### 1. Start Docker Containers
Make sure Docker Desktop is running, then execute:
```bash
docker compose up -d
```

### 2. Build the Codebase
Build the project binaries using the Java 21 SDK runtime path:
```bash
$env:JAVA_HOME="C:\Users\Faizal\.sdkman\candidates\java\21.0.11-tem"
& "C:\Users\Faizal\.sdkman\candidates\maven\current\bin\mvn.cmd" clean package
```

### 3. Run Automated Validation Tests
Run all unit and integration test suites (uses Testcontainers to automatically manage temporary Kafka test instances):
```bash
& "C:\Users\Faizal\.sdkman\candidates\maven\current\bin\mvn.cmd" clean verify
```

---

## How to Run & Test Manually

### A. Testing the Elasticsearch Consumer
1. Create the `twitter` topic:
   ```bash
   docker exec -it kafka-forge-broker kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic twitter
   ```
2. Run the Elasticsearch bulk consumer:
   ```bash
   java -cp kafka-consumer-elasticsearch/target/kafka-consumer-elasticsearch-1.0.jar com.github.faizalzafri.kafkaapp.ElasticSearchConsumerBulk
   ```
3. Produce test messages to the topic:
   ```bash
   docker exec -it kafka-forge-broker kafka-console-producer --bootstrap-server localhost:9092 --topic twitter
   # Paste this sample tweet JSON:
   {"id_str":"1001","text":"Standardizing on Jackson and Java 21 Virtual Threads!","user":{"followers_count":15000}}
   ```
4. Verify the document was indexed in Elasticsearch:
   ```bash
   curl http://localhost:9200/twitter/_search?pretty
   ```

### B. Testing the Backpressure Consumer
1. Create the `customer-events` topic:
   ```bash
   docker exec -it kafka-forge-broker kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic customer-events
   ```
2. Run the backpressured app:
   ```bash
   java -cp kafka-consumer-backpressure/target/kafka-consumer-backpressure-1.0.jar com.github.faizalzafri.kafkaapp.BackpressureConsumerApp
   ```
3. Observe console output. The application simulates slow database writes and logs the exact pause/resume triggers and offset commits.

---

## License

This project is licensed under the [MIT License](file:///e:/Dev/kafka-forge/LICENSE).
