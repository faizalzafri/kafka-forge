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
| [`kafka-spring-forge`](file:///e:/Dev/kafka-forge/kafka-spring-forge) | Production-grade Spring Boot 3.3 application showcasing Spring Kafka, `@RetryableTopic`, `@DltHandler`, Spring Kafka Streams, Spring Data Elasticsearch, and Java 21 Virtual Threads. | Spring Boot 3.3, Spring Kafka, Spring Data ES |

---

## Design Patterns

### 1. Resilient Non-blocking Retries & DLQ
To avoid blocking the partition consumption thread when encountering transient network or database failures, this repository demonstrates non-blocking retries across two paradigms:
- **Core Java (`kafka-consumer-retry-dlq`)**: Custom partition-level seek and backoff delays, routing unrecoverable failures to DLQ headers.
- **Spring Kafka (`kafka-spring-forge`)**: Declarative `@RetryableTopic(attempts = "3", backoff = @Backoff(delay = 1000, multiplier = 2.0))` with `@DltHandler` for instant poison-pill isolation.

### 2. High-Throughput Pause/Resume Backpressure (Java 21 Virtual Threads)
When processing records concurrently using a thread pool, consuming too fast will saturate memory or downstream systems:
- **Core Java (`kafka-consumer-backpressure`)**: Allocation-free batch iteration with `Semaphore` gates, partition `pause()` and `seek()` to safe contiguous committed offsets.
- **Spring Boot 3 (`kafka-spring-forge`)**: Native Java 21 Virtual Thread support via `spring.threads.virtual.enabled: true` and dynamic container pause/resume via `KafkaListenerEndpointRegistry`.

### 3. Real-Time Stream Processing & Elasticsearch Sinks
- **Kafka Streams**: Declarative `KStream` topology filtering and branching events in real-time.
- **Elasticsearch Ingestion**: High-throughput document indexing with automatic mapping and offset tracking.

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
Run all unit and integration test suites across all 7 modules:
```bash
& "C:\Users\Faizal\.sdkman\candidates\maven\current\bin\mvn.cmd" clean verify
```

---

## How to Run & Test Manually

For local execution, we use Maven plugin runners which automatically resolve classpaths and dependencies.

### A. Testing the Elasticsearch Consumer
1. Create the `twitter` topic:
   ```bash
   docker exec -i kafka-forge-broker kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic twitter
   ```
2. Run the Elasticsearch bulk consumer:
   ```bash
   $env:JAVA_HOME="C:\Users\Faizal\.sdkman\candidates\java\21.0.11-tem"
   & "C:\Users\Faizal\.sdkman\candidates\maven\current\bin\mvn.cmd" -pl kafka-consumer-elasticsearch exec:java '-Dexec.mainClass=com.github.faizalzafri.kafkaapp.ElasticSearchConsumerBulk'
   ```
3. Produce test messages to the topic:
   ```bash
   docker exec -i kafka-forge-broker kafka-console-producer --bootstrap-server localhost:9092 --topic twitter
   # Paste this sample tweet JSON:
   {"id_str":"1001","text":"Standardizing on Jackson and Java 21 Virtual Threads!","user":{"followers_count":15000}}
   ```
4. Verify the document was indexed in Elasticsearch:
   ```bash
   # In PowerShell:
   (Invoke-RestMethod -Uri "http://localhost:9200/twitter/_search?pretty").hits.hits
   
   # Or in standard bash/curl:
   curl -s http://localhost:9200/twitter/_search?pretty
   ```

### B. Testing the Backpressure Consumer
1. Create the `customer-events` topic:
   ```bash
   docker exec -i kafka-forge-broker kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic customer-events
   ```
2. Run the backpressured app:
   ```bash
   $env:JAVA_HOME="C:\Users\Faizal\.sdkman\candidates\java\21.0.11-tem"
   & "C:\Users\Faizal\.sdkman\candidates\maven\current\bin\mvn.cmd" -pl kafka-consumer-backpressure exec:java '-Dexec.mainClass=com.github.faizalzafri.kafkaapp.BackpressureConsumerApp'
   ```
3. Observe console output. The application simulates slow database writes and logs the exact pause/resume triggers and offset commits.

### C. Testing the Retry & DLQ Consumer
1. Create the orders topics:
   ```bash
   docker exec -i kafka-forge-broker kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic main-orders
   docker exec -i kafka-forge-broker kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic main-orders-retry
   docker exec -i kafka-forge-broker kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic main-orders-dlq
   ```
2. Run the Retry/DLQ app:
   ```bash
   $env:JAVA_HOME="C:\Users\Faizal\.sdkman\candidates\java\21.0.11-tem"
   & "C:\Users\Faizal\.sdkman\candidates\maven\current\bin\mvn.cmd" -pl kafka-consumer-retry-dlq exec:java '-Dexec.mainClass=com.github.faizalzafri.kafkaapp.RetryDlpConsumerApp'
   ```
3. Produce test messages to the `main-orders` topic to witness successful processing, permanent failure routing to DLQ, and transient failures retrying with backoff before exhaustion.

### D. Testing the Spring Boot Application (`kafka-spring-forge`)
1. Run the Spring Boot application:
   ```bash
   $env:JAVA_HOME="C:\Users\Faizal\.sdkman\candidates\java\21.0.11-tem"
   & "C:\Users\Faizal\.sdkman\candidates\maven\current\bin\mvn.cmd" -pl kafka-spring-forge spring-boot:run
   ```
2. Observe Spring Kafka auto-provisioning topics, starting Virtual Thread listener containers, configuring `@RetryableTopic` backoff chains, starting `@EnableKafkaStreams` topologies, and connecting to Elasticsearch.

---

## License

This project is open-sourced under the terms of the [MIT License](file:///e:/Dev/kafka-forge/LICENSE).

