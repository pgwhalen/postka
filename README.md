# Postka

**Postka** is a PostgreSQL-backed implementation of the Apache Kafka client API. The name is a portmanteau of "Postgres" and "Kafka".

## Project Goals

### Drop-in Kafka Replacement

Postka aims to be a **drop-in replacement** for the Apache Kafka Java client libraries. The API mirrors Kafka's client API exactly, with one simple naming convention: replace "Kafka" with "Postka".

```java
// Kafka
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

// Postka
import producer.com.pgwhalen.postka.clients.PostkaProducer;
import producer.com.pgwhalen.postka.clients.ProducerRecord;
```

A simple find-and-replace of `kafka` → `postka` and `Kafka` → `Postka` should be sufficient to migrate between implementations.

### Functional Parity Over Performance

The primary goal is **functional parity with Kafka**, not performance optimization. Postka should behave identically to Kafka from an API consumer's perspective. Performance tuning is a secondary concern and should never compromise correctness or API compatibility.

### When to Use Postka

Postka is ideal for scenarios where:
- You already have PostgreSQL infrastructure and want to avoid Kafka operational complexity
- Message throughput is moderate (thousands/sec rather than millions/sec)
- You value transactional guarantees with your existing database
- You want simpler local development without running Kafka/Zookeeper
- You need exactly-once semantics by coordinating with other database operations

Postka is **not** a replacement for Kafka in high-throughput, distributed streaming scenarios.

## Architecture Overview

```
┌─────────────────┐     ┌─────────────────┐
│  PostkaProducer │     │  PostkaConsumer │
└────────┬────────┘     └────────┬────────┘
         │                       │
         │   JDBC/SQL            │
         ▼                       ▼
┌─────────────────────────────────────────┐
│              PostgreSQL                 │
│  (topics, partitions, records, offsets) │
└─────────────────────────────────────────┘
```

Instead of connecting to Kafka brokers, Postka clients connect to a PostgreSQL database. Topics, partitions, records, and consumer group offsets are all stored in PostgreSQL tables.

## Testing Strategy

### Proving Feature Parity

The testing strategy is designed to **prove functional parity** between Postka and Kafka. This is achieved through:

1. **Abstract Test Contracts**: An abstract base test class (`AbstractProducerConsumerTest`) defines the expected behavior that both implementations must satisfy.

2. **Parallel Implementations**: Two concrete test classes extend the abstract contract:
   - `KafkaProducerConsumerTest` - runs against a real Kafka broker
   - `PostkaProducerConsumerTest` - runs against a real PostgreSQL database

3. **TestContainers**: Both test suites use [TestContainers](https://testcontainers.org/) to spin up real instances of Kafka and PostgreSQL. No mocks are used—tests run against actual infrastructure.

```
AbstractProducerConsumerTest (defines contract)
        │
        ├── KafkaProducerConsumerTest (real Kafka)
        │
        └── PostkaProducerConsumerTest (real PostgreSQL)
```

### Writing New Tests

When adding new functionality:

1. **Add the test to the abstract class first**. Define what behavior you expect.
2. **Verify the Kafka test passes**. This confirms your test correctly captures Kafka's behavior.
3. **Implement the Postka feature** until the Postka test also passes.

This ensures every Postka feature is verified against real Kafka behavior.

### Running Tests

```bash
# Run all tests
./gradlew test

# Run only Kafka tests (to verify test correctness)
./gradlew test --tests "*KafkaProducerConsumerTest*"

# Run only Postka tests
./gradlew test --tests "*PostkaProducerConsumerTest*"
```

**Note**: Tests require Docker to be running for TestContainers.

## Database Setup

Postka uses [Flyway](https://flywaydb.org/) for database migrations. Migrations are located in:

```
postka-clients/src/main/resources/db/migration/
```

For production deployment, run Flyway migrations against your PostgreSQL database before using Postka clients.

## Design Principles for Contributors

### 1. Mirror Kafka's API Exactly

When implementing new features, always reference the [Kafka JavaDoc](https://kafka.apache.org/documentation/) and match:
- Method signatures
- Parameter names
- Exception types
- Behavioral semantics

### 2. Test Against Real Kafka First

Before implementing a feature in Postka:
1. Write a test that uses real Kafka
2. Verify the test passes and captures the expected behavior
3. Then implement the Postka version

### 3. Prefer Correctness Over Cleverness

The PostgreSQL implementation doesn't need to be clever or optimal—it needs to be correct. Simple, readable SQL is preferred over complex optimizations.

### 4. Configuration Compatibility

Postka-specific configuration (like `DATASOURCE_CONFIG`) should coexist with Kafka configuration keys. Applications should be able to use the same configuration map for both, with Postka ignoring Kafka-specific keys and vice versa.

