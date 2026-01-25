# Claude Code Guide

This file helps Claude (and other AI assistants) understand the Postka project.

## Quick Start

**Read [README.md](./README.md)** for:
- Project goals and design philosophy
- Testing strategy (critical for maintaining Kafka parity)
- API reference and usage examples
- Design principles for contributors

## Key Concepts

1. **API Mirroring**: All classes mirror Kafka's API with "Kafka" → "Postka" naming
2. **Test-Driven Parity**: Always test against real Kafka first, then implement for Postka
3. **Correctness Over Performance**: Simple, correct implementations are preferred

## Adding Features
 - When adding a new feature to Postka, **ALWAYS** at a compatability test to  
 - When changing the flyway migration to add a new feature, assume the existing migration can be changed rather than
 creating a new migration.  This is alpha software not running in production, so we don't need to worry about migrating
 data yet.

## Build & Test

```bash
./gradlew build    # Compile
./gradlew test     # Run all tests (requires Docker)
```

## Project Structure

```
postka-clients/
├── src/main/java/com/pgwhalen/postka/
│   ├── clients/producer/   # PostkaProducer, ProducerRecord, etc.
│   ├── clients/consumer/   # PostkaConsumer, ConsumerRecord, etc.
│   └── common/             # Shared types (TopicPartition, serializers)
├── src/main/resources/db/migration/  # Flyway SQL migrations
└── src/test/java/          # TestContainers tests
```
