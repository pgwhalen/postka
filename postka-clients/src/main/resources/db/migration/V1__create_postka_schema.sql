-- Postka Schema: PostgreSQL-backed Kafka replacement
-- This migration creates the core tables and functions needed for Postka

-- Enum for timestamp type (matches Kafka's TimestampType)
CREATE TYPE postka_timestamp_type AS ENUM ('CREATE_TIME', 'LOG_APPEND_TIME');

-- Composite type for record headers (key-value pairs where key is text and value is binary)
CREATE TYPE postka_header AS (
    key TEXT,
    value BYTEA
);

-- Topics metadata table
CREATE TABLE postka_topics (
    topic_name TEXT PRIMARY KEY,
    partition_count INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT (NOW() AT TIME ZONE 'UTC')
);

-- Partitions within topics
CREATE TABLE postka_partitions (
    topic_name TEXT NOT NULL REFERENCES postka_topics(topic_name) ON DELETE CASCADE,
    partition_id INTEGER NOT NULL,
    PRIMARY KEY (topic_name, partition_id)
);

-- Records storage - the main message table
CREATE TABLE postka_records (
    id BIGSERIAL PRIMARY KEY,
    topic_name TEXT NOT NULL,
    partition_id INTEGER NOT NULL,
    offset_id BIGINT NOT NULL,
    record_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT (NOW() AT TIME ZONE 'UTC'),
    timestamp_type postka_timestamp_type NOT NULL DEFAULT 'CREATE_TIME',
    key_bytes BYTEA,
    value_bytes BYTEA,
    headers postka_header[] DEFAULT ARRAY[]::postka_header[],
    created_at TIMESTAMP WITH TIME ZONE DEFAULT (NOW() AT TIME ZONE 'UTC'),
    UNIQUE (topic_name, partition_id, offset_id),
    FOREIGN KEY (topic_name, partition_id) REFERENCES postka_partitions(topic_name, partition_id)
);

-- Index for efficient consumer polling
CREATE INDEX idx_postka_records_poll
    ON postka_records(topic_name, partition_id, offset_id);

-- Consumer group offset tracking
CREATE TABLE postka_consumer_offsets (
    group_id TEXT NOT NULL,
    topic_name TEXT NOT NULL,
    partition_id INTEGER NOT NULL,
    committed_offset BIGINT NOT NULL,
    metadata TEXT,
    committed_at TIMESTAMP WITH TIME ZONE DEFAULT (NOW() AT TIME ZONE 'UTC'),
    PRIMARY KEY (group_id, topic_name, partition_id),
    FOREIGN KEY (topic_name, partition_id) REFERENCES postka_partitions(topic_name, partition_id)
);

-- Function to get next offset for a topic/partition (atomic read)
CREATE OR REPLACE FUNCTION postka_next_offset(p_topic VARCHAR, p_partition INTEGER)
RETURNS BIGINT AS $$
DECLARE
    next_offset BIGINT;
BEGIN
    SELECT COALESCE(MAX(offset_id), -1) + 1 INTO next_offset
    FROM postka_records
    WHERE topic_name = p_topic AND partition_id = p_partition;
    RETURN next_offset;
END;
$$ LANGUAGE plpgsql;

-- Function to auto-create topic and partitions if they don't exist
CREATE OR REPLACE FUNCTION postka_ensure_topic(p_topic VARCHAR, p_partitions INTEGER DEFAULT 1)
RETURNS VOID AS $$
BEGIN
    INSERT INTO postka_topics (topic_name, partition_count)
    VALUES (p_topic, p_partitions)
    ON CONFLICT (topic_name) DO NOTHING;

    FOR i IN 0..(p_partitions - 1) LOOP
        INSERT INTO postka_partitions (topic_name, partition_id)
        VALUES (p_topic, i)
        ON CONFLICT DO NOTHING;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
