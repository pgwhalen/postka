-- Postka Schema: PostgreSQL-backed Kafka replacement
-- This migration creates the core tables and functions needed for Postka
-- Using per-topic tables with PostgreSQL range partitioning on partition_id

-- Enum for timestamp type (matches Kafka's TimestampType)
CREATE TYPE postka_timestamp_type AS ENUM ('CREATE_TIME', 'LOG_APPEND_TIME');

-- Composite type for record headers (key-value pairs where key is text and value is binary)
CREATE TYPE postka_header AS (
    key TEXT,
    value BYTEA
);

-- Sequence for generating unique table name suffixes
CREATE SEQUENCE postka_topic_id_seq;

-- Topics metadata table
CREATE TABLE postka_topics (
    topic_name TEXT PRIMARY KEY,
    partition_count INTEGER NOT NULL DEFAULT 1,
    records_table_name TEXT NOT NULL UNIQUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT (NOW() AT TIME ZONE 'UTC')
);

-- Consumer group offset tracking
CREATE TABLE postka_consumer_offsets (
    group_id TEXT NOT NULL,
    topic_name TEXT NOT NULL REFERENCES postka_topics(topic_name),
    partition_id INTEGER NOT NULL,
    committed_offset BIGINT NOT NULL,
    metadata TEXT,
    committed_at TIMESTAMP WITH TIME ZONE DEFAULT (NOW() AT TIME ZONE 'UTC'),
    PRIMARY KEY (group_id, topic_name, partition_id)
);

-- Function to generate a unique table name for a topic
-- Sanitizes topic name: replace non-alphanumeric with _, truncate to 40 chars, append sequence ID
CREATE OR REPLACE FUNCTION postka_generate_table_name(p_topic TEXT)
RETURNS TEXT AS $$
DECLARE
    sanitized TEXT;
    seq_id BIGINT;
BEGIN
    -- Replace non-alphanumeric characters with underscores
    sanitized := regexp_replace(p_topic, '[^a-zA-Z0-9]', '_', 'g');
    -- Truncate to 40 characters
    sanitized := left(sanitized, 40);
    -- Get next sequence value
    seq_id := nextval('postka_topic_id_seq');
    -- Return table name
    RETURN 'postka_records_' || sanitized || '_' || seq_id;
END;
$$ LANGUAGE plpgsql;

-- Function to create a per-topic partitioned table
CREATE OR REPLACE FUNCTION postka_create_topic_table(p_table_name TEXT)
RETURNS VOID AS $$
BEGIN
    EXECUTE format('
        CREATE TABLE %I (
            id BIGSERIAL,
            partition_id INTEGER NOT NULL,
            offset_id BIGINT NOT NULL,
            record_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
            timestamp_type postka_timestamp_type NOT NULL DEFAULT ''CREATE_TIME'',
            key_bytes BYTEA,
            value_bytes BYTEA,
            headers postka_header[] DEFAULT ARRAY[]::postka_header[],
            created_at TIMESTAMP WITH TIME ZONE DEFAULT (NOW() AT TIME ZONE ''UTC''),
            PRIMARY KEY (partition_id, offset_id)
        ) PARTITION BY RANGE (partition_id)', p_table_name);

    -- Create index for efficient consumer polling
    EXECUTE format('CREATE INDEX %I ON %I (partition_id, offset_id)',
        'idx_' || p_table_name || '_poll', p_table_name);
END;
$$ LANGUAGE plpgsql;

-- Function to add a partition to a topic table
CREATE OR REPLACE FUNCTION postka_add_partition(p_table_name TEXT, p_partition_id INTEGER)
RETURNS VOID AS $$
DECLARE
    partition_table_name TEXT;
BEGIN
    partition_table_name := p_table_name || '_p' || p_partition_id;
    EXECUTE format('CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%s) TO (%s)',
        partition_table_name, p_table_name, p_partition_id, p_partition_id + 1);
END;
$$ LANGUAGE plpgsql;

-- Function to get the records table name for a topic
CREATE OR REPLACE FUNCTION postka_get_records_table(p_topic TEXT)
RETURNS TEXT AS $$
DECLARE
    table_name TEXT;
BEGIN
    SELECT records_table_name INTO table_name
    FROM postka_topics
    WHERE topic_name = p_topic;
    RETURN table_name;
END;
$$ LANGUAGE plpgsql;

-- Function to get next offset for a table/partition (atomic read)
CREATE OR REPLACE FUNCTION postka_next_offset(p_table_name TEXT, p_partition INTEGER)
RETURNS BIGINT AS $$
DECLARE
    next_offset BIGINT;
BEGIN
    EXECUTE format('SELECT COALESCE(MAX(offset_id), -1) + 1 FROM %I WHERE partition_id = $1', p_table_name)
    INTO next_offset
    USING p_partition;
    RETURN next_offset;
END;
$$ LANGUAGE plpgsql;

-- Function to auto-create topic, table and partitions if they don't exist
-- Returns the records table name
CREATE OR REPLACE FUNCTION postka_ensure_topic(p_topic TEXT, p_partitions INTEGER DEFAULT 1)
RETURNS TEXT AS $$
DECLARE
    table_name TEXT;
BEGIN
    -- Check if topic already exists
    SELECT records_table_name INTO table_name
    FROM postka_topics
    WHERE topic_name = p_topic;

    IF table_name IS NOT NULL THEN
        RETURN table_name;
    END IF;

    -- Generate table name
    table_name := postka_generate_table_name(p_topic);

    -- Insert topic
    INSERT INTO postka_topics (topic_name, partition_count, records_table_name)
    VALUES (p_topic, p_partitions, table_name);

    -- Create the partitioned table
    PERFORM postka_create_topic_table(table_name);

    -- Create PostgreSQL partitions for each partition_id
    FOR i IN 0..(p_partitions - 1) LOOP
        PERFORM postka_add_partition(table_name, i);
    END LOOP;

    RETURN table_name;
END;
$$ LANGUAGE plpgsql;

-- Trigger function to handle partition_count updates
CREATE OR REPLACE FUNCTION postka_partition_count_trigger()
RETURNS TRIGGER AS $$
DECLARE
    table_name TEXT;
BEGIN
    -- Prevent decreasing partition count
    IF NEW.partition_count < OLD.partition_count THEN
        RAISE EXCEPTION 'Cannot decrease partition count from % to %', OLD.partition_count, NEW.partition_count;
    END IF;

    -- If partition count increased, create new PostgreSQL partitions
    IF NEW.partition_count > OLD.partition_count THEN
        table_name := OLD.records_table_name;
        FOR i IN OLD.partition_count..(NEW.partition_count - 1) LOOP
            PERFORM postka_add_partition(table_name, i);
        END LOOP;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger for partition_count updates
CREATE TRIGGER trg_postka_partition_count
    BEFORE UPDATE OF partition_count ON postka_topics
    FOR EACH ROW
    EXECUTE FUNCTION postka_partition_count_trigger();
