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

-- Composite type for partition offset requests
CREATE TYPE postka_partition_offset AS (
    partition_id INTEGER,
    start_offset BIGINT
);

-- Table function to fetch records for a topic given partition/offset pairs
-- Returns records starting at each specified offset for each partition
CREATE OR REPLACE FUNCTION postka_fetch_records(
    p_topic TEXT,
    p_partition_offsets postka_partition_offset[]
)
RETURNS TABLE (
    partition_id INTEGER,
    offset_id BIGINT,
    record_timestamp TIMESTAMP WITH TIME ZONE,
    key_bytes BYTEA,
    value_bytes BYTEA,
    headers postka_header[]
) AS $$
DECLARE
    table_name TEXT;
    partition_count INTEGER;
    po postka_partition_offset;
    where_clauses TEXT[];
    where_clause TEXT;
    i INTEGER;
BEGIN
    -- Get table name and partition count for topic
    SELECT t.records_table_name, t.partition_count
    INTO table_name, partition_count
    FROM postka_topics t
    WHERE t.topic_name = p_topic;

    IF table_name IS NULL THEN
        RETURN;
    END IF;

    -- If no partition offsets provided, use all partitions starting at offset 0
    IF p_partition_offsets IS NULL OR array_length(p_partition_offsets, 1) IS NULL THEN
        p_partition_offsets := ARRAY[]::postka_partition_offset[];
        FOR i IN 0..(partition_count - 1) LOOP
            p_partition_offsets := array_append(p_partition_offsets, ROW(i, 0)::postka_partition_offset);
        END LOOP;
    END IF;

    -- Build WHERE clause from partition/offset pairs
    where_clauses := ARRAY[]::TEXT[];
    FOREACH po IN ARRAY p_partition_offsets LOOP
        where_clauses := array_append(where_clauses,
            format('(partition_id = %s AND offset_id >= %s)', po.partition_id, po.start_offset));
    END LOOP;

    IF array_length(where_clauses, 1) IS NULL THEN
        RETURN;
    END IF;

    where_clause := array_to_string(where_clauses, ' OR ');

    -- Execute dynamic query and return results
    RETURN QUERY EXECUTE format(
        'SELECT r.partition_id, r.offset_id, r.record_timestamp, r.key_bytes, r.value_bytes, r.headers
         FROM %I r
         WHERE %s
         ORDER BY r.partition_id, r.offset_id',
        table_name, where_clause);
END;
$$ LANGUAGE plpgsql;

-- Function to insert a record into a topic
-- Handles topic creation, offset assignment, and record insertion
-- Returns the assigned offset
CREATE OR REPLACE FUNCTION postka_insert_record(
    p_topic TEXT,
    p_partition INTEGER,
    p_timestamp TIMESTAMP WITH TIME ZONE,
    p_key_bytes BYTEA,
    p_value_bytes BYTEA,
    p_headers postka_header[]
)
RETURNS BIGINT AS $$
DECLARE
    table_name TEXT;
    assigned_offset BIGINT;
BEGIN
    -- Ensure topic exists and get the records table name
    table_name := postka_ensure_topic(p_topic, 1);

    -- Get next offset and insert record atomically
    EXECUTE format(
        'INSERT INTO %I (partition_id, offset_id, record_timestamp, key_bytes, value_bytes, headers)
         VALUES ($1, postka_next_offset(%L, $1), $2, $3, $4, $5)
         RETURNING offset_id',
        table_name, table_name)
    INTO assigned_offset
    USING p_partition, p_timestamp, p_key_bytes, p_value_bytes, p_headers;

    RETURN assigned_offset;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- Consumer Group Coordination Tables and Functions
-- ============================================================================

-- Consumer group membership tracking
CREATE TABLE postka_consumer_group_members (
    group_id TEXT NOT NULL,
    member_id TEXT NOT NULL,  -- UUID per consumer instance
    topic_name TEXT NOT NULL REFERENCES postka_topics(topic_name),
    joined_at TIMESTAMP WITH TIME ZONE DEFAULT (NOW() AT TIME ZONE 'UTC'),
    last_heartbeat TIMESTAMP WITH TIME ZONE DEFAULT (NOW() AT TIME ZONE 'UTC'),
    PRIMARY KEY (group_id, member_id, topic_name)
);

-- Partition assignments for consumer groups
CREATE TABLE postka_partition_assignments (
    group_id TEXT NOT NULL,
    topic_name TEXT NOT NULL REFERENCES postka_topics(topic_name),
    partition_id INTEGER NOT NULL,
    member_id TEXT NOT NULL,
    assigned_at TIMESTAMP WITH TIME ZONE DEFAULT (NOW() AT TIME ZONE 'UTC'),
    PRIMARY KEY (group_id, topic_name, partition_id)
);

-- Consumer group generation tracking (incremented on each rebalance)
CREATE TABLE postka_group_generation (
    group_id TEXT NOT NULL,
    topic_name TEXT NOT NULL REFERENCES postka_topics(topic_name),
    generation_id INTEGER NOT NULL DEFAULT 0,
    last_rebalance TIMESTAMP WITH TIME ZONE DEFAULT (NOW() AT TIME ZONE 'UTC'),
    PRIMARY KEY (group_id, topic_name)
);

-- Composite type for partition assignment result
CREATE TYPE postka_join_result AS (
    generation_id INTEGER,
    rebalance_triggered BOOLEAN,
    assigned_partitions INTEGER[]
);

-- Function to assign partitions using range assignment strategy
-- Sorts members alphabetically, distributes partitions evenly
CREATE OR REPLACE FUNCTION postka_assign_partitions(p_group_id TEXT, p_topic TEXT)
RETURNS VOID AS $$
DECLARE
    partition_count INTEGER;
    member_ids TEXT[];
    member_count INTEGER;
    partitions_per_member INTEGER;
    extra_partitions INTEGER;
    current_partition INTEGER := 0;
    partitions_for_this_member INTEGER;
    member TEXT;
    i INTEGER;
BEGIN
    -- Get partition count for the topic
    SELECT t.partition_count INTO partition_count
    FROM postka_topics t
    WHERE t.topic_name = p_topic;

    IF partition_count IS NULL THEN
        RETURN;
    END IF;

    -- Get sorted list of member IDs
    SELECT array_agg(m.member_id ORDER BY m.member_id)
    INTO member_ids
    FROM postka_consumer_group_members m
    WHERE m.group_id = p_group_id AND m.topic_name = p_topic;

    IF member_ids IS NULL OR array_length(member_ids, 1) IS NULL THEN
        -- No members, clear all assignments
        DELETE FROM postka_partition_assignments
        WHERE group_id = p_group_id AND topic_name = p_topic;
        RETURN;
    END IF;

    member_count := array_length(member_ids, 1);
    partitions_per_member := partition_count / member_count;
    extra_partitions := partition_count % member_count;

    -- Clear existing assignments
    DELETE FROM postka_partition_assignments
    WHERE group_id = p_group_id AND topic_name = p_topic;

    -- Assign partitions using range assignment
    FOREACH member IN ARRAY member_ids LOOP
        -- First 'extra_partitions' members get one extra partition
        IF extra_partitions > 0 THEN
            partitions_for_this_member := partitions_per_member + 1;
            extra_partitions := extra_partitions - 1;
        ELSE
            partitions_for_this_member := partitions_per_member;
        END IF;

        -- Assign consecutive partitions to this member
        FOR i IN 0..(partitions_for_this_member - 1) LOOP
            INSERT INTO postka_partition_assignments (group_id, topic_name, partition_id, member_id)
            VALUES (p_group_id, p_topic, current_partition, member);
            current_partition := current_partition + 1;
        END LOOP;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Function for a consumer to join or heartbeat to a group
-- Uses advisory lock for coordination
-- Returns generation_id, whether rebalance was triggered, and assigned partitions
CREATE OR REPLACE FUNCTION postka_join_group(
    p_group_id TEXT,
    p_member_id TEXT,
    p_topic TEXT,
    p_session_timeout_ms INTEGER DEFAULT 10000
)
RETURNS postka_join_result AS $$
DECLARE
    lock_id BIGINT;
    membership_changed BOOLEAN := FALSE;
    current_gen INTEGER;
    result postka_join_result;
    stale_cutoff TIMESTAMP WITH TIME ZONE;
    deleted_count INTEGER;
    existing_member BOOLEAN;
BEGIN
    -- Ensure the topic exists (auto-create with 1 partition if not)
    PERFORM postka_ensure_topic(p_topic, 1);

    -- Compute lock ID from group_id and topic
    lock_id := hashtext(p_group_id || '::' || p_topic);

    -- Acquire advisory lock for this group/topic combination
    PERFORM pg_advisory_lock(lock_id);

    BEGIN
        -- Calculate stale cutoff time
        stale_cutoff := (NOW() AT TIME ZONE 'UTC') - (p_session_timeout_ms || ' milliseconds')::interval;

        -- Check if this member already exists
        SELECT EXISTS(
            SELECT 1 FROM postka_consumer_group_members
            WHERE group_id = p_group_id AND member_id = p_member_id AND topic_name = p_topic
        ) INTO existing_member;

        -- Clean up stale members (except ourselves)
        DELETE FROM postka_consumer_group_members
        WHERE group_id = p_group_id
          AND topic_name = p_topic
          AND member_id != p_member_id
          AND last_heartbeat < stale_cutoff;
        GET DIAGNOSTICS deleted_count = ROW_COUNT;

        IF deleted_count > 0 THEN
            membership_changed := TRUE;
        END IF;

        -- Insert or update our membership
        INSERT INTO postka_consumer_group_members (group_id, member_id, topic_name, joined_at, last_heartbeat)
        VALUES (p_group_id, p_member_id, p_topic, NOW() AT TIME ZONE 'UTC', NOW() AT TIME ZONE 'UTC')
        ON CONFLICT (group_id, member_id, topic_name)
        DO UPDATE SET last_heartbeat = NOW() AT TIME ZONE 'UTC';

        -- If this is a new member joining, trigger rebalance
        IF NOT existing_member THEN
            membership_changed := TRUE;
        END IF;

        -- If membership changed, increment generation and reassign
        IF membership_changed THEN
            INSERT INTO postka_group_generation (group_id, topic_name, generation_id, last_rebalance)
            VALUES (p_group_id, p_topic, 1, NOW() AT TIME ZONE 'UTC')
            ON CONFLICT (group_id, topic_name)
            DO UPDATE SET generation_id = postka_group_generation.generation_id + 1,
                          last_rebalance = NOW() AT TIME ZONE 'UTC';

            -- Reassign partitions
            PERFORM postka_assign_partitions(p_group_id, p_topic);
        END IF;

        -- Get current generation
        SELECT generation_id INTO current_gen
        FROM postka_group_generation
        WHERE group_id = p_group_id AND topic_name = p_topic;

        IF current_gen IS NULL THEN
            current_gen := 0;
        END IF;

        -- Get assigned partitions for this member
        SELECT current_gen,
               membership_changed,
               COALESCE(array_agg(partition_id ORDER BY partition_id), ARRAY[]::INTEGER[])
        INTO result
        FROM postka_partition_assignments
        WHERE group_id = p_group_id AND topic_name = p_topic AND member_id = p_member_id;

        -- Release the lock
        PERFORM pg_advisory_unlock(lock_id);

        RETURN result;
    EXCEPTION WHEN OTHERS THEN
        -- Make sure to release lock on error
        PERFORM pg_advisory_unlock(lock_id);
        RAISE;
    END;
END;
$$ LANGUAGE plpgsql;

-- Function for a consumer to leave a group
CREATE OR REPLACE FUNCTION postka_leave_group(
    p_group_id TEXT,
    p_member_id TEXT,
    p_topic TEXT
)
RETURNS VOID AS $$
DECLARE
    lock_id BIGINT;
BEGIN
    -- Compute lock ID from group_id and topic
    lock_id := hashtext(p_group_id || '::' || p_topic);

    -- Acquire advisory lock for this group/topic combination
    PERFORM pg_advisory_lock(lock_id);

    BEGIN
        -- Remove member from group
        DELETE FROM postka_consumer_group_members
        WHERE group_id = p_group_id AND member_id = p_member_id AND topic_name = p_topic;

        -- Remove partition assignments for this member
        DELETE FROM postka_partition_assignments
        WHERE group_id = p_group_id AND member_id = p_member_id AND topic_name = p_topic;

        -- Increment generation and reassign partitions
        INSERT INTO postka_group_generation (group_id, topic_name, generation_id, last_rebalance)
        VALUES (p_group_id, p_topic, 1, NOW() AT TIME ZONE 'UTC')
        ON CONFLICT (group_id, topic_name)
        DO UPDATE SET generation_id = postka_group_generation.generation_id + 1,
                      last_rebalance = NOW() AT TIME ZONE 'UTC';

        -- Reassign partitions among remaining members
        PERFORM postka_assign_partitions(p_group_id, p_topic);

        -- Release the lock
        PERFORM pg_advisory_unlock(lock_id);
    EXCEPTION WHEN OTHERS THEN
        -- Make sure to release lock on error
        PERFORM pg_advisory_unlock(lock_id);
        RAISE;
    END;
END;
$$ LANGUAGE plpgsql;

-- Function to get current generation and assigned partitions for a member
CREATE OR REPLACE FUNCTION postka_get_assignment(
    p_group_id TEXT,
    p_member_id TEXT,
    p_topic TEXT
)
RETURNS postka_join_result AS $$
DECLARE
    current_gen INTEGER;
    result postka_join_result;
BEGIN
    -- Get current generation
    SELECT generation_id INTO current_gen
    FROM postka_group_generation
    WHERE group_id = p_group_id AND topic_name = p_topic;

    IF current_gen IS NULL THEN
        current_gen := 0;
    END IF;

    -- Get assigned partitions for this member
    SELECT current_gen,
           FALSE,
           COALESCE(array_agg(partition_id ORDER BY partition_id), ARRAY[]::INTEGER[])
    INTO result
    FROM postka_partition_assignments
    WHERE group_id = p_group_id AND topic_name = p_topic AND member_id = p_member_id;

    RETURN result;
END;
$$ LANGUAGE plpgsql;
