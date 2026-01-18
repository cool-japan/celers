-- CeleRS Table Partitioning Support
-- This migration adds support for table partitioning for large-scale deployments
--
-- WARNING: This migration is optional and only needed for very large deployments.
-- Do NOT run this on existing databases with data - it requires table recreation.
-- For existing deployments, use manual partitioning conversion or export/import data.
--
-- Partitioning strategy: Range partitioning by created_at (monthly partitions)
-- Benefits:
-- - Faster queries on recent tasks
-- - Easy archiving by dropping old partitions
-- - Reduced table bloat
-- - Better VACUUM performance

-- Function to create monthly partitions for tasks table
CREATE OR REPLACE FUNCTION create_tasks_partition(
    partition_date DATE
) RETURNS TEXT AS $$
DECLARE
    partition_name TEXT;
    start_date DATE;
    end_date DATE;
BEGIN
    -- Calculate partition bounds (monthly)
    start_date := DATE_TRUNC('month', partition_date);
    end_date := start_date + INTERVAL '1 month';

    -- Generate partition name (e.g., celers_tasks_2026_01)
    partition_name := 'celers_tasks_' || TO_CHAR(start_date, 'YYYY_MM');

    -- Check if partition already exists
    IF EXISTS (
        SELECT 1 FROM pg_class
        WHERE relname = partition_name
    ) THEN
        RETURN partition_name || ' already exists';
    END IF;

    -- Create partition
    EXECUTE format(
        'CREATE TABLE %I PARTITION OF celers_tasks
         FOR VALUES FROM (%L) TO (%L)',
        partition_name,
        start_date,
        end_date
    );

    RETURN 'Created partition: ' || partition_name;
END;
$$ LANGUAGE plpgsql;

-- Function to create partitions for a date range
CREATE OR REPLACE FUNCTION create_tasks_partitions_range(
    start_date DATE,
    end_date DATE
) RETURNS TABLE(partition_name TEXT, status TEXT) AS $$
DECLARE
    current_date DATE;
BEGIN
    current_date := DATE_TRUNC('month', start_date);

    WHILE current_date <= end_date LOOP
        partition_name := 'celers_tasks_' || TO_CHAR(current_date, 'YYYY_MM');
        status := create_tasks_partition(current_date);
        RETURN NEXT;

        current_date := current_date + INTERVAL '1 month';
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Function to drop old partitions (for archiving)
CREATE OR REPLACE FUNCTION drop_tasks_partition(
    partition_date DATE
) RETURNS TEXT AS $$
DECLARE
    partition_name TEXT;
    start_date DATE;
BEGIN
    start_date := DATE_TRUNC('month', partition_date);
    partition_name := 'celers_tasks_' || TO_CHAR(start_date, 'YYYY_MM');

    -- Check if partition exists
    IF NOT EXISTS (
        SELECT 1 FROM pg_class
        WHERE relname = partition_name
    ) THEN
        RETURN partition_name || ' does not exist';
    END IF;

    -- Drop partition
    EXECUTE format('DROP TABLE %I', partition_name);

    RETURN 'Dropped partition: ' || partition_name;
END;
$$ LANGUAGE plpgsql;

-- Function to list all task partitions
CREATE OR REPLACE FUNCTION list_tasks_partitions()
RETURNS TABLE(
    partition_name TEXT,
    partition_start DATE,
    partition_end DATE,
    row_count BIGINT,
    size_bytes BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        c.relname::TEXT AS partition_name,
        DATE_TRUNC('month', TO_DATE(SUBSTRING(c.relname FROM 'celers_tasks_(\d{4}_\d{2})'), 'YYYY_MM'))::DATE AS partition_start,
        (DATE_TRUNC('month', TO_DATE(SUBSTRING(c.relname FROM 'celers_tasks_(\d{4}_\d{2})'), 'YYYY_MM')) + INTERVAL '1 month')::DATE AS partition_end,
        pg_stat_get_live_tuples(c.oid) AS row_count,
        pg_relation_size(c.oid) AS size_bytes
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname LIKE 'celers_tasks_%'
    AND c.relname ~ 'celers_tasks_\d{4}_\d{2}'
    AND n.nspname = 'public'
    ORDER BY partition_start DESC;
END;
$$ LANGUAGE plpgsql;

-- Function to get partition for a specific date
CREATE OR REPLACE FUNCTION get_tasks_partition_name(task_date DATE)
RETURNS TEXT AS $$
BEGIN
    RETURN 'celers_tasks_' || TO_CHAR(DATE_TRUNC('month', task_date), 'YYYY_MM');
END;
$$ LANGUAGE plpgsql;

-- Function to convert existing celers_tasks table to partitioned table
-- WARNING: This requires downtime and will lock the table during conversion
-- Only use on small tables or during maintenance window
CREATE OR REPLACE FUNCTION convert_tasks_to_partitioned()
RETURNS TEXT AS $$
DECLARE
    min_date DATE;
    max_date DATE;
BEGIN
    -- Get date range from existing data
    SELECT
        DATE_TRUNC('month', MIN(created_at))::DATE,
        DATE_TRUNC('month', MAX(created_at))::DATE
    INTO min_date, max_date
    FROM celers_tasks;

    -- If table is empty, just convert structure
    IF min_date IS NULL THEN
        -- Rename existing table
        ALTER TABLE celers_tasks RENAME TO celers_tasks_old;

        -- Create new partitioned table
        CREATE TABLE celers_tasks (
            LIKE celers_tasks_old INCLUDING ALL
        ) PARTITION BY RANGE (created_at);

        -- Drop old table
        DROP TABLE celers_tasks_old;

        RETURN 'Converted empty table to partitioned table';
    END IF;

    -- For non-empty tables, return instructions
    RETURN 'ERROR: Table contains data. Manual conversion required:
    1. Stop all workers
    2. Export data: pg_dump -t celers_tasks
    3. Drop and recreate as partitioned
    4. Create partitions for date range
    5. Import data
    See documentation for detailed steps.';
END;
$$ LANGUAGE plpgsql;

-- Create a default partition for tasks outside the partition range (optional)
-- This prevents errors if tasks are enqueued with timestamps outside partition bounds
-- Uncomment if you want to enable default partition:
-- CREATE TABLE IF NOT EXISTS celers_tasks_default PARTITION OF celers_tasks DEFAULT;

-- Maintenance function to automatically create future partitions
CREATE OR REPLACE FUNCTION maintain_tasks_partitions(
    months_ahead INTEGER DEFAULT 3
) RETURNS TEXT AS $$
DECLARE
    current_date DATE;
    end_date DATE;
    result TEXT;
BEGIN
    current_date := DATE_TRUNC('month', NOW());
    end_date := current_date + (months_ahead || ' months')::INTERVAL;

    -- Create partitions for current month + months_ahead
    PERFORM create_tasks_partitions_range(current_date, end_date);

    RETURN format('Ensured partitions exist from %s to %s', current_date, end_date);
END;
$$ LANGUAGE plpgsql;

-- Notes:
-- 1. To enable partitioning on a new database:
--    - Modify 001_init.sql to create celers_tasks as partitioned table
--    - Add: PARTITION BY RANGE (created_at) after table definition
--    - Run: SELECT create_tasks_partitions_range(NOW()::DATE - INTERVAL '1 month', NOW()::DATE + INTERVAL '6 months');
--
-- 2. To maintain partitions automatically:
--    - Call: SELECT maintain_tasks_partitions(3); periodically (e.g., daily via cron)
--    - Or use the Rust method: maintain_partitions(months_ahead)
--
-- 3. To archive old data:
--    - Detach partition: ALTER TABLE celers_tasks DETACH PARTITION celers_tasks_2024_01;
--    - Archive to separate storage
--    - Drop: SELECT drop_tasks_partition('2024-01-01'::DATE);
--
-- 4. Performance considerations:
--    - Partition pruning works best with WHERE created_at clauses
--    - Each partition has its own indexes (automatically created)
--    - Keep partition count reasonable (12-36 partitions typical)
--    - Monitor partition sizes with: SELECT * FROM list_tasks_partitions();
