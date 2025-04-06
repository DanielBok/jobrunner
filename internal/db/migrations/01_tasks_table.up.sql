CREATE SCHEMA task;

-- Definition table to store task definitions
CREATE TABLE task.definition
(
    id              BIGSERIAL PRIMARY KEY,
    name            VARCHAR(255)             NOT NULL,
    description     TEXT,
    image_name      TEXT,
    command         TEXT                     NOT NULL,
    timeout_seconds INT                      NOT NULL DEFAULT 3600,
    max_retries     INT                      NOT NULL DEFAULT 0,
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    is_active       BOOLEAN                  NOT NULL DEFAULT TRUE
);

CREATE TYPE task.REQUIRED_CONDITION AS ENUM ('success', 'completion', 'failure', 'cancelled', 'lapsed');

-- Task dependencies table
CREATE TABLE task.dependency
(
    id                 BIGSERIAL PRIMARY KEY,
    task_id            BIGINT                   NOT NULL REFERENCES task.definition (id) ON DELETE CASCADE,
    depends_on         BIGINT                   NOT NULL REFERENCES task.definition (id) ON DELETE CASCADE,
    lookback_window    INT                      NOT NULL DEFAULT 86400,
    min_wait_time      INT                      NOT NULL DEFAULT 0, -- Minimum wait time (seconds) after parent dependency met
    required_condition task.REQUIRED_CONDITION  NOT NULL DEFAULT 'success',
    created_at         TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dependency UNIQUE (task_id, depends_on),
    CONSTRAINT ck_prevent_self_dependency CHECK (task_id != depends_on)
);

-- Index for faster lookups
CREATE INDEX idx_dependency_task ON task.dependency (task_id);
CREATE INDEX idx_dependency_parent_task ON task.dependency (depends_on);

-- Schedules table to store when tasks should run
CREATE TABLE task.schedule
(
    id              BIGSERIAL PRIMARY KEY,
    task_id         BIGINT REFERENCES task.definition (id) ON DELETE CASCADE,
    cron_expression VARCHAR(100)             NOT NULL, -- Cron expression for flexible scheduling
    timezone        VARCHAR(50)              NOT NULL DEFAULT 'UTC',
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TYPE task.RUN_STATUS AS ENUM ('pending', 'running', 'completed', 'failed', 'cancelled', 'lapsed');

-- Run is an instantiation of a task Definition
CREATE TABLE task.run
(
    id             BIGSERIAL PRIMARY KEY,
    task_id         BIGINT REFERENCES task.definition (id) ON DELETE CASCADE,
    status         task.RUN_STATUS          NOT NULL DEFAULT 'pending',
    start_time     TIMESTAMP WITH TIME ZONE,
    end_time       TIMESTAMP WITH TIME ZONE,
    exit_code      INT                      NOT NULL DEFAULT -1,
    output         TEXT,
    error          TEXT,
    attempts       INT                      NOT NULL DEFAULT 0,
    worker_id      TEXT,
    last_heartbeat TIMESTAMP WITH TIME ZONE,
    created_at     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Create indexes for performance
CREATE INDEX idx_execution_task_id ON task.run (task_id);
CREATE INDEX idx_execution_status ON task.run (status);
CREATE INDEX idx_schedule_task_id ON task.schedule (task_id);


-- Triggers to auto-update updated_at

-- 1. Create trigger function for updated_at timestamp (schema-level function)
CREATE OR REPLACE FUNCTION task.update_timestamp()
    RETURNS TRIGGER AS
$$
BEGIN
    new.updated_at = NOW();
    RETURN new;
END;
$$ LANGUAGE plpgsql;

-- 2. Apply triggers to the task table
CREATE TRIGGER set_updated_at_task
    BEFORE UPDATE
    ON task.definition
    FOR EACH ROW
EXECUTE FUNCTION task.update_timestamp();

-- 3. Apply triggers to the dependency table
CREATE TRIGGER set_updated_at_dependency
    BEFORE UPDATE
    ON task.dependency
    FOR EACH ROW
EXECUTE FUNCTION task.update_timestamp();

-- 4. Apply triggers to the schedule table
CREATE TRIGGER set_updated_at_schedule
    BEFORE UPDATE
    ON task.schedule
    FOR EACH ROW
EXECUTE FUNCTION task.update_timestamp();
