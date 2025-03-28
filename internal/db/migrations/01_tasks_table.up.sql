CREATE SCHEMA tasks;

-- Jobs table to store job definitions
CREATE TABLE tasks.job
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

-- Job dependencies table
CREATE TABLE tasks.dependency
(
    id                 BIGSERIAL PRIMARY KEY,
    job_id             BIGINT                   NOT NULL REFERENCES tasks.job (id) ON DELETE CASCADE,
    depends_on         BIGINT                   NOT NULL REFERENCES tasks.job (id) ON DELETE CASCADE,
    lookback_window    INT                      NOT NULL DEFAULT 86400,
    min_wait_time      INT                      NOT NULL DEFAULT 0, -- Minimum wait time (seconds) after parent dependency met
    required_condition VARCHAR(20)              NOT NULL DEFAULT 'success' CHECK (required_condition IN ('success', 'completion', 'failure')),
    created_at         TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dependency UNIQUE (job_id, depends_on),
    CONSTRAINT ck_prevent_self_dependency CHECK (job_id != depends_on)
);

-- Index for faster lookups
CREATE INDEX idx_dependency_job ON tasks.dependency (job_id);
CREATE INDEX idx_dependency_parent_job ON tasks.dependency (depends_on);

-- Schedules table to store when jobs should run
CREATE TABLE tasks.schedule
(
    id              BIGSERIAL PRIMARY KEY,
    job_id          BIGINT REFERENCES tasks.job (id) ON DELETE CASCADE,
    cron_expression VARCHAR(100)             NOT NULL, -- Cron expression for flexible scheduling
    timezone        VARCHAR(50)              NOT NULL DEFAULT 'UTC',
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Job executions to track history
CREATE TABLE tasks.execution
(
    id               BIGSERIAL PRIMARY KEY,
    job_id           BIGINT REFERENCES tasks.job (id) ON DELETE CASCADE,
    status           VARCHAR(50)              NOT NULL CHECK (status IN ('pending', 'running', 'completed', 'failed')),
    start_time       TIMESTAMP WITH TIME ZONE,
    end_time         TIMESTAMP WITH TIME ZONE,
    exit_code        INT                      NOT NULL DEFAULT -1,
    output           TEXT,
    error            TEXT,
    attempts         INT                      NOT NULL DEFAULT 0,
    dependencies_met BOOLEAN                  NOT NULL DEFAULT FALSE,
    worker_id        VARCHAR(100),
    created_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Create indexes for performance
CREATE INDEX idx_execution_job_id ON tasks.execution (job_id);
CREATE INDEX idx_execution_status ON tasks.execution (status);
CREATE INDEX idx_schedule_job_id ON tasks.schedule (job_id);


-- Triggers to auto-update updated_at

-- 1. Create trigger function for updated_at timestamp (schema-level function)
CREATE OR REPLACE FUNCTION tasks.update_timestamp()
    RETURNS TRIGGER AS
$$
BEGIN
    new.updated_at = NOW();
    RETURN new;
END;
$$ LANGUAGE plpgsql;

-- 2. Apply triggers to the job table
CREATE TRIGGER set_updated_at_job
    BEFORE UPDATE
    ON tasks.job
    FOR EACH ROW
EXECUTE FUNCTION tasks.update_timestamp();

-- 3. Apply triggers to the dependency table
CREATE TRIGGER set_updated_at_dependency
    BEFORE UPDATE
    ON tasks.dependency
    FOR EACH ROW
EXECUTE FUNCTION tasks.update_timestamp();

-- 4. Apply triggers to the schedule table
CREATE TRIGGER set_updated_at_schedule
    BEFORE UPDATE
    ON tasks.schedule
    FOR EACH ROW
EXECUTE FUNCTION tasks.update_timestamp();
