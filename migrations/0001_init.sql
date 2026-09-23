CREATE FUNCTION pgspawn.set_updated_at ()
    RETURNS TRIGGER
    AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$
LANGUAGE plpgsql;

CREATE TABLE pgspawn.jobs (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid (),
    name text NOT NULL,
    status text NOT NULL,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    queue_name text,
    priority integer NOT NULL DEFAULT 0,
    run_at timestamptz NOT NULL DEFAULT now(),
    job_key text,
    attempt integer NOT NULL DEFAULT 0,
    max_attempts integer NOT NULL DEFAULT 25,
    locked_by text,
    locked_at timestamptz,
    error text,
    queued_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz,
    finished_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT jobs_status_check CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')),
    CONSTRAINT jobs_attempt_check CHECK (attempt >= 0),
    CONSTRAINT jobs_max_attempts_check CHECK (max_attempts > 0)
);

CREATE INDEX jobs_available_idx ON pgspawn.jobs (priority, run_at, created_at)
    WHERE status = 'queued';

CREATE INDEX jobs_name_created_at_idx ON pgspawn.jobs (name, created_at DESC);

CREATE INDEX jobs_locked_idx ON pgspawn.jobs (locked_by, locked_at)
    WHERE status = 'running';

CREATE UNIQUE INDEX jobs_queue_running_idx ON pgspawn.jobs (queue_name)
    WHERE status = 'running' AND queue_name IS NOT NULL;

CREATE UNIQUE INDEX jobs_job_key_active_idx ON pgspawn.jobs (job_key)
    WHERE status IN ('queued', 'running') AND job_key IS NOT NULL;

CREATE INDEX jobs_finished_idx ON pgspawn.jobs (finished_at)
    WHERE status IN ('succeeded', 'failed', 'cancelled') AND finished_at IS NOT NULL;

CREATE TRIGGER _100__timestamps
    BEFORE INSERT OR UPDATE ON pgspawn.jobs
    FOR EACH ROW
    EXECUTE FUNCTION pgspawn.set_updated_at ();

-- Notifications only signal queue availability, so an empty payload lets PostgreSQL fold identical notifications within a transaction.
CREATE FUNCTION pgspawn.notify_jobs ()
    RETURNS TRIGGER
    AS $$
BEGIN
    PERFORM pg_notify('pgspawn_jobs', '');
    RETURN NULL;
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER _900_notify
    AFTER INSERT OR UPDATE OF status, run_at ON pgspawn.jobs
    FOR EACH ROW
    WHEN (NEW.status = 'queued')
    EXECUTE FUNCTION pgspawn.notify_jobs ();

-- A terminal job releases its named queue for another job.
CREATE TRIGGER _910_notify_queue
    AFTER UPDATE OF status ON pgspawn.jobs
    FOR EACH ROW
    WHEN (OLD.status = 'running' AND NEW.status IN ('succeeded', 'failed', 'cancelled') AND OLD.queue_name IS NOT NULL)
    EXECUTE FUNCTION pgspawn.notify_jobs ();

CREATE TABLE pgspawn.workers (
    id text PRIMARY KEY,
    task_names text[] NOT NULL,
    started_at timestamptz NOT NULL DEFAULT now(),
    last_heartbeat_at timestamptz NOT NULL DEFAULT now(),
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TRIGGER _100__timestamps
    BEFORE INSERT OR UPDATE ON pgspawn.workers
    FOR EACH ROW
    EXECUTE FUNCTION pgspawn.set_updated_at ();

CREATE TABLE pgspawn.crons (
    identifier text PRIMARY KEY,
    name text NOT NULL,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    last_run_at timestamptz,
    next_run_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TRIGGER _100__timestamps
    BEFORE INSERT OR UPDATE ON pgspawn.crons
    FOR EACH ROW
    EXECUTE FUNCTION pgspawn.set_updated_at ();
