-- Initialization for the device fault log platform (runs once on an empty data dir).
--
-- 1) device_fault_logs: hot table that keeps receiving writes while the index
--    is being built online.
-- 2) index_build_jobs:  ledger used by the stateless API instances to
--    coordinate online index builds (single-winner claim + fencing).

CREATE TABLE IF NOT EXISTS device_fault_logs (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    device_id   TEXT        NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    severity    TEXT        NOT NULL DEFAULT 'error',
    message     TEXT        NOT NULL
);

-- Preload historical fault logs so that the online index build is non-trivial.
INSERT INTO device_fault_logs (device_id, occurred_at, severity, message)
SELECT 'dev-' || lpad((floor(random() * 2000))::int::text, 5, '0'),
       now() - (random() * interval '180 days'),
       (ARRAY['info', 'warn', 'error', 'critical'])[floor(random() * 4 + 1)],
       'preloaded fault event #' || g
FROM generate_series(1, 4000000) AS g;

-- One row per managed index. The UNIQUE constraint on index_name is what
-- guarantees that concurrent build requests across all API instances produce
-- exactly one claimed build. heartbeat_at is refreshed by the claiming
-- instance while it is alive; a 'building' claim may only be recycled once
-- the heartbeat has expired AND no build backend is active AND no build
-- thread holds the build advisory lock (see app/index_manager.py).
CREATE TABLE IF NOT EXISTS index_build_jobs (
    id           UUID PRIMARY KEY,
    index_name   TEXT        NOT NULL UNIQUE,
    status       TEXT        NOT NULL CHECK (status IN ('building', 'ready', 'failed')),
    builder      TEXT,
    error        TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Runtime-tunable knobs. Currently used by the self-test to inject a
-- deterministic pre-DDL stall (key 'pre_ddl_delay_seconds').
CREATE TABLE IF NOT EXISTS index_build_controls (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

VACUUM ANALYZE device_fault_logs;
