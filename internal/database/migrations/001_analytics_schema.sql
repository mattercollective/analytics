-- ============================================================
-- ANALYTICS SCHEMA
-- Separate namespace so analytics tables don't collide with
-- the royalty system tables in the public schema.
-- ============================================================
CREATE SCHEMA IF NOT EXISTS analytics;

-- -------------------------------------------------------
-- Metric type taxonomy
-- -------------------------------------------------------
CREATE TYPE analytics.metric_type AS ENUM (
    'streams',
    'views',
    'downloads',
    'saves',
    'playlist_adds',
    'playlist_removes',
    'followers',
    'listeners',
    'shazams',
    'sound_uses',
    'watch_time_hours',
    'impressions',
    'likes',
    'shares',
    'comments',
    'content_id_claims',
    'revenue_estimate'
);

-- -------------------------------------------------------
-- analytics.metrics — Core fact table
-- One row per (asset, platform, territory, date, metric_type).
-- Granularity: daily.
-- -------------------------------------------------------
CREATE TABLE analytics.metrics (
    id              BIGSERIAL PRIMARY KEY,
    asset_id        UUID NOT NULL REFERENCES public.assets(id),
    platform_id     TEXT NOT NULL REFERENCES public.platforms(id),
    territory       CHAR(2),
    metric_date     DATE NOT NULL,
    metric_type     analytics.metric_type NOT NULL,
    value           BIGINT NOT NULL,
    value_decimal   NUMERIC(18,6),
    external_id     TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_analytics_metric
        UNIQUE (asset_id, platform_id, territory, metric_date, metric_type)
);

CREATE INDEX idx_am_asset_date      ON analytics.metrics (asset_id, metric_date DESC);
CREATE INDEX idx_am_platform_date   ON analytics.metrics (platform_id, metric_date DESC);
CREATE INDEX idx_am_date            ON analytics.metrics (metric_date DESC);
CREATE INDEX idx_am_asset_platform  ON analytics.metrics (asset_id, platform_id, metric_type, metric_date DESC);
CREATE INDEX idx_am_territory       ON analytics.metrics (territory, metric_date DESC) WHERE territory IS NOT NULL;

-- -------------------------------------------------------
-- analytics.sync_state — Incremental sync cursor per platform
-- -------------------------------------------------------
CREATE TABLE analytics.sync_state (
    id              SERIAL PRIMARY KEY,
    platform_id     TEXT NOT NULL REFERENCES public.platforms(id),
    scope           TEXT NOT NULL DEFAULT 'all',
    last_sync_at    TIMESTAMPTZ,
    last_data_date  DATE,
    cursor_token    TEXT,
    backfill_start  DATE,
    backfill_complete BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_sync_state UNIQUE (platform_id, scope)
);

-- -------------------------------------------------------
-- analytics.sync_runs — Audit log of every sync execution
-- -------------------------------------------------------
CREATE TABLE analytics.sync_runs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    platform_id     TEXT NOT NULL REFERENCES public.platforms(id),
    scope           TEXT NOT NULL DEFAULT 'all',
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ,
    status          TEXT NOT NULL DEFAULT 'running'
                        CHECK (status IN ('running', 'success', 'partial', 'error')),
    rows_fetched    INT DEFAULT 0,
    rows_inserted   INT DEFAULT 0,
    rows_updated    INT DEFAULT 0,
    data_date_min   DATE,
    data_date_max   DATE,
    error_message   TEXT,
    error_count     INT DEFAULT 0,
    api_calls_made  INT DEFAULT 0,
    rate_limit_waits INT DEFAULT 0
);

CREATE INDEX idx_sr_platform ON analytics.sync_runs (platform_id, started_at DESC);
CREATE INDEX idx_sr_status   ON analytics.sync_runs (status) WHERE status != 'success';

-- -------------------------------------------------------
-- analytics.platform_credentials — API credentials per platform
-- Secrets should reference GCP Secret Manager in production.
-- -------------------------------------------------------
CREATE TABLE analytics.platform_credentials (
    id              SERIAL PRIMARY KEY,
    platform_id     TEXT NOT NULL REFERENCES public.platforms(id),
    credential_type TEXT NOT NULL,
    client_id       TEXT,
    client_secret   TEXT,
    private_key     TEXT,
    key_id          TEXT,
    team_id         TEXT,
    service_account_json TEXT,
    additional_config JSONB,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_platform_cred UNIQUE (platform_id, credential_type)
);

-- -------------------------------------------------------
-- analytics.client_assets — Maps clients to assets for multi-tenancy
-- Bridge table until royalty system convergence is complete.
-- -------------------------------------------------------
CREATE TABLE analytics.client_assets (
    client_id       UUID NOT NULL REFERENCES public.clients(id),
    asset_id        UUID NOT NULL REFERENCES public.assets(id),
    PRIMARY KEY (client_id, asset_id)
);

CREATE INDEX idx_ca_client ON analytics.client_assets (client_id);
CREATE INDEX idx_ca_asset  ON analytics.client_assets (asset_id);

-- -------------------------------------------------------
-- Materialized view: daily aggregates collapsed across territory
-- -------------------------------------------------------
CREATE MATERIALIZED VIEW analytics.asset_platform_daily AS
SELECT
    asset_id,
    platform_id,
    metric_date,
    metric_type,
    SUM(value) AS total_value,
    SUM(value_decimal) AS total_value_decimal,
    COUNT(DISTINCT territory) AS territory_count
FROM analytics.metrics
GROUP BY asset_id, platform_id, metric_date, metric_type;

CREATE UNIQUE INDEX idx_apd_unique ON analytics.asset_platform_daily
    (asset_id, platform_id, metric_date, metric_type);
