-- 006: Engagement and demographics
-- Per-source behavioral data (playlist vs radio vs search) and age/gender breakdowns

BEGIN;

-- Engagement by source: where listeners find and interact with content
CREATE TABLE analytics.engagement (
    id                BIGSERIAL PRIMARY KEY,
    asset_id          UUID NOT NULL REFERENCES public.assets(id),
    platform_id       TEXT NOT NULL REFERENCES public.platforms(id),
    territory         CHAR(2),
    engagement_date   DATE NOT NULL,
    source            TEXT NOT NULL,          -- 'playlist', 'radio', 'search', 'album', 'artist_page', 'browse', 'other'
    source_uri        TEXT,                   -- e.g., Spotify playlist URI
    streams           BIGINT NOT NULL DEFAULT 0,
    saves             BIGINT NOT NULL DEFAULT 0,
    skips             BIGINT NOT NULL DEFAULT 0,
    completions       BIGINT NOT NULL DEFAULT 0,
    discovery         BIGINT NOT NULL DEFAULT 0,  -- first-listen count
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (asset_id, platform_id, territory, engagement_date, source)
);

CREATE INDEX idx_engagement_asset_date ON analytics.engagement (asset_id, engagement_date DESC);
CREATE INDEX idx_engagement_platform_date ON analytics.engagement (platform_id, engagement_date DESC);
CREATE INDEX idx_engagement_source ON analytics.engagement (source, engagement_date DESC);

-- Demographics: age and gender breakdown of listeners/streamers
CREATE TABLE analytics.demographics (
    id              BIGSERIAL PRIMARY KEY,
    asset_id        UUID NOT NULL REFERENCES public.assets(id),
    platform_id     TEXT NOT NULL REFERENCES public.platforms(id),
    territory       CHAR(2),
    demo_date       DATE NOT NULL,
    age_bucket      TEXT NOT NULL,            -- '13-17', '18-24', '25-34', '35-44', '45-54', '55-64', '65+'
    gender          TEXT NOT NULL,            -- 'male', 'female', 'non_binary', 'unknown'
    streams         BIGINT NOT NULL DEFAULT 0,
    listeners       BIGINT NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (asset_id, platform_id, territory, demo_date, age_bucket, gender)
);

CREATE INDEX idx_demographics_asset_date ON analytics.demographics (asset_id, demo_date DESC);
CREATE INDEX idx_demographics_platform_date ON analytics.demographics (platform_id, demo_date DESC);

COMMIT;
