-- 002: Revenue tracking
-- Adds granular revenue storage with type/source/currency dimensions

BEGIN;

-- Revenue type enum (YouTube-style breakdown)
CREATE TYPE analytics.revenue_type AS ENUM (
    'auction',           -- AdSense auction ads
    'reserved',          -- YouTube-sold / DoubleClick ads
    'ugc',               -- Revenue from user-generated content claims
    'pro_rata',          -- YouTube Premium / subscription share
    'art_track',         -- Art track-specific revenue
    'shorts',            -- Shorts monetization
    'audio_tier',        -- YouTube Audio Tier (greater-of calculation)
    'subscription',      -- Platform subscription revenue (Apple, Amazon, etc.)
    'download_purchase', -- Paid downloads
    'ad_supported',      -- Generic ad-supported revenue
    'other'
);

-- Revenue source enum (high-level categorization)
CREATE TYPE analytics.revenue_source AS ENUM (
    'ads',
    'subscriptions',
    'transactions',
    'other'
);

-- Revenue fact table
CREATE TABLE analytics.revenue (
    id              BIGSERIAL PRIMARY KEY,
    asset_id        UUID NOT NULL REFERENCES public.assets(id),
    platform_id     TEXT NOT NULL REFERENCES public.platforms(id),
    territory       CHAR(2),
    revenue_date    DATE NOT NULL,
    revenue_type    analytics.revenue_type NOT NULL,
    revenue_source  analytics.revenue_source,
    currency        CHAR(3) NOT NULL DEFAULT 'USD',
    amount          NUMERIC(18,6) NOT NULL,
    amount_usd      NUMERIC(18,6),
    content_type    TEXT,           -- 'art_track', 'music_video', 'ugc', etc.
    external_id     TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (asset_id, platform_id, territory, revenue_date, revenue_type)
);

-- Indexes for common query patterns
CREATE INDEX idx_revenue_asset_date ON analytics.revenue (asset_id, revenue_date DESC);
CREATE INDEX idx_revenue_platform_date ON analytics.revenue (platform_id, revenue_date DESC);
CREATE INDEX idx_revenue_date ON analytics.revenue (revenue_date DESC);
CREATE INDEX idx_revenue_type_date ON analytics.revenue (revenue_type, revenue_date DESC);
CREATE INDEX idx_revenue_territory ON analytics.revenue (territory, revenue_date DESC) WHERE territory IS NOT NULL;

COMMIT;
