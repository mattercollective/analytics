-- 004: Playlist tracking (Chartmetric-style)
-- Tracks which playlists songs are on, position changes, and follower history

BEGIN;

-- Playlist metadata
CREATE TABLE analytics.playlists (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    platform_id     TEXT NOT NULL REFERENCES public.platforms(id),
    external_id     TEXT NOT NULL,           -- Spotify URI, Apple playlist ID, YT playlist ID
    name            TEXT NOT NULL,
    description     TEXT,
    curator_name    TEXT,
    curator_type    TEXT,                     -- 'editorial', 'algorithmic', 'user', 'label'
    follower_count  BIGINT,
    track_count     INT,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    image_url       TEXT,
    platform_url    TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (platform_id, external_id)
);

CREATE INDEX idx_playlists_platform ON analytics.playlists (platform_id);
CREATE INDEX idx_playlists_curator_type ON analytics.playlists (curator_type) WHERE curator_type IS NOT NULL;

-- Daily position snapshots: which songs are on which playlists and where
CREATE TABLE analytics.playlist_positions (
    id              BIGSERIAL PRIMARY KEY,
    playlist_id     UUID NOT NULL REFERENCES analytics.playlists(id),
    asset_id        UUID NOT NULL REFERENCES public.assets(id),
    snapshot_date   DATE NOT NULL,
    position        INT,                     -- 1-indexed track position, NULL if unknown
    added_date      DATE,                    -- when the track was first seen on this playlist
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (playlist_id, asset_id, snapshot_date)
);

CREATE INDEX idx_playlist_positions_asset ON analytics.playlist_positions (asset_id, snapshot_date DESC);
CREATE INDEX idx_playlist_positions_playlist ON analytics.playlist_positions (playlist_id, snapshot_date DESC);

-- Which playlists to actively poll
CREATE TABLE analytics.tracked_playlists (
    id              SERIAL PRIMARY KEY,
    playlist_id     UUID NOT NULL REFERENCES analytics.playlists(id),
    client_id       UUID REFERENCES public.clients(id),  -- NULL = track for all clients
    added_by        TEXT,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (playlist_id, client_id)
);

-- Follower count history for trend analysis
CREATE TABLE analytics.playlist_followers (
    playlist_id     UUID NOT NULL REFERENCES analytics.playlists(id),
    snapshot_date   DATE NOT NULL,
    follower_count  BIGINT NOT NULL,

    PRIMARY KEY (playlist_id, snapshot_date)
);

COMMIT;
