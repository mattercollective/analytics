-- 003: Artist metadata on assets
-- Adds artist_name column for byArtist aggregation

BEGIN;

ALTER TABLE public.assets ADD COLUMN IF NOT EXISTS artist_name TEXT;

CREATE INDEX IF NOT EXISTS idx_assets_artist_name
    ON public.assets (artist_name)
    WHERE artist_name IS NOT NULL;

COMMIT;
