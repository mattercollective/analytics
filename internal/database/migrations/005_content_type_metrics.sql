-- 005: Art track / music video metric types
-- Extends the metric_type enum for YouTube Music content type breakdowns

BEGIN;

ALTER TYPE analytics.metric_type ADD VALUE IF NOT EXISTS 'views_art_track';
ALTER TYPE analytics.metric_type ADD VALUE IF NOT EXISTS 'streams_art_track';
ALTER TYPE analytics.metric_type ADD VALUE IF NOT EXISTS 'watch_time_art_track';
ALTER TYPE analytics.metric_type ADD VALUE IF NOT EXISTS 'views_music_video';
ALTER TYPE analytics.metric_type ADD VALUE IF NOT EXISTS 'revenue_art_track';
ALTER TYPE analytics.metric_type ADD VALUE IF NOT EXISTS 'revenue_music_video';

COMMIT;
