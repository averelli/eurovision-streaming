CREATE SCHEMA live_data;

CREATE TABLE window_aggregates (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,

    window_posts INTEGER NOT NULL,
    total_posts INTEGER NOT NULL,

    window_sentiment FLOAT NOT NULL,
    total_sentiment FLOAT NOT NULL,
    window_avg_sentiment FLOAT NOT NULL,
    total_avg_sentiment FLOAT NOT NULL,

    window_platform_counter JSONB NOT NULL,
    total_platform_counter JSONB NOT NULL,

    window_top_song JSONB NOT NULL,
    window_top_country JSONB NOT NULL,
    window_top_contestant JSONB NOT NULL,
    total_top_song JSONB NOT NULL,
    total_top_country JSONB NOT NULL,
    total_top_contestant JSONB NOT NULL,
    event_label VARCHAR NOT NULL
);

CREATE INDEX win_start_idx ON live_data.window_aggregates (window_start);