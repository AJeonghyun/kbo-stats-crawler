CREATE TABLE IF NOT EXISTS teams (
    team_id BIGSERIAL PRIMARY KEY,
    team_code TEXT NOT NULL UNIQUE,
    team_name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS players (
    player_id BIGSERIAL PRIMARY KEY,
    kbo_player_id TEXT UNIQUE,
    player_name TEXT NOT NULL,
    team_id BIGINT REFERENCES teams(team_id),
    birth_date DATE,
    position_name TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (player_name, team_id)
);

CREATE TABLE IF NOT EXISTS crawl_jobs (
    crawl_job_id BIGSERIAL PRIMARY KEY,
    source_name TEXT NOT NULL,
    source_url TEXT NOT NULL,
    stat_scope TEXT NOT NULL,
    target_date DATE NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'started',
    row_count INTEGER NOT NULL DEFAULT 0,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS player_hitter_daily_stats (
    player_hitter_daily_stat_id BIGSERIAL PRIMARY KEY,
    player_id BIGINT NOT NULL REFERENCES players(player_id),
    team_id BIGINT NOT NULL REFERENCES teams(team_id),
    stat_date DATE NOT NULL,
    season_year INTEGER NOT NULL,
    stat_rank INTEGER,
    games_played INTEGER,
    plate_appearances INTEGER,
    at_bats INTEGER,
    runs INTEGER,
    hits INTEGER,
    doubles INTEGER,
    triples INTEGER,
    home_runs INTEGER,
    total_bases INTEGER,
    runs_batted_in INTEGER,
    stolen_bases INTEGER,
    caught_stealing INTEGER,
    walks INTEGER,
    intentional_walks INTEGER,
    hit_by_pitch INTEGER,
    strikeouts INTEGER,
    double_plays INTEGER,
    sacrifice_bunts INTEGER,
    sacrifice_flies INTEGER,
    multi_hits INTEGER,
    batting_average NUMERIC(6, 3),
    on_base_percentage NUMERIC(6, 3),
    slugging_percentage NUMERIC(6, 3),
    ops NUMERIC(6, 3),
    isolated_power NUMERIC(6, 3),
    babip NUMERIC(6, 3),
    runners_in_scoring_position_avg NUMERIC(6, 3),
    pinch_hit_batting_average NUMERIC(6, 3),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    crawl_job_id BIGINT REFERENCES crawl_jobs(crawl_job_id),
    UNIQUE (player_id, stat_date)
);

CREATE INDEX IF NOT EXISTS idx_player_hitter_daily_stats_stat_date
    ON player_hitter_daily_stats (stat_date DESC);

CREATE INDEX IF NOT EXISTS idx_player_hitter_daily_stats_team_date
    ON player_hitter_daily_stats (team_id, stat_date DESC);

CREATE INDEX IF NOT EXISTS idx_player_hitter_daily_stats_season_year
    ON player_hitter_daily_stats (season_year, stat_date DESC);

CREATE TABLE IF NOT EXISTS team_daily_stats (
    team_daily_stat_id BIGSERIAL PRIMARY KEY,
    team_id BIGINT NOT NULL REFERENCES teams(team_id),
    stat_date DATE NOT NULL,
    season_year INTEGER NOT NULL,
    stat_rank INTEGER,
    games_played INTEGER,
    wins INTEGER,
    losses INTEGER,
    draws INTEGER,
    winning_percentage NUMERIC(6, 3),
    plate_appearances INTEGER,
    at_bats INTEGER,
    runs_scored INTEGER,
    hits INTEGER,
    doubles INTEGER,
    triples INTEGER,
    runs_batted_in INTEGER,
    sacrifice_bunts INTEGER,
    sacrifice_flies INTEGER,
    walks INTEGER,
    intentional_walks INTEGER,
    hit_by_pitch INTEGER,
    strikeouts INTEGER,
    double_plays INTEGER,
    total_bases INTEGER,
    runs_allowed INTEGER,
    team_batting_average NUMERIC(6, 3),
    team_on_base_percentage NUMERIC(6, 3),
    team_slugging_percentage NUMERIC(6, 3),
    team_ops NUMERIC(6, 3),
    team_home_runs INTEGER,
    team_stolen_bases INTEGER,
    team_multi_hits INTEGER,
    team_risp_avg NUMERIC(6, 3),
    team_pinch_hit_batting_average NUMERIC(6, 3),
    team_era NUMERIC(6, 2),
    team_whip NUMERIC(6, 3),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    crawl_job_id BIGINT REFERENCES crawl_jobs(crawl_job_id),
    UNIQUE (team_id, stat_date)
);

CREATE INDEX IF NOT EXISTS idx_team_daily_stats_stat_date
    ON team_daily_stats (stat_date DESC);

CREATE INDEX IF NOT EXISTS idx_team_daily_stats_season_year
    ON team_daily_stats (season_year, stat_date DESC);

CREATE TABLE IF NOT EXISTS raw_crawl_results (
    raw_crawl_result_id BIGSERIAL PRIMARY KEY,
    crawl_job_id BIGINT NOT NULL REFERENCES crawl_jobs(crawl_job_id),
    entity_type TEXT NOT NULL,
    entity_key TEXT NOT NULL,
    stat_date DATE NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_crawl_results_job_id
    ON raw_crawl_results (crawl_job_id);

CREATE INDEX IF NOT EXISTS idx_raw_crawl_results_entity_date
    ON raw_crawl_results (entity_type, stat_date DESC);

ALTER TABLE player_hitter_daily_stats
    ADD COLUMN IF NOT EXISTS stat_rank INTEGER,
    ADD COLUMN IF NOT EXISTS total_bases INTEGER,
    ADD COLUMN IF NOT EXISTS intentional_walks INTEGER,
    ADD COLUMN IF NOT EXISTS multi_hits INTEGER,
    ADD COLUMN IF NOT EXISTS runners_in_scoring_position_avg NUMERIC(6, 3),
    ADD COLUMN IF NOT EXISTS pinch_hit_batting_average NUMERIC(6, 3);

ALTER TABLE team_daily_stats
    ADD COLUMN IF NOT EXISTS stat_rank INTEGER,
    ADD COLUMN IF NOT EXISTS plate_appearances INTEGER,
    ADD COLUMN IF NOT EXISTS at_bats INTEGER,
    ADD COLUMN IF NOT EXISTS hits INTEGER,
    ADD COLUMN IF NOT EXISTS doubles INTEGER,
    ADD COLUMN IF NOT EXISTS triples INTEGER,
    ADD COLUMN IF NOT EXISTS runs_batted_in INTEGER,
    ADD COLUMN IF NOT EXISTS sacrifice_bunts INTEGER,
    ADD COLUMN IF NOT EXISTS sacrifice_flies INTEGER,
    ADD COLUMN IF NOT EXISTS walks INTEGER,
    ADD COLUMN IF NOT EXISTS intentional_walks INTEGER,
    ADD COLUMN IF NOT EXISTS hit_by_pitch INTEGER,
    ADD COLUMN IF NOT EXISTS strikeouts INTEGER,
    ADD COLUMN IF NOT EXISTS double_plays INTEGER,
    ADD COLUMN IF NOT EXISTS total_bases INTEGER,
    ADD COLUMN IF NOT EXISTS team_multi_hits INTEGER,
    ADD COLUMN IF NOT EXISTS team_risp_avg NUMERIC(6, 3),
    ADD COLUMN IF NOT EXISTS team_pinch_hit_batting_average NUMERIC(6, 3);
