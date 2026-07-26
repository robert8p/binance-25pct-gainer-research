create extension if not exists pgcrypto;

create table if not exists binance_scan_jobs (
  id uuid primary key default gen_random_uuid(),
  status text not null check (status in ('queued','running','completed','completed_with_warnings','failed')),
  created_at timestamptz not null default now(),
  started_at timestamptz,
  completed_at timestamptz,
  heartbeat_at timestamptz,
  event_definition_version text not null default 'v1_25pct_rolling_8h' check (event_definition_version = 'v1_25pct_rolling_8h'),
  lookback_days integer not null default 60,
  window_minutes integer not null default 480 check (window_minutes = 480),
  threshold_pct numeric not null default 25 check (threshold_pct = 25),
  quote_assets jsonb not null default '["USDT","USDC","FDUSD"]'::jsonb,
  min_exit_notional numeric not null default 500,
  confirmation_window_seconds integer not null default 300,
  symbols_total integer not null default 0,
  symbols_processed integer not null default 0,
  daily_rows bigint not null default 0,
  candidates_found integer not null default 0,
  events_found integer not null default 0,
  failures integer not null default 0,
  result_json jsonb,
  error_message text
);

create table if not exists binance_symbol_snapshots (
  scan_id uuid not null references binance_scan_jobs(id) on delete cascade,
  snapshot_at timestamptz not null,
  symbol text not null,
  base_asset text not null,
  quote_asset text not null,
  quote_priority integer not null,
  selected_canonical boolean not null default false,
  status text not null,
  spot_permission boolean not null,
  is_spot_trading_allowed boolean not null,
  stablecoin_like boolean not null default false,
  leveraged_token_like boolean not null default false,
  raw_json jsonb not null,
  primary key (scan_id, symbol)
);

create table if not exists binance_daily_bars (
  scan_id uuid not null references binance_scan_jobs(id) on delete cascade,
  symbol text not null,
  interval text not null default '1d',
  open_time timestamptz not null,
  close_time timestamptz not null,
  open numeric not null,
  high numeric not null,
  low numeric not null,
  close numeric not null,
  volume numeric not null,
  quote_volume numeric not null,
  trade_count bigint not null,
  taker_buy_base_volume numeric not null,
  taker_buy_quote_volume numeric not null,
  primary key (scan_id, symbol, open_time)
);

create table if not exists binance_gainer_events (
  id uuid primary key,
  scan_id uuid not null references binance_scan_jobs(id) on delete cascade,
  symbol text not null,
  base_asset text not null,
  quote_asset text not null,
  event_date date not null,
  event_definition_version text not null default 'v1_25pct_rolling_8h' check (event_definition_version = 'v1_25pct_rolling_8h'),
  previous_day_close numeric not null,
  previous_day_bar_available boolean not null default true,
  threshold_pct numeric not null check (threshold_pct = 25),
  threshold_price numeric not null,
  window_minutes integer not null default 480 check (window_minutes = 480),
  measurement_method text not null default 'rolling prior-minute low to later-minute high',
  baseline_time timestamptz,
  baseline_price numeric,
  baseline_trade_time timestamptz,
  baseline_agg_trade_id bigint,
  baseline_trade_unresolved boolean not null default false,
  minutes_baseline_open_to_cross_open integer,
  exact_baseline_to_cross_seconds numeric,
  exact_window_pass boolean not null default false,
  rolling_gain_pct_at_cross_trade numeric,
  day_open numeric not null,
  first_minute_close numeric,
  first_cross_time timestamptz not null,
  first_cross_trade_time timestamptz,
  crossing_agg_trade_id bigint,
  crossing_trade_price numeric,
  crossing_trade_unresolved boolean not null default false,
  crossing_minute_open numeric not null,
  crossing_minute_high numeric not null,
  day_high numeric not null,
  day_high_time timestamptz not null,
  day_close numeric not null,
  day_quote_volume numeric not null,
  day_trade_count bigint not null,
  previous_close_to_high_pct numeric,
  day_open_to_high_pct numeric,
  first_minute_close_to_high_pct numeric,
  minutes_from_day_start_to_cross integer not null,
  minutes_from_day_start_to_peak integer not null,
  pre_cross_minutes integer not null,
  pre_cross_quote_volume numeric not null,
  crossed_in_first_minute boolean not null,
  missing_minute_bars integer not null,
  missing_window_minute_bars integer not null default 0,
  stablecoin_like boolean not null default false,
  leveraged_token_like boolean not null default false,
  sellability_method text not null,
  confirmation_window_seconds integer not null,
  minimum_exit_notional numeric not null,
  seller_taker_notional_at_or_above numeric not null,
  all_trade_notional_at_or_above numeric not null,
  seller_taker_notional_any_price numeric not null default 0,
  seller_taker_base_quantity_any_price numeric not null default 0,
  minimum_exit_vwap numeric,
  minimum_exit_vwap_pct_vs_threshold numeric,
  minimum_exit_reached_price numeric,
  lowest_seller_exit_price numeric,
  highest_seller_exit_price numeric,
  first_seller_exit_time timestamptz,
  minimum_exit_reached_time timestamptz,
  sellability_pass boolean not null,
  sellability_trades_truncated boolean not null default false,
  current_exchange_tradability_only boolean not null default true,
  quality_status text not null,
  created_at timestamptz not null default now(),
  unique (scan_id, symbol, event_date)
);

create table if not exists binance_event_minute_bars (
  event_id uuid not null references binance_gainer_events(id) on delete cascade,
  scan_id uuid not null references binance_scan_jobs(id) on delete cascade,
  symbol text not null,
  event_date date not null,
  interval text not null default '1m',
  open_time timestamptz not null,
  close_time timestamptz not null,
  open numeric not null,
  high numeric not null,
  low numeric not null,
  close numeric not null,
  volume numeric not null,
  quote_volume numeric not null,
  trade_count bigint not null,
  taker_buy_base_volume numeric not null,
  taker_buy_quote_volume numeric not null,
  primary key (event_id, open_time)
);

create table if not exists binance_event_agg_trades (
  event_id uuid not null references binance_gainer_events(id) on delete cascade,
  scan_id uuid not null references binance_scan_jobs(id) on delete cascade,
  symbol text not null,
  event_date date not null,
  agg_trade_id bigint not null,
  trade_time timestamptz not null,
  price numeric not null,
  quantity numeric not null,
  quote_notional numeric not null,
  buyer_was_maker boolean not null,
  at_or_above_threshold boolean not null,
  primary key (event_id, agg_trade_id)
);

create table if not exists binance_decision_observations (
  event_id uuid not null references binance_gainer_events(id) on delete cascade,
  scan_id uuid not null references binance_scan_jobs(id) on delete cascade,
  symbol text not null,
  decision_label text not null,
  decision_time_utc timestamptz not null,
  entry_time_utc timestamptz not null,
  entry_close numeric not null,
  subsequent_high numeric not null,
  subsequent_high_time_utc timestamptz not null,
  max_gain_pct numeric,
  primary key (event_id, decision_label)
);

create table if not exists binance_scan_issues (
  id bigint generated always as identity primary key,
  scan_id uuid not null references binance_scan_jobs(id) on delete cascade,
  symbol text,
  stage text not null,
  message text not null,
  created_at timestamptz not null default now()
);

create table if not exists binance_research_jobs (
  id uuid primary key default gen_random_uuid(),
  scan_id uuid not null references binance_scan_jobs(id) on delete cascade,
  status text not null check (status in ('queued','running','completed','completed_with_warnings','failed')),
  created_at timestamptz not null default now(),
  started_at timestamptz,
  completed_at timestamptz,
  heartbeat_at timestamptz,
  prior_days integer not null default 10,
  maximum_events integer not null default 1,
  include_1s_klines boolean not null default true,
  include_agg_trades boolean not null default true,
  include_raw_trades boolean not null default false,
  events_total integer not null default 0,
  events_processed integer not null default 0,
  events_completed integer not null default 0,
  events_failed integer not null default 0,
  result_json jsonb,
  error_message text
);

create table if not exists binance_research_events (
  research_job_id uuid not null references binance_research_jobs(id) on delete cascade,
  event_id uuid not null references binance_gainer_events(id) on delete cascade,
  symbol text not null,
  event_date date not null,
  status text not null,
  storage_path text,
  warning_count integer not null default 0,
  created_at timestamptz not null default now(),
  primary key (research_job_id, event_id)
);

create table if not exists binance_research_files (
  id uuid primary key default gen_random_uuid(),
  research_job_id uuid not null references binance_research_jobs(id) on delete cascade,
  event_id uuid references binance_gainer_events(id) on delete cascade,
  storage_path text not null,
  filename text not null,
  size_bytes bigint not null,
  sha256 text not null,
  content_type text not null,
  role text,
  source_url text,
  created_at timestamptz not null default now(),
  unique (research_job_id, storage_path)
);

create table if not exists binance_research_issues (
  id bigint generated always as identity primary key,
  research_job_id uuid not null references binance_research_jobs(id) on delete cascade,
  event_id uuid references binance_gainer_events(id) on delete cascade,
  stage text not null,
  message text not null,
  created_at timestamptz not null default now()
);

create table if not exists binance_worker_heartbeats (
  worker_name text primary key,
  heartbeat_at timestamptz not null
);

-- Additive migration for installations created by v1.x.
-- Existing rows remain NULL so v1 scans cannot be mistaken for v2 scans.
alter table binance_scan_jobs add column if not exists event_definition_version text;
alter table binance_scan_jobs add column if not exists window_minutes integer not null default 180;
alter table binance_scan_jobs add column if not exists candidates_found integer not null default 0;
alter table binance_scan_jobs alter column lookback_days set default 60;

alter table binance_gainer_events add column if not exists event_definition_version text;
alter table binance_gainer_events add column if not exists previous_day_bar_available boolean not null default true;
alter table binance_gainer_events add column if not exists window_minutes integer not null default 180;
alter table binance_gainer_events add column if not exists measurement_method text not null default 'rolling prior-minute low to later-minute high';
alter table binance_gainer_events add column if not exists baseline_time timestamptz;
alter table binance_gainer_events add column if not exists baseline_price numeric;
alter table binance_gainer_events add column if not exists baseline_trade_time timestamptz;
alter table binance_gainer_events add column if not exists baseline_agg_trade_id bigint;
alter table binance_gainer_events add column if not exists baseline_trade_unresolved boolean not null default false;
alter table binance_gainer_events add column if not exists minutes_baseline_open_to_cross_open integer;
alter table binance_gainer_events add column if not exists exact_baseline_to_cross_seconds numeric;
alter table binance_gainer_events add column if not exists exact_window_pass boolean not null default false;
alter table binance_gainer_events add column if not exists rolling_gain_pct_at_cross_trade numeric;
alter table binance_gainer_events add column if not exists missing_window_minute_bars integer not null default 0;
alter table binance_gainer_events add column if not exists seller_taker_notional_any_price numeric not null default 0;
alter table binance_gainer_events add column if not exists seller_taker_base_quantity_any_price numeric not null default 0;
alter table binance_gainer_events add column if not exists minimum_exit_vwap numeric;
alter table binance_gainer_events add column if not exists minimum_exit_vwap_pct_vs_threshold numeric;
alter table binance_gainer_events add column if not exists minimum_exit_reached_price numeric;
alter table binance_gainer_events add column if not exists lowest_seller_exit_price numeric;
alter table binance_gainer_events add column if not exists highest_seller_exit_price numeric;

create index if not exists idx_binance_events_scan_date on binance_gainer_events(scan_id, event_date, symbol);
create index if not exists idx_binance_events_sellable on binance_gainer_events(scan_id, sellability_pass);
create index if not exists idx_binance_scan_jobs_status on binance_scan_jobs(status, created_at);
create index if not exists idx_binance_research_jobs_status on binance_research_jobs(status, created_at);
create index if not exists idx_binance_research_files_job on binance_research_files(research_job_id, created_at);

insert into storage.buckets (id, name, public, file_size_limit)
values ('binance-25pct-gainer-research', 'binance-25pct-gainer-research', false, 52428800)
on conflict (id) do update set public = excluded.public, file_size_limit = excluded.file_size_limit;

alter table binance_scan_jobs enable row level security;
alter table binance_symbol_snapshots enable row level security;
alter table binance_daily_bars enable row level security;
alter table binance_gainer_events enable row level security;
alter table binance_event_minute_bars enable row level security;
alter table binance_event_agg_trades enable row level security;
alter table binance_decision_observations enable row level security;
alter table binance_scan_issues enable row level security;
alter table binance_research_jobs enable row level security;
alter table binance_research_events enable row level security;
alter table binance_research_files enable row level security;
alter table binance_research_issues enable row level security;
alter table binance_worker_heartbeats enable row level security;

-- No anonymous policies are created. The app uses the server-side service-role key only.
-- Binance 3-hour surge research v3.0 matched-control migration.
-- Additive only: existing scans, events and research files are preserved.

create table if not exists binance_matched_control_jobs (
  id uuid primary key default gen_random_uuid(),
  scan_id uuid not null references binance_scan_jobs(id) on delete cascade,
  status text not null check (status in ('queued','running','completed','completed_with_warnings','failed')),
  created_at timestamptz not null default now(),
  started_at timestamptz,
  completed_at timestamptz,
  heartbeat_at timestamptz,
  controls_per_event integer not null default 5 check (controls_per_event between 1 and 10),
  prior_days integer not null default 10 check (prior_days between 1 and 30),
  horizons_minutes jsonb not null default '[15,30,60,120,180,480]'::jsonb,
  contamination_before_minutes integer not null default 120,
  contamination_after_minutes integer not null default 480,
  min_entry_notional numeric not null default 500,
  discovery_pct integer not null default 70,
  validation_pct integer not null default 15,
  events_total integer not null default 0,
  events_processed integer not null default 0,
  controls_target integer not null default 0,
  controls_created integer not null default 0,
  feature_rows integer not null default 0,
  failures integer not null default 0,
  result_json jsonb,
  error_message text
);

create table if not exists binance_control_matches (
  matched_control_job_id uuid not null references binance_matched_control_jobs(id) on delete cascade,
  event_id uuid not null references binance_gainer_events(id) on delete cascade,
  control_id uuid not null,
  symbol text not null,
  split text not null check (split in ('discovery','validation','sealed_test')),
  event_anchor_time timestamptz not null,
  control_anchor_time timestamptz not null,
  control_rank integer not null,
  clock_offset_minutes integer not null,
  calendar_distance_days integer not null,
  weekday_match boolean not null,
  match_tier text not null,
  prior_global_reuse_count integer not null default 0,
  minimum_5m_quote_volume numeric,
  prior_history_observed_fraction numeric,
  quality_status text not null,
  created_at timestamptz not null default now(),
  primary key (matched_control_job_id, control_id),
  unique (matched_control_job_id, event_id, control_rank)
);

create table if not exists binance_matched_control_files (
  id uuid primary key default gen_random_uuid(),
  matched_control_job_id uuid not null references binance_matched_control_jobs(id) on delete cascade,
  split text check (split in ('discovery','validation','sealed_test')),
  storage_path text not null,
  filename text not null,
  size_bytes bigint not null,
  sha256 text not null,
  content_type text not null,
  role text not null,
  created_at timestamptz not null default now(),
  unique (matched_control_job_id, storage_path)
);

create table if not exists binance_matched_control_issues (
  id bigint generated always as identity primary key,
  matched_control_job_id uuid not null references binance_matched_control_jobs(id) on delete cascade,
  event_id uuid references binance_gainer_events(id) on delete cascade,
  stage text not null,
  message text not null,
  created_at timestamptz not null default now()
);

create index if not exists idx_binance_matched_jobs_status
  on binance_matched_control_jobs(status, created_at);
create index if not exists idx_binance_control_matches_event
  on binance_control_matches(matched_control_job_id, event_id, control_rank);
create index if not exists idx_binance_control_matches_symbol_time
  on binance_control_matches(matched_control_job_id, symbol, control_anchor_time);
create index if not exists idx_binance_matched_files_job
  on binance_matched_control_files(matched_control_job_id, created_at);

alter table binance_matched_control_jobs enable row level security;
alter table binance_control_matches enable row level security;
alter table binance_matched_control_files enable row level security;
alter table binance_matched_control_issues enable row level security;

-- No anonymous policies are created. The app uses the server-side secret/service-role key only.

-- Version 4 additive ten-day-context contract.
alter table binance_scan_jobs add column if not exists window_start_date date;
alter table binance_scan_jobs add column if not exists window_end_date_exclusive date;

create table if not exists binance_context_jobs (
  id uuid primary key default gen_random_uuid(),
  matched_control_job_id uuid not null references binance_matched_control_jobs(id) on delete cascade,
  status text not null check (status in ('queued','running','completed','completed_with_warnings','failed')),
  research_mode text not null default 'exploratory_reuse' check (research_mode in ('exploratory_reuse','fresh_staged')),
  created_at timestamptz not null default now(),
  started_at timestamptz,
  completed_at timestamptz,
  heartbeat_at timestamptz,
  prior_days integer not null default 10 check (prior_days = 10),
  horizons_minutes jsonb not null default '[15,30,60,120,180,480]'::jsonb,
  windows_minutes jsonb not null default '[15,30,60,120,180,360,480,720,1440,2880,4320,7200,10080,14400]'::jsonb,
  min_entry_notional numeric not null default 500,
  samples_total integer not null default 0,
  samples_processed integer not null default 0,
  events_total integer not null default 0,
  controls_total integer not null default 0,
  feature_rows integer not null default 0,
  failures integer not null default 0,
  result_json jsonb,
  error_message text
);

create table if not exists binance_context_files (
  id uuid primary key default gen_random_uuid(),
  context_job_id uuid not null references binance_context_jobs(id) on delete cascade,
  split text check (split in ('discovery','validation','sealed_test')),
  storage_path text not null,
  filename text not null,
  size_bytes bigint not null,
  sha256 text not null,
  content_type text not null,
  role text not null,
  created_at timestamptz not null default now(),
  unique (context_job_id, storage_path)
);

create table if not exists binance_context_issues (
  id bigint generated always as identity primary key,
  context_job_id uuid not null references binance_context_jobs(id) on delete cascade,
  symbol text,
  stage text not null,
  message text not null,
  created_at timestamptz not null default now()
);

create index if not exists idx_binance_context_jobs_status
  on binance_context_jobs(status, created_at);
create index if not exists idx_binance_context_files_job
  on binance_context_files(context_job_id, created_at);
create index if not exists idx_binance_scan_custom_window
  on binance_scan_jobs(window_start_date, window_end_date_exclusive);

alter table binance_context_jobs enable row level security;
alter table binance_context_files enable row level security;
alter table binance_context_issues enable row level security;
-- Binance 25% baseline-context tables
-- Additive migration: preserves all existing scans, matched controls and context outputs.

create table if not exists binance_baseline_context_jobs (
  id uuid primary key default gen_random_uuid(),
  matched_control_job_id uuid not null references binance_matched_control_jobs(id) on delete cascade,
  status text not null check (status in ('queued','running','completed','completed_with_warnings','failed')),
  research_mode text not null default 'exploratory_reuse' check (research_mode in ('exploratory_reuse','fresh_staged')),
  created_at timestamptz not null default now(),
  started_at timestamptz,
  completed_at timestamptz,
  heartbeat_at timestamptz,
  prior_days integer not null default 10 check (prior_days = 10),
  snapshot_offsets_minutes jsonb not null default '[14400,10080,7200,4320,2880,1440,720,480,360,180,60,0]'::jsonb,
  pre_cross_horizons_minutes jsonb not null default '[15,30,60,120,180,480]'::jsonb,
  min_entry_notional numeric not null default 500,
  samples_total integer not null default 0,
  samples_processed integer not null default 0,
  events_total integer not null default 0,
  controls_total integer not null default 0,
  feature_rows integer not null default 0,
  pre_cross_rows integer not null default 0,
  failures integer not null default 0,
  result_json jsonb,
  error_message text
);

create table if not exists binance_baseline_context_files (
  id uuid primary key default gen_random_uuid(),
  baseline_context_job_id uuid not null references binance_baseline_context_jobs(id) on delete cascade,
  split text check (split in ('discovery','validation','sealed_test')),
  storage_path text not null,
  filename text not null,
  size_bytes bigint not null,
  sha256 text not null,
  content_type text not null,
  role text not null,
  created_at timestamptz not null default now(),
  unique (baseline_context_job_id, storage_path)
);

create table if not exists binance_baseline_context_issues (
  id bigint generated always as identity primary key,
  baseline_context_job_id uuid not null references binance_baseline_context_jobs(id) on delete cascade,
  symbol text,
  stage text not null,
  message text not null,
  created_at timestamptz not null default now()
);

create index if not exists idx_binance_baseline_context_jobs_status
  on binance_baseline_context_jobs(status, created_at);
create index if not exists idx_binance_baseline_context_files_job
  on binance_baseline_context_files(baseline_context_job_id, created_at);
create index if not exists idx_binance_baseline_context_issues_job
  on binance_baseline_context_issues(baseline_context_job_id, created_at);

alter table binance_baseline_context_jobs enable row level security;
alter table binance_baseline_context_files enable row level security;
alter table binance_baseline_context_issues enable row level security;

-- No anonymous policies are created. The app uses the server-side secret/service-role key only.
-- Confirmation and trading-rule backtest tables are intentionally omitted from the 25% discovery release.

-- Binance 25% research V1 final invariants.
alter table binance_scan_jobs alter column threshold_pct set default 25;
alter table binance_scan_jobs alter column window_minutes set default 480;
alter table binance_scan_jobs alter column event_definition_version set default 'v1_25pct_rolling_8h';
alter table binance_gainer_events alter column window_minutes set default 480;
alter table binance_gainer_events alter column event_definition_version set default 'v1_25pct_rolling_8h';
-- Binance 25% V1.2 frozen C2/C4 external-validation release.
-- Additive and idempotent: preserves all existing discovery, validation and
-- sealed-test rows. The V1.2 worker ignores legacy queued jobs.

begin;

alter table public.binance_scan_jobs
  add column if not exists research_purpose text not null default 'chatgpt_discovery';

alter table public.binance_matched_control_jobs
  add column if not exists research_purpose text not null default 'chatgpt_discovery';

-- Replace purpose checks idempotently.
alter table public.binance_scan_jobs
  drop constraint if exists binance_scan_jobs_research_purpose_check;
alter table public.binance_scan_jobs
  add constraint binance_scan_jobs_research_purpose_check
  check (research_purpose in ('chatgpt_discovery','external_validation_c2_c4'));

alter table public.binance_matched_control_jobs
  drop constraint if exists binance_matched_control_jobs_research_purpose_check;
alter table public.binance_matched_control_jobs
  add constraint binance_matched_control_jobs_research_purpose_check
  check (research_purpose in ('chatgpt_discovery','external_validation_c2_c4'));

-- The external-validation cohort is intentionally one opened set rather than
-- another discovery/validation/sealed split.
alter table public.binance_control_matches
  drop constraint if exists binance_control_matches_split_check;
alter table public.binance_control_matches
  add constraint binance_control_matches_split_check
  check (split in ('discovery','validation','sealed_test','external_validation'));

alter table public.binance_matched_control_files
  drop constraint if exists binance_matched_control_files_split_check;
alter table public.binance_matched_control_files
  add constraint binance_matched_control_files_split_check
  check (split in ('discovery','validation','sealed_test','external_validation'));

-- Ensure the manually corrected V1.1 baseline columns exist on every database.
alter table public.binance_baseline_context_jobs
  add column if not exists pre_cross_horizons_minutes jsonb not null
  default '[15,30,60,120,180,480]'::jsonb;
alter table public.binance_baseline_context_jobs
  add column if not exists pre_cross_rows integer not null default 0;

create table if not exists public.binance_external_validation_jobs (
  id uuid primary key default gen_random_uuid(),
  matched_control_job_id uuid not null
    references public.binance_matched_control_jobs(id) on delete cascade,
  status text not null
    check (status in ('queued','running','completed','completed_with_warnings','failed')),
  created_at timestamptz not null default now(),
  started_at timestamptz,
  completed_at timestamptz,
  heartbeat_at timestamptz,
  candidate_register_sha256 text not null,
  decision_horizon_minutes integer not null default 480
    check (decision_horizon_minutes = 480),
  samples_total integer not null default 0,
  samples_processed integer not null default 0,
  events_total integer not null default 0,
  controls_total integer not null default 0,
  feature_rows integer not null default 0,
  usable_groups integer not null default 0,
  failures integer not null default 0,
  overall_decision text,
  result_json jsonb,
  error_message text,
  unique (matched_control_job_id)
);

create table if not exists public.binance_external_validation_files (
  id uuid primary key default gen_random_uuid(),
  external_validation_job_id uuid not null
    references public.binance_external_validation_jobs(id) on delete cascade,
  storage_path text not null,
  filename text not null,
  size_bytes bigint not null,
  sha256 text not null,
  content_type text not null,
  role text not null,
  created_at timestamptz not null default now(),
  unique (external_validation_job_id, storage_path)
);

create table if not exists public.binance_external_validation_issues (
  id bigint generated always as identity primary key,
  external_validation_job_id uuid not null
    references public.binance_external_validation_jobs(id) on delete cascade,
  symbol text,
  stage text not null,
  message text not null,
  created_at timestamptz not null default now()
);

create index if not exists idx_binance_scan_research_purpose
  on public.binance_scan_jobs(research_purpose, status, created_at);
create index if not exists idx_binance_matched_research_purpose
  on public.binance_matched_control_jobs(research_purpose, status, created_at);
create index if not exists idx_binance_external_validation_jobs_status
  on public.binance_external_validation_jobs(status, created_at);
create index if not exists idx_binance_external_validation_files_job
  on public.binance_external_validation_files(external_validation_job_id, created_at);
create index if not exists idx_binance_external_validation_issues_job
  on public.binance_external_validation_issues(external_validation_job_id, created_at);

alter table public.binance_external_validation_jobs enable row level security;
alter table public.binance_external_validation_files enable row level security;
alter table public.binance_external_validation_issues enable row level security;

commit;
