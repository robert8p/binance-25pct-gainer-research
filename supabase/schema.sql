create extension if not exists pgcrypto;

create table if not exists public.stock25_scans (
  id uuid primary key default gen_random_uuid(),
  status text not null check (status in ('queued','running','completed','failed')),
  source text not null default 'manual',
  parameters jsonb not null default '{}'::jsonb,
  parameters_hash text,
  created_at timestamptz not null default now(),
  started_at timestamptz,
  heartbeat_at timestamptz,
  completed_at timestamptz,
  lookback_start date,
  lookback_end date,
  progress_stage text,
  progress_current integer not null default 0,
  progress_total integer not null default 0,
  universe_count integer not null default 0,
  candidate_day_count integer not null default 0,
  result_count integer not null default 0,
  coverage_notes jsonb not null default '[]'::jsonb,
  error_message text
);

create table if not exists public.stock25_scan_results (
  id uuid primary key default gen_random_uuid(),
  scan_id uuid not null references public.stock25_scans(id) on delete cascade,
  symbol text not null,
  company_name text,
  exchange text,
  event_date date not null,
  current_status text,
  currently_tradable boolean not null default false,
  fractionable boolean not null default false,
  shortable boolean not null default false,
  easy_to_borrow boolean not null default false,
  feed text not null,
  threshold_pct numeric not null,
  prior_close numeric not null,
  threshold_price numeric not null,
  session_open numeric not null,
  session_high numeric not null,
  session_low numeric not null,
  session_close numeric not null,
  session_volume bigint not null default 0,
  session_trade_count bigint not null default 0,
  opening_gap_pct numeric not null,
  high_vs_prior_close_pct numeric not null,
  open_to_peak_pct numeric not null,
  first_minute_close numeric not null,
  first_minute_entry_to_peak_pct numeric not null,
  threshold_cross_bar_start timestamptz,
  peak_bar_start timestamptz,
  peak_price numeric not null,
  minutes_from_open_to_cross integer,
  minutes_from_open_to_peak integer,
  first_bar_volume bigint not null default 0,
  first_bar_trade_count bigint not null default 0,
  peak_bar_volume bigint not null default 0,
  peak_bar_trade_count bigint not null default 0,
  max_missing_bar_gap_minutes integer not null default 0,
  quality_flags jsonb not null default '[]'::jsonb,
  result_hash text not null,
  created_at timestamptz not null default now(),
  unique (scan_id, symbol, event_date)
);

create table if not exists public.stock25_event_bars (
  id bigint generated always as identity primary key,
  scan_id uuid not null references public.stock25_scans(id) on delete cascade,
  result_id uuid not null references public.stock25_scan_results(id) on delete cascade,
  symbol text not null,
  event_date date not null,
  bar_timestamp timestamptz not null,
  open numeric not null,
  high numeric not null,
  low numeric not null,
  close numeric not null,
  volume bigint not null default 0,
  trade_count bigint not null default 0,
  vwap numeric,
  unique (scan_id, symbol, bar_timestamp)
);

create table if not exists public.stock25_asset_snapshots (
  snapshot_date date not null,
  asset_id uuid not null,
  symbol text not null,
  name text,
  exchange text,
  status text,
  tradable boolean not null default false,
  fractionable boolean not null default false,
  shortable boolean not null default false,
  easy_to_borrow boolean not null default false,
  marginable boolean not null default false,
  attributes jsonb not null default '[]'::jsonb,
  created_at timestamptz not null default now(),
  primary key (snapshot_date, asset_id)
);

create index if not exists stock25_scans_created_at_idx on public.stock25_scans (created_at desc);
create index if not exists stock25_scan_results_scan_idx on public.stock25_scan_results (scan_id, event_date desc);
create index if not exists stock25_scan_results_symbol_idx on public.stock25_scan_results (symbol, event_date desc);
create index if not exists stock25_scan_results_gain_idx on public.stock25_scan_results (high_vs_prior_close_pct desc);
create index if not exists stock25_event_bars_result_idx on public.stock25_event_bars (result_id, bar_timestamp);
create index if not exists stock25_asset_snapshots_symbol_idx on public.stock25_asset_snapshots (symbol, snapshot_date desc);

alter table public.stock25_scans enable row level security;
alter table public.stock25_scan_results enable row level security;
alter table public.stock25_event_bars enable row level security;
alter table public.stock25_asset_snapshots enable row level security;

-- No public RLS policies are created. The backend uses the Supabase service-role key,
-- which bypasses RLS. Never expose that key to a browser or commit it to GitHub.
-- Run this once on an existing v1 Alpaca 25% Scanner Supabase project.

create table if not exists public.stock25_research_jobs (
  id uuid primary key default gen_random_uuid(),
  source_scan_id uuid not null references public.stock25_scans(id) on delete cascade,
  status text not null check (status in ('queued','running','completed','failed')),
  parameters jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  started_at timestamptz,
  heartbeat_at timestamptz,
  completed_at timestamptz,
  progress_stage text,
  progress_current integer not null default 0,
  progress_total integer not null default 0,
  source_event_count integer not null default 0,
  eligible_event_count integer not null default 0,
  completed_event_count integer not null default 0,
  failed_event_count integer not null default 0,
  index_file_id uuid,
  index_storage_path text,
  error_message text
);

create table if not exists public.stock25_research_events (
  id uuid primary key default gen_random_uuid(),
  research_job_id uuid not null references public.stock25_research_jobs(id) on delete cascade,
  source_result_id uuid not null references public.stock25_scan_results(id) on delete cascade,
  symbol text not null,
  event_date date not null,
  status text not null check (status in ('processing','collecting','completed','failed')),
  eligible boolean not null default false,
  sellability_status text,
  exact_cross_timestamp timestamptz,
  exact_cross_timestamp_raw text,
  adjusted_threshold_price numeric,
  raw_threshold_price numeric,
  adjustment_scale numeric,
  minimum_notional numeric,
  sellability_window_seconds integer,
  active_bid_at_cross_price numeric,
  active_bid_at_cross_notional numeric,
  displayed_seconds_at_or_above_threshold numeric,
  max_contiguous_displayed_seconds numeric,
  seconds_to_first_confirmed_exit numeric,
  first_confirmed_exit_slippage_bps numeric,
  max_bid_price numeric,
  max_bid_notional numeric,
  max_trade_price_after_cross numeric,
  trade_volume_at_or_above_threshold bigint,
  trade_count_at_or_above_threshold bigint,
  first_confirmed_exit_timestamp timestamptz,
  first_confirmed_exit_timestamp_raw text,
  first_confirmed_exit_bid numeric,
  first_confirmed_exit_notional numeric,
  horizon_metrics jsonb not null default '{}'::jsonb,
  row_counts jsonb not null default '{}'::jsonb,
  quality_flags jsonb not null default '[]'::jsonb,
  event_storage_path text,
  event_package_size_bytes bigint,
  error_message text,
  created_at timestamptz not null default now(),
  completed_at timestamptz,
  unique (research_job_id, source_result_id)
);

create table if not exists public.stock25_research_files (
  id uuid primary key default gen_random_uuid(),
  research_job_id uuid not null references public.stock25_research_jobs(id) on delete cascade,
  research_event_id uuid references public.stock25_research_events(id) on delete cascade,
  file_kind text not null,
  storage_path text not null,
  filename text not null,
  size_bytes bigint not null,
  sha256 text not null,
  created_at timestamptz not null default now(),
  unique (research_job_id, storage_path)
);


-- Heartbeats allow interrupted Render jobs to resume safely after a crash or deploy.
alter table public.stock25_scans add column if not exists heartbeat_at timestamptz;
alter table public.stock25_research_jobs add column if not exists heartbeat_at timestamptz;
alter table public.stock25_research_jobs add column if not exists failed_event_count integer not null default 0;
alter table public.stock25_research_events add column if not exists exact_cross_timestamp_raw text;
alter table public.stock25_research_events add column if not exists first_confirmed_exit_timestamp_raw text;
alter table public.stock25_research_events add column if not exists active_bid_at_cross_price numeric;
alter table public.stock25_research_events add column if not exists active_bid_at_cross_notional numeric;
alter table public.stock25_research_events add column if not exists displayed_seconds_at_or_above_threshold numeric;
alter table public.stock25_research_events add column if not exists max_contiguous_displayed_seconds numeric;
alter table public.stock25_research_events add column if not exists seconds_to_first_confirmed_exit numeric;
alter table public.stock25_research_events add column if not exists first_confirmed_exit_slippage_bps numeric;

create index if not exists stock25_research_jobs_created_idx on public.stock25_research_jobs (created_at desc);
create index if not exists stock25_research_events_job_idx on public.stock25_research_events (research_job_id, event_date desc);
create index if not exists stock25_research_events_eligible_idx on public.stock25_research_events (research_job_id, eligible, event_date desc);
create index if not exists stock25_research_files_job_idx on public.stock25_research_files (research_job_id, created_at);
create index if not exists stock25_research_files_event_idx on public.stock25_research_files (research_event_id, file_kind, created_at);

alter table public.stock25_research_jobs enable row level security;
alter table public.stock25_research_events enable row level security;
alter table public.stock25_research_files enable row level security;

-- Private object-storage bucket for event ZIPs and index ZIPs.
insert into storage.buckets (id, name, public)
values ('alpaca-25pct-research', 'alpaca-25pct-research', false)
on conflict (id) do update set public = false;

-- No public RLS policies are created. The backend uses the server-side secret/service-role key.
-- Alpaca 25% Gainer Research Collector v3 migration.
-- Run once in the existing Supabase project after v2 has completed.

create table if not exists public.stock25_control_jobs (
  id uuid primary key default gen_random_uuid(),
  source_research_job_id uuid not null references public.stock25_research_jobs(id) on delete cascade,
  status text not null check (status in ('queued','running','completed','failed')),
  parameters jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  started_at timestamptz,
  heartbeat_at timestamptz,
  completed_at timestamptz,
  progress_stage text,
  progress_current integer not null default 0,
  progress_total integer not null default 0,
  positive_event_count integer not null default 0,
  matched_pair_count integer not null default 0,
  unique_control_count integer not null default 0,
  completed_control_count integer not null default 0,
  failed_control_count integer not null default 0,
  unmatched_positive_count integer not null default 0,
  analysis_export_file_id uuid,
  analysis_export_storage_path text,
  matching_version text,
  balance_gate_status text,
  balance_report_storage_path text,
  excellent_pair_count integer not null default 0,
  good_pair_count integer not null default 0,
  strong_pair_count integer not null default 0,
  error_message text
);

create table if not exists public.stock25_control_observations (
  id uuid primary key default gen_random_uuid(),
  control_job_id uuid not null references public.stock25_control_jobs(id) on delete cascade,
  symbol text not null,
  exchange text,
  event_date date not null,
  pseudo_event_timestamp timestamptz not null,
  pseudo_event_timestamp_raw text not null,
  pseudo_event_key text not null,
  status text not null check (status in ('matched','collecting','completed','failed')),
  prior_sessions jsonb not null default '[]'::jsonb,
  feature_snapshot jsonb not null default '{}'::jsonb,
  row_counts jsonb not null default '{}'::jsonb,
  quality_flags jsonb not null default '[]'::jsonb,
  compact_storage_path text,
  compact_size_bytes bigint,
  error_message text,
  created_at timestamptz not null default now(),
  completed_at timestamptz,
  unique (control_job_id, symbol, event_date, pseudo_event_key)
);

create table if not exists public.stock25_control_pairs (
  id uuid primary key default gen_random_uuid(),
  control_job_id uuid not null references public.stock25_control_jobs(id) on delete cascade,
  positive_research_event_id uuid not null references public.stock25_research_events(id) on delete cascade,
  positive_source_result_id uuid not null references public.stock25_scan_results(id) on delete cascade,
  control_observation_id uuid references public.stock25_control_observations(id) on delete set null,
  positive_symbol text not null,
  event_date date not null,
  positive_tier text not null check (positive_tier in ('primary_clean','extended')),
  control_symbol text not null,
  control_exchange text,
  control_rank integer not null,
  match_score numeric not null,
  match_quality text not null,
  matching_version text,
  pseudo_event_timestamp timestamptz not null,
  pseudo_event_timestamp_raw text not null,
  positive_features jsonb not null default '{}'::jsonb,
  control_features jsonb not null default '{}'::jsonb,
  standardized_deltas jsonb not null default '{}'::jsonb,
  status text not null default 'matched' check (status in ('matched','collecting','completed','failed')),
  quality_flags jsonb not null default '[]'::jsonb,
  error_message text,
  created_at timestamptz not null default now(),
  completed_at timestamptz,
  unique (control_job_id, positive_research_event_id, control_rank)
);

create table if not exists public.stock25_control_match_diagnostics (
  id uuid primary key default gen_random_uuid(),
  control_job_id uuid not null references public.stock25_control_jobs(id) on delete cascade,
  positive_research_event_id uuid not null references public.stock25_research_events(id) on delete cascade,
  positive_symbol text not null,
  event_date date not null,
  requested_control_count integer not null,
  selected_control_count integer not null,
  reason text not null,
  candidate_count integer not null default 0,
  accepted_candidate_count integer not null default 0,
  rejection_counts jsonb not null default '{}'::jsonb,
  nearest_rejected jsonb not null default '[]'::jsonb,
  matching_version text not null default 'strict_global_v3.0.2',
  created_at timestamptz not null default now(),
  unique (control_job_id, positive_research_event_id)
);

create table if not exists public.stock25_control_datasets (
  id uuid primary key default gen_random_uuid(),
  control_job_id uuid not null references public.stock25_control_jobs(id) on delete cascade,
  symbol text not null,
  session_date date not null,
  window_type text not null check (window_type in ('full_session','prefix')),
  window_start timestamptz not null,
  window_end timestamptz not null,
  window_end_raw text not null,
  window_key text not null,
  feed text not null,
  status text not null check (status in ('collecting','completed','failed')),
  row_counts jsonb not null default '{}'::jsonb,
  derived_features jsonb not null default '{}'::jsonb,
  quality_flags jsonb not null default '[]'::jsonb,
  storage_prefix text,
  error_message text,
  created_at timestamptz not null default now(),
  completed_at timestamptz,
  unique (control_job_id, symbol, session_date, window_type, window_key, feed)
);

create table if not exists public.stock25_control_files (
  id uuid primary key default gen_random_uuid(),
  control_job_id uuid not null references public.stock25_control_jobs(id) on delete cascade,
  control_observation_id uuid references public.stock25_control_observations(id) on delete cascade,
  control_dataset_id uuid references public.stock25_control_datasets(id) on delete cascade,
  file_kind text not null,
  storage_path text not null,
  filename text not null,
  size_bytes bigint not null,
  sha256 text not null,
  created_at timestamptz not null default now(),
  unique (control_job_id, storage_path)
);

create index if not exists stock25_control_jobs_created_idx on public.stock25_control_jobs (created_at desc);
create index if not exists stock25_control_pairs_job_idx on public.stock25_control_pairs (control_job_id, event_date, positive_symbol);
create index if not exists stock25_control_pairs_positive_idx on public.stock25_control_pairs (positive_research_event_id, control_rank);
create index if not exists stock25_control_pairs_control_idx on public.stock25_control_pairs (control_symbol, event_date);
create index if not exists stock25_control_match_diagnostics_job_idx on public.stock25_control_match_diagnostics (control_job_id, event_date, positive_symbol);
create index if not exists stock25_control_observations_job_idx on public.stock25_control_observations (control_job_id, status, event_date);
create index if not exists stock25_control_datasets_job_idx on public.stock25_control_datasets (control_job_id, symbol, session_date);
create index if not exists stock25_control_files_job_idx on public.stock25_control_files (control_job_id, file_kind, created_at);
create index if not exists stock25_control_files_observation_idx on public.stock25_control_files (control_observation_id, file_kind);
create index if not exists stock25_control_files_dataset_idx on public.stock25_control_files (control_dataset_id, file_kind);

alter table public.stock25_control_jobs enable row level security;
alter table public.stock25_control_pairs enable row level security;
alter table public.stock25_control_match_diagnostics enable row level security;
alter table public.stock25_control_observations enable row level security;
alter table public.stock25_control_datasets enable row level security;
alter table public.stock25_control_files enable row level security;

-- Add the foreign key only after stock25_control_files exists. Re-running is safe.
do $$
begin
  if not exists (
    select 1 from pg_constraint where conname = 'control_jobs_analysis_export_file_fk'
  ) then
    alter table public.stock25_control_jobs
      add constraint control_jobs_analysis_export_file_fk
      foreign key (analysis_export_file_id) references public.stock25_control_files(id) on delete set null;
  end if;
end $$;

-- The existing private alpaca-25pct-research bucket is reused. No public policies are added;
-- only the server-side Supabase secret/service-role key can access these tables and files.
-- Alpaca 25% Gainer Research Lab V3.0.3 entry-feasibility/export migration.
-- Run once after migration_v3.sql and migration_v3_0_2.sql. Safe to run again.

create table if not exists public.stock25_entry_jobs (
  id uuid primary key default gen_random_uuid(),
  source_control_job_id uuid not null references public.stock25_control_jobs(id) on delete cascade,
  status text not null check (status in ('queued','running','completed','failed')),
  parameters jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  started_at timestamptz,
  heartbeat_at timestamptz,
  completed_at timestamptz,
  progress_stage text,
  progress_current integer not null default 0,
  progress_total integer not null default 0,
  positive_event_count integer not null default 0,
  entry_feasible_count integer not null default 0,
  primary_actionable_count integer not null default 0,
  excluded_positive_count integer not null default 0,
  failed_assessment_count integer not null default 0,
  matched_actionable_positive_count integer not null default 0,
  retained_control_pair_count integer not null default 0,
  export_storage_path text,
  error_message text
);

create table if not exists public.stock25_entry_assessments (
  id uuid primary key default gen_random_uuid(),
  entry_job_id uuid not null references public.stock25_entry_jobs(id) on delete cascade,
  research_event_id uuid not null references public.stock25_research_events(id) on delete cascade,
  source_result_id uuid not null references public.stock25_scan_results(id) on delete cascade,
  symbol text not null,
  event_date date not null,
  split_name text not null check (split_name in ('discovery','validation','sealed_test')),
  status text not null check (status in ('processing','completed','failed')),
  market_open_timestamp timestamptz,
  market_open_timestamp_raw text,
  exact_cross_timestamp timestamptz,
  exact_cross_timestamp_raw text,
  raw_threshold_price numeric,
  adjusted_threshold_price numeric,
  session_open numeric,
  opening_gap_pct numeric,
  seconds_open_to_threshold numeric,
  minimum_entry_notional numeric,
  reaction_delay_seconds numeric,
  minimum_opportunity_seconds numeric,
  minimum_gross_edge_pct numeric,
  require_subsequent_trade boolean,
  purchase_feasible boolean not null default false,
  primary_actionable boolean not null default false,
  first_entry_timestamp_raw text,
  first_entry_ask numeric,
  first_entry_notional numeric,
  first_entry_gross_edge_pct numeric,
  opportunity_seconds numeric,
  seconds_entry_to_threshold numeric,
  sellability_confirmed boolean not null default false,
  first_confirmed_exit_timestamp_raw text,
  first_confirmed_exit_bid numeric,
  first_confirmed_exit_notional numeric,
  entry_window_metrics jsonb not null default '{}'::jsonb,
  row_counts jsonb not null default '{}'::jsonb,
  quality_flags jsonb not null default '[]'::jsonb,
  exclusion_reason text,
  error_message text,
  created_at timestamptz not null default now(),
  completed_at timestamptz,
  unique (entry_job_id, research_event_id)
);

create table if not exists public.stock25_entry_files (
  id uuid primary key default gen_random_uuid(),
  entry_job_id uuid not null references public.stock25_entry_jobs(id) on delete cascade,
  file_kind text not null,
  storage_path text not null,
  filename text not null,
  size_bytes bigint not null,
  sha256 text not null,
  created_at timestamptz not null default now(),
  unique (entry_job_id, storage_path)
);

create index if not exists stock25_entry_jobs_created_idx
  on public.stock25_entry_jobs (created_at desc);
create index if not exists stock25_entry_assessments_job_idx
  on public.stock25_entry_assessments (entry_job_id, status, event_date, symbol);
create index if not exists stock25_entry_assessments_actionable_idx
  on public.stock25_entry_assessments (entry_job_id, primary_actionable, split_name, event_date);
create index if not exists stock25_entry_files_job_idx
  on public.stock25_entry_files (entry_job_id, file_kind, created_at);

alter table public.stock25_entry_jobs enable row level security;
alter table public.stock25_entry_assessments enable row level security;
alter table public.stock25_entry_files enable row level security;

comment on table public.stock25_entry_assessments is
  'Post-open purchase-feasibility classification using only pre-threshold quotes/trades; split-specific predictor exports are generated separately.';


-- V4 execution-aware full-universe backtest tables.
-- Alpaca 25% Gainer Research Lab V4.0.0 execution-aware backtest migration.
-- Run once after the V3.0.3 migration. Safe to run again.

create table if not exists public.stock25_backtest_jobs (
  id uuid primary key default gen_random_uuid(),
  source_entry_job_id uuid not null references public.stock25_entry_jobs(id) on delete cascade,
  status text not null check (status in ('queued','running','completed','failed')),
  parameters jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  started_at timestamptz,
  heartbeat_at timestamptz,
  completed_at timestamptz,
  progress_stage text,
  progress_current integer not null default 0,
  progress_total integer not null default 0,
  window_start date,
  window_end date,
  universe_symbol_count integer not null default 0,
  trigger_count integer not null default 0,
  selected_trade_count integer not null default 0,
  filled_trade_count integer not null default 0,
  total_pnl_usd numeric,
  export_storage_path text,
  error_message text
);

create table if not exists public.stock25_backtest_days (
  id uuid primary key default gen_random_uuid(),
  backtest_job_id uuid not null references public.stock25_backtest_jobs(id) on delete cascade,
  trade_date date not null,
  status text not null check (status in ('completed','failed')),
  eligible_symbol_count integer not null default 0,
  preopen_trigger_count integer not null default 0,
  midday_trigger_count integer not null default 0,
  selected_trade_count integer not null default 0,
  filled_trade_count integer not null default 0,
  diagnostics jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  unique(backtest_job_id, trade_date)
);

create table if not exists public.stock25_backtest_triggers (
  id uuid primary key default gen_random_uuid(),
  backtest_job_id uuid not null references public.stock25_backtest_jobs(id) on delete cascade,
  strategy text not null check (strategy in ('preopen','midday')),
  trade_date date not null,
  symbol text not null,
  rank integer not null,
  selected boolean not null default false,
  signal_value numeric not null,
  prior_close numeric not null,
  decision_timestamp timestamptz,
  features jsonb not null default '{}'::jsonb,
  quality_flags jsonb not null default '[]'::jsonb,
  created_at timestamptz not null default now(),
  unique(backtest_job_id, strategy, trade_date, symbol)
);

create table if not exists public.stock25_backtest_trades (
  id uuid primary key default gen_random_uuid(),
  backtest_job_id uuid not null references public.stock25_backtest_jobs(id) on delete cascade,
  strategy text not null check (strategy in ('preopen','midday')),
  trade_date date not null,
  symbol text not null,
  rank integer not null,
  signal_value numeric not null,
  prior_close numeric not null,
  position_notional_requested numeric not null,
  filled boolean not null default false,
  unfilled_reason text,
  entry_timestamp timestamptz,
  entry_ask_raw numeric,
  entry_price numeric,
  shares numeric,
  invested_notional numeric,
  target_price numeric,
  stop_price numeric,
  exit_timestamp timestamptz,
  exit_bid_raw numeric,
  exit_price numeric,
  exit_reason text check (exit_reason in ('target','stop','time') or exit_reason is null),
  return_pct numeric,
  pnl_usd numeric,
  max_bid_after_entry numeric,
  min_bid_after_entry numeric,
  max_quote_gap_seconds numeric,
  capacity_shortfall boolean not null default false,
  execution_sensitivity jsonb not null default '{}'::jsonb,
  quality_flags jsonb not null default '[]'::jsonb,
  created_at timestamptz not null default now(),
  unique(backtest_job_id, strategy, trade_date, symbol)
);

create table if not exists public.stock25_backtest_files (
  id uuid primary key default gen_random_uuid(),
  backtest_job_id uuid not null references public.stock25_backtest_jobs(id) on delete cascade,
  file_kind text not null,
  storage_path text not null,
  filename text not null,
  size_bytes bigint not null,
  sha256 text not null,
  created_at timestamptz not null default now(),
  unique(backtest_job_id, storage_path)
);

create index if not exists stock25_backtest_jobs_created_idx on public.stock25_backtest_jobs(created_at desc);
create index if not exists stock25_backtest_days_job_idx on public.stock25_backtest_days(backtest_job_id, trade_date);
create index if not exists stock25_backtest_triggers_job_idx on public.stock25_backtest_triggers(backtest_job_id, strategy, trade_date, selected);
create index if not exists stock25_backtest_trades_job_idx on public.stock25_backtest_trades(backtest_job_id, strategy, trade_date, filled);
create index if not exists stock25_backtest_files_job_idx on public.stock25_backtest_files(backtest_job_id, file_kind);

alter table public.stock25_backtest_jobs enable row level security;
alter table public.stock25_backtest_days enable row level security;
alter table public.stock25_backtest_triggers enable row level security;
alter table public.stock25_backtest_trades enable row level security;
alter table public.stock25_backtest_files enable row level security;

comment on table public.stock25_backtest_jobs is
  'Frozen-signal, full-universe, execution-aware historical profitability backtests. No order placement.';

alter table public.stock25_backtest_trades
  add column if not exists execution_sensitivity jsonb not null default '{}'::jsonb;
