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
