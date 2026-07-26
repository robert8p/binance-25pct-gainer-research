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
