-- Binance 25% V1.3 memory-safe and resumable external validation.
-- Additive and idempotent. Existing scan rows, events, controls and files remain.

begin;

-- Durable checkpoint metadata on each long-running stage.
alter table public.binance_scan_jobs
  add column if not exists checkpoint_json jsonb not null default '{}'::jsonb;
alter table public.binance_scan_jobs
  add column if not exists resume_count integer not null default 0;
alter table public.binance_scan_jobs
  add column if not exists last_checkpoint_at timestamptz;
alter table public.binance_scan_jobs
  add column if not exists last_stage text;
alter table public.binance_scan_jobs
  add column if not exists last_unit text;

alter table public.binance_matched_control_jobs
  add column if not exists checkpoint_json jsonb not null default '{}'::jsonb;
alter table public.binance_matched_control_jobs
  add column if not exists resume_count integer not null default 0;
alter table public.binance_matched_control_jobs
  add column if not exists last_checkpoint_at timestamptz;
alter table public.binance_matched_control_jobs
  add column if not exists last_stage text;
alter table public.binance_matched_control_jobs
  add column if not exists last_unit text;

alter table public.binance_external_validation_jobs
  add column if not exists checkpoint_json jsonb not null default '{}'::jsonb;
alter table public.binance_external_validation_jobs
  add column if not exists resume_count integer not null default 0;
alter table public.binance_external_validation_jobs
  add column if not exists last_checkpoint_at timestamptz;
alter table public.binance_external_validation_jobs
  add column if not exists last_stage text;
alter table public.binance_external_validation_jobs
  add column if not exists last_unit text;

-- Event-level checkpoints for matched-control construction. A completed event
-- is never recalculated after a worker restart.
create table if not exists public.binance_matched_control_progress (
  matched_control_job_id uuid not null
    references public.binance_matched_control_jobs(id) on delete cascade,
  event_id uuid not null
    references public.binance_gainer_events(id) on delete cascade,
  symbol text not null,
  status text not null check (status in ('completed','failed')),
  controls_created integer not null default 0,
  rejection_json jsonb not null default '{}'::jsonb,
  error_message text,
  completed_at timestamptz,
  updated_at timestamptz not null default now(),
  primary key (matched_control_job_id, event_id)
);

-- Sample-level durable feature rows for frozen-rule evaluation. These replace
-- an all-in-memory feature matrix and survive Render process restarts.
create table if not exists public.binance_external_validation_feature_rows (
  external_validation_job_id uuid not null
    references public.binance_external_validation_jobs(id) on delete cascade,
  sample_id text not null,
  match_group_id text not null,
  sample_type text not null check (sample_type in ('event','control')),
  label integer not null check (label in (0,1)),
  symbol text not null,
  feature_json jsonb not null,
  audit_json jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (external_validation_job_id, sample_id)
);

create table if not exists public.binance_external_validation_source_manifest (
  external_validation_job_id uuid not null
    references public.binance_external_validation_jobs(id) on delete cascade,
  symbol text not null,
  source_date date not null,
  manifest_json jsonb not null,
  created_at timestamptz not null default now(),
  primary key (external_validation_job_id, symbol, source_date)
);

create index if not exists idx_binance_matched_progress_job_status
  on public.binance_matched_control_progress(matched_control_job_id, status, symbol);
create index if not exists idx_binance_external_feature_job_symbol
  on public.binance_external_validation_feature_rows(external_validation_job_id, symbol);
create index if not exists idx_binance_external_feature_group
  on public.binance_external_validation_feature_rows(external_validation_job_id, match_group_id);
create index if not exists idx_binance_external_manifest_job
  on public.binance_external_validation_source_manifest(external_validation_job_id, symbol, source_date);

alter table public.binance_matched_control_progress enable row level security;
alter table public.binance_external_validation_feature_rows enable row level security;
alter table public.binance_external_validation_source_manifest enable row level security;

-- Bootstrap a safe symbol-level resume point for an already-running V1.2 scan.
-- Existing V1.2 matched-control and evaluation jobs did not persist their
-- intermediate rows, so their first V1.3 attempt may need to rebuild the
-- current stage once; all subsequent V1.3 attempts resume durably.
update public.binance_scan_jobs
set checkpoint_json = jsonb_build_object(
      'schema_version', 1,
      'phase', 'scan_symbols',
      'next_symbol_index', coalesce(symbols_processed, 0),
      'symbols_total', coalesce(symbols_total, 0),
      'candidates_found', coalesce(candidates_found, 0),
      'events_found', coalesce(events_found, 0),
      'failures', coalesce(failures, 0),
      'daily_rows', coalesce(daily_rows, 0),
      'bootstrapped_from_v1_2', true
    ),
    last_stage = 'v1_2_checkpoint_bootstrap',
    last_checkpoint_at = now()
where research_purpose = 'external_validation_c2_c4'
  and status in ('queued','running','failed')
  and (checkpoint_json is null or checkpoint_json = '{}'::jsonb)
  and coalesce(symbols_processed, 0) > 0;

commit;
