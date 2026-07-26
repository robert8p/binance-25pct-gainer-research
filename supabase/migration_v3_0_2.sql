-- Alpaca 25% Gainer Research Collector V3.0.2 strict matching migration.
-- Run once after migration_v3.sql. Safe to run again.

alter table public.stock25_control_jobs
  add column if not exists matching_version text,
  add column if not exists balance_gate_status text,
  add column if not exists balance_report_storage_path text,
  add column if not exists excellent_pair_count integer not null default 0,
  add column if not exists good_pair_count integer not null default 0,
  add column if not exists strong_pair_count integer not null default 0;

alter table public.stock25_control_pairs
  add column if not exists matching_version text;

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

create index if not exists stock25_control_match_diagnostics_job_idx
  on public.stock25_control_match_diagnostics (control_job_id, event_date, positive_symbol);

alter table public.stock25_control_match_diagnostics enable row level security;

comment on table public.stock25_control_match_diagnostics is
  'Records why a positive event received fewer than the requested number of strict matched controls.';
