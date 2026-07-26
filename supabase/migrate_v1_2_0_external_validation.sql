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
