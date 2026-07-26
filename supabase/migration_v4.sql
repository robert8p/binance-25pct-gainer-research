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
