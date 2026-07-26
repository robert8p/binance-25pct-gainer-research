-- V1.1.1: align the bucket with Supabase Free-plan object limits.
-- The application writes independently readable ZIP parts below 45,000,000 bytes.
update storage.buckets
set file_size_limit = 52428800
where id = 'binance-25pct-gainer-research';
