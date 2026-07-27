# V1.3 memory-safe resumable upgrade

Use the existing 25% Supabase project, GitHub repository and Render services. This upgrade preserves all prior scans, events, controls and files.

## 1. Suspend the old worker

In Render, suspend `binance-25pct-scanner-worker` before applying the database migration or replacing the code.

Do not delete the current job.

## 2. Run the Supabase migration

Open **Supabase → SQL Editor → New query** and run the complete contents of:

```text
supabase/migrate_v1_3_0_memory_resume.sql
```

The migration adds:

- checkpoint and resume metadata to all three long-running stages;
- event-level matched-control progress;
- durable sample-level evaluation features;
- durable source-manifest rows;
- a safe checkpoint bootstrap for a partially completed V1.2 scan.

A successful migration reports `Success. No rows returned`.

## 3. Replace the GitHub files

Extract the V1.3 ZIP. Upload everything inside it to the root of the existing private repository and replace the previous files.

Commit with:

```text
Upgrade to V1.3 memory-safe resumable validation
```

## 4. Preserve the Render worker size

The V1.3 `render.yaml` deliberately does not specify a worker plan, so it will not force the worker back to Starter.

Keep the worker on the Pro instance you already selected. The web service can remain Free.

The worker uses the existing 10 GB persistent disk at `/var/data`.

## 5. Redeploy

Deploy the latest commit to both the web service and worker. Resume the worker if Render leaves it suspended.

No new secret is required. The Blueprint includes these non-secret settings:

```text
MAX_AUTO_RESUMES=8
MINIMUM_DISK_FREE_BYTES=750000000
PERSIST_EVENT_AGG_TRADES=false
```

## 6. Verify

Open:

```text
https://YOUR-WEB-SERVICE.onrender.com/health
```

Expected fields include:

```json
{
  "status": "ok",
  "version": "1.3.0",
  "execution_model": "memory_bounded_resumable"
}
```

The worker log should show:

```text
V1.3 memory-safe resumable C2/C4 external-validation worker started
```

## 7. Resume the interrupted job

### Current job still says `running`

The new worker automatically converts it to `queued`, increments the resume count and continues from the last checkpoint.

### Current job says `failed`

Open the dashboard. The failed row now has a **Resume** button. Click it once.

For a V1.2 scan, V1.3 can resume from the recorded `symbols_processed` position and existing symbol snapshot.

V1.2 did not persist matched-control events or evaluation samples incrementally. Therefore, a V1.2 failure in Step 2 or Step 3 may need to repeat that current stage once. After V1.3 begins writing checkpoints, any later restart resumes rather than starting the stage again.

## 8. Watch the checkpoint columns

The dashboard now shows:

- `Checkpoint` — current stage and last completed symbol/event;
- `Resumes` — number of automatic or manual resumptions;
- progress counts that persist across worker restarts.

The worker logs also emit periodic resource checkpoints containing memory and disk headroom.

## 9. Continue the locked workflow

1. Complete the fixed-period scan.
2. Build matched controls.
3. Run the frozen C2/C4 evaluation.
4. Download `external_validation_index.zip` and every `external_validation_features...zip` part.
5. Upload all parts to ChatGPT together.

Do not open the old sealed-test ZIP and do not change C2/C4 thresholds.
