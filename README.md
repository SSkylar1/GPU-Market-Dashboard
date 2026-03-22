# GPU Market Dashboard (MVP)

Internal Next.js + TypeScript dashboard for local GPU marketplace analysis.

## Stack

- Next.js (App Router)
- TypeScript
- Tailwind CSS
- Prisma 7
- PostgreSQL + `@prisma/adapter-pg` + `pg`

## Prerequisites

- Node.js 20.9+
- PostgreSQL running locally or remotely
- `DATABASE_URL` set in `.env`

Example `.env` entry:

```bash
DATABASE_URL="postgresql://user:password@localhost:5432/gpu_market_dashboard?schema=public"
```

## Install

```bash
npm install
```

## Prisma setup

Generate client:

```bash
npm run db:generate
```

Run migrations:

```bash
npm run db:migrate
```

## Seed mock snapshot data

```bash
npm run collect
```

`collect` defaults to mock mode. For live ingestion:

```bash
INGEST_MODE=vast npm run collect
```

Live mode required env:

```bash
# optional override; defaults to Vast bundles endpoint
VAST_API_URL="https://console.vast.ai/api/v0/bundles/"
# optional override; defaults to POST
VAST_API_METHOD="POST"
# required for authenticated requests
VAST_API_KEY="your-token"
# optional JSON body override for POST requests
VAST_REQUEST_JSON='{"limit":100,"type":"on-demand","verified":{"eq":true}}'
```

## Recompute GPU rollups

```bash
npm run recompute
```

## Pipeline status

```bash
npm run status
```

## Install 30-minute cron

```bash
npm run cron:install
```

Cron logs:

```bash
tail -f /tmp/gpu-market-dashboard-cron.log
```

## Run every 30 minutes in GitHub Actions

Workflow file:

- `.github/workflows/market-pipeline.yml`

It runs on a UTC half-hour schedule and supports manual runs via `workflow_dispatch`.

Set these repository secrets in GitHub (`Settings` -> `Secrets and variables` -> `Actions`):

- `DATABASE_URL` (required)
- `VAST_API_KEY` (required)
- `VAST_API_URL` (optional, defaults in app code)
- `VAST_API_METHOD` (optional, defaults in app code)
- `VAST_REQUEST_JSON` (optional)

For the provided workflow, only `DATABASE_URL` and `VAST_API_KEY` are used by default. Keep optional override secrets unset unless you need custom endpoint/query behavior.

Important:

- GitHub-hosted runners must be able to reach your Postgres host from the public internet.
- If your DB is local-only (for example `localhost` on your Mac), use local cron or a self-hosted runner instead.

## Run app

```bash
npm run dev
```

Then open [http://localhost:3000](http://localhost:3000). The app redirects `/` to `/market`.

## Routes

- `/market` dashboard table for latest rollups
- `/gpus/[gpu]` GPU detail with 24h trend + latest host/machine breakdown
- `/scoring` weighted scenario scoring view
- `/pricing` simple price band recommendations
- `/api/metrics` latest snapshot rollup JSON
- `/api/metrics/gpu/[gpu]` 24h bucketed trend points + latest host/machine breakdown
- `/api/scores` score calculator endpoint

## Notes

- No secrets are committed.
- `scripts/collectSnapshots.ts` supports both `mock` and `vast` ingestion modes.
- Trend aggregation is bucketed at UTC half-hour boundaries (`:00` / `:30`).
- Trend rows are idempotent via uniqueness key (`gpuName`, `bucketStartUtc`, `source`).
- Offer records persist Vast `host_id` / `machine_id` when present for supplier concentration analysis.
