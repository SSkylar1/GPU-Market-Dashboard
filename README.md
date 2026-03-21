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
VAST_REQUEST_JSON='{"limit":100,"verified":{"eq":true},"rentable":{"eq":true},"rented":{"eq":false}}'
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

## Run app

```bash
npm run dev
```

Then open [http://localhost:3000](http://localhost:3000). The app redirects `/` to `/market`.

## Routes

- `/market` dashboard table for latest rollups
- `/gpus/[gpu]` GPU detail stub
- `/scoring` weighted scenario scoring view
- `/pricing` simple price band recommendations
- `/api/metrics` latest snapshot rollup JSON
- `/api/metrics/gpu/[gpu]` 24h bucketed trend points
- `/api/scores` score calculator endpoint

## Notes

- No secrets are committed.
- `scripts/collectSnapshots.ts` supports both `mock` and `vast` ingestion modes.
- Trend aggregation is bucketed at UTC half-hour boundaries (`:00` / `:30`).
- Trend rows are idempotent via uniqueness key (`gpuName`, `bucketStartUtc`, `source`).
