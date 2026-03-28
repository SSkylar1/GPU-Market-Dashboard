# Predictive Probability Engine (v2)

This project now treats marketplace snapshots as a visible-supply feed and infers demand pressure probabilistically from offer lifecycle behavior.

## Core layers

1. Offer lifecycle tracking
- Every offer is tracked by a deterministic fingerprint and/or provider external id.
- We persist lifecycle outcomes: first seen, last seen, total visible hours, disappearances, reappearances, and price edits.

2. Cohort state inference
- Cohorts are keyed by `(source, gpuName, numGpus, offerType)`.
- GPU-family combined cohorts `(numGpus=null, offerType=null)` are also computed.
- Each half-hour bucket computes pressure, depth, concentration, persistence, and confidence.
- Cohort state is classified as: `oversupplied`, `balanced`, `tightening`, `tight`, `volatile`, `thin-data`.

3. Probability forecasting
- Forecasts are deterministic + interpretable transforms:
  - `P(tight in 24h/72h/7d)`
  - `P(price up/flat/down in 24h)`
  - consumption probability conditional on relative price position
- Utilization and ROI are returned as uncertainty ranges and threshold probabilities.

## Shrinkage / pooling

Exact cohorts are preserved and forecasted directly.
For thin exact cohorts, pressure and hazard are shrunk toward the family baseline with effective sample-size weighting.

## Explainability

All outputs include:
- observed signals
- inferred state and hazards
- forecasted probabilities
- confidence and risk flags

## Backtesting hooks

`ForecastBacktest` stores realized outcomes against prior predictions.
`scripts/backtestForecasts.ts` reports Brier and calibration summaries.
