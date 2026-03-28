# Predictive Engine v3

## Summary
v3 upgrades the prior predictive framework to a decision-grade system by explicitly modeling identity reliability, persistence/censoring, inferability, and churn-vs-demand discrimination.

Core principle remains unchanged: demand is inferred from visible supply behavior, not directly observed.

## Key Changes

### 1. Identity model split (stable vs version)
- Stable lifecycle key: `stableOfferFingerprint` for continuity across mutable edits.
- Version key: `versionFingerprint` for mutation tracking.
- Persisted identity metadata: `identityStrategy`, `identityQualityScore`.
- Mutable edits (price/reliability/network) now increment mutation behavior instead of creating false disappearance events.

### 2. Persistence and censoring
- Persistent disappearance now uses configurable multi-bucket lookahead (`PERSISTENCE_BUCKETS`, default `3`) instead of 1-step lookahead.
- Distinguishes:
  - temporary missing
  - persistently disappeared
  - reappeared short gap
  - reappeared long gap
- Tracks:
  - `medianReappearanceDelayBuckets`
  - `reappearedShortGapRate`
  - `reappearedLongGapRate`
  - `persistentDisappearanceRateN`
  - `churnAdjustedDisappearanceRate`

### 3. New explicit regimes
- Added states:
  - `churn-dominated`
  - `non-inferable`
- Regime-aware suppression is applied to pressure interpretation, forecast confidence, price sensitivity, utilization, and recommendations.

### 4. Pressure and signal discrimination
- New explicit scores:
  - `churnScore`
  - `signalStrengthScore`
  - `inferabilityScore`
  - `identityQualityScore`
- Pressure now weights persistence/contraction evidence higher and discounts raw churn.

### 5. Weak-signal curve handling
- Consumption probability by price position is flattened toward neutral under weak signal/inferability.
- Price-position payload now includes uncertainty bands (`Low`/`High`) to avoid false precision.

### 6. Utilization/economics uncertainty
- Weak-signal and low-inferability regimes widen utilization ranges and add downside skew.
- Mean utilization is shrunk toward conservative baseline when inferability is low.
- ROI/payback ranges inherit these wider conservative intervals.

### 7. Recommendation hardening
- Explicit vetoes:
  - non-inferable -> no strong buy-like outcomes
  - churn-dominated -> speculative unless exceptional evidence
  - weak identity quality -> actionability suppression
- Recommendation policy now treats trustworthiness as a first-class gate before upside.

### 8. Backtest/label upgrade
- Backtest now stores sustained tightness and segmentation fields:
  - `realizedTightSustained`
  - `confidenceBucket`
  - `inferabilityBucket`
  - `stateAtPrediction`
- Reporting includes horizon/state/confidence/inferability breakout.

### 9. API/UI trust visibility
- Scoring API now surfaces trust and inferability signals in `currentState`/`confidence`.
- UI emphasizes warnings for `non-inferable` and `churn-dominated` regimes.
- Price curve now shows uncertainty band overlay.

## Compatibility Notes
- Legacy score outputs are retained for backward compatibility (`legacy` block and route compatibility fields).
- Existing architecture is preserved: ingestion -> recompute aggregates -> scoring API -> dashboard.

## Operational Notes
- New migration required: `20260328133000_predictive_engine_v3`.
- Recompute required after migration to populate v3 aggregate fields.

## v3.1 Corrective Patch (Trace Consistency)

v3.1 is a focused refinement that removes internal contradictions observed in churn-heavy cohorts (not a redesign).

### Persistence corrections
- Short-gap relists are no longer counted as persistent disappearance.
- Persistent disappearance uses evaluable denominator logic with right-censor protection near the series boundary.
- Churn-adjusted disappearance now explicitly discounts churn-like returns.

### Movement vs signal vs inferability
- `movementScore` now tracks market activity regardless of quality.
- `signalStrengthScore` now penalizes churn/noise more directly.
- `inferabilityScore` now applies hard churn/noise penalties and identity/depth support.
- State and inferability are aligned so `non-inferable` cannot coexist with high inferability output.

### Regime suppression propagation
- `churn-dominated` and `non-inferable` now suppress downstream math, not only recommendation.
- Suppression applies to:
  - pressure interpretation (toward neutral)
  - tightness probabilities
  - price-position consumption slope
  - utilization/economics range width and downside skew

### Recommendation and payload safety
- Veto path now emits explicit suppression metadata:
  - `forecastSuppressed`
  - `vetoReason` (`non_inferable`, `low_inferability`, `identity_quality`, `churn_dominated`)
- Payload confidence block mirrors suppression to avoid mixed messaging.
