# Identity and Persistence v3

## Identity

### Stable identity
Purpose: lifecycle stitching and continuity.

Priority:
1. external ID (`external_id`)
2. machine signature (`machine_signature`)
3. host signature (`host_signature`)
4. weak fallback (`weak_signature`)

Stable identity excludes mutable fields (price/reliability/network throughput) to prevent false exits/relistings.

### Version identity
Purpose: capture mutable offer changes while preserving lifecycle continuity.

Version key includes mutable state. Version changes are counted as mutations (`mutationCount`) and not treated as disappearances.

### Persisted fields
- Offer:
  - `stableOfferFingerprint`
  - `versionFingerprint`
  - `identityStrategy`
  - `identityQualityScore`
- Offer lifecycle:
  - `stableOfferFingerprint`
  - `latestVersionFingerprint`
  - `identityStrategy`
  - `identityQualityScore`
  - `mutationCount`

## Persistence and censoring

### Multi-bucket persistence rule
A disappearance is considered persistent only when absent for at least `PERSISTENCE_BUCKETS` future buckets.

Defaults:
- `PERSISTENCE_BUCKETS=3`
- `SHORT_GAP_MAX_BUCKETS=2`

### Outcomes
- `persistently_disappeared`
- `temporarily_missing`
- `reappeared_short_gap`
- `reappeared_long_gap`

### Metrics derived
- `temporaryMissingRate`
- `reappearedShortGapRate`
- `reappearedLongGapRate`
- `medianReappearanceDelayBuckets`
- `persistentDisappearanceRateN`
- `churnAdjustedDisappearanceRate`

### v3.1 persistence corrections
- Right-censoring is handled at series tail: events without enough future buckets to evaluate persistence are not counted as persistent exits.
- Persistent and temporary-missing rates are normalized by evaluable disappeared events (not broad prior offer count), which keeps rates interpretable during high churn.
- Short-gap relists explicitly reduce churn-adjusted disappearance and no longer inflate tightness proxies.

### Signal semantics tie-in
- High reappearance can raise `movementScore` while lowering `signalStrengthScore` and `inferabilityScore`.
- Identity quality contributes as support, but cannot fully override churn/noise penalties.

## Why this matters
- Reduces false disappearance spikes caused by mutable listing edits.
- Improves churn-vs-demand separation.
- Makes survival-style interpretation less biased by short transient relisting gaps.
- Supports inferability-aware recommendation safety.

## Debug workflow
Use:
- `npm run debug:identity -- <source> <take>`

Debug output now includes:
- strategy usage rates (external/machine/host/weak)
- stable/version mismatch counts
- suspected collision rate
- suspected split rate
- mutation rate
- mean identity quality
