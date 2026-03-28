# 1. Repo map for prediction engine

## ingestion
- Path: `scripts/collectSnapshots.ts`
  - Responsibility: Collect marketplace offers (`mock` or `vast`), compute offer identity, collision diagnostics, persist `MarketSnapshot` + `Offer` rows.
  - Why it matters to model correctness: Identity fidelity here determines lifecycle stitching correctness and whether disappearance/reappearance is demand-like behavior or identity noise.
- Path: `src/lib/vast/collector` (called by collect script)
  - Responsibility: Raw source extraction and normalization into `SnapshotOfferInput` fields.
  - Why it matters: Field completeness (machine/host/price/reliability) controls fingerprint robustness and feature completeness.

## offer identity / lifecycle
- Path: `src/lib/metrics/offerIdentity.ts`
  - Responsibility: Deterministic identity (`buildOfferIdentity`) and collision diagnostics (`detectFingerprintCollisions`).
  - Why it matters: Directly controls false merge / false split rates in lifecycle and hazard estimates.
- Path: `scripts/recomputeMetrics.ts`
  - Responsibility: `inferOfferFingerprint`, lifecycle stitching, segment creation, reappearance counting, persistence bookkeeping.
  - Why it matters: This is where observed visibility transitions become inferred consumption/tightness signals.
- Path: `src/lib/metrics/transitions.ts`
  - Responsibility: Generic transition/segmentation helpers used by tests and reusable transition logic.
  - Why it matters: Defines expected semantics for continuing/new/disappeared/reappeared and continuous segment boundaries.

## metric recompute / intelligence
- Path: `scripts/recomputeMetrics.ts`
  - Responsibility: Per-bucket exact/family cohort feature generation, pressure/state/confidence, shrinkage, forecast rows, backtest rows.
  - Why it matters: Core transformation from snapshots to model-ready inference and labels.
- Path: `src/lib/metrics/intelligence.ts`
  - Responsibility: Core deterministic math: pressure, state classifier, confidence, shrinkage, probabilities, utilization/economics, recommendation, calibration/noise helpers.
  - Why it matters: This file contains the model equations and decision rules reviewers need to critique.

## forecasting / scenario scoring
- Path: `src/lib/scoring/marketScore.ts`
  - Responsibility: Scenario scoring pipeline (load cohort/family, infer state/confidence, forecast probs, utilization/economics, recommendation, risk flags, persistence).
  - Why it matters: This is the production scoring path that users hit and where weak-signal smoothing/overconfidence can appear.
- Path: `src/lib/scoring/score.ts`
  - Responsibility: Legacy weighted score (`Buy/Watch/Avoid`) retained for compatibility.
  - Why it matters: Legacy logic still ships in payload and can create policy inconsistency versus predictive recommendation.

## API response shaping
- Path: `src/app/api/scoring/route.ts`
  - Responsibility: Input validation, invoke scoring, return predictive + legacy compatibility fields, metadata endpoint for UI options.
  - Why it matters: Determines what downstream consumers interpret as observed/inferred/forecasted.
- Path: `src/app/api/metrics/route.ts`
  - Responsibility: Legacy market rollup endpoint with recommendation/regime synthesis.
  - Why it matters: Still operational and mixes older/legacy assumptions; can diverge from predictive engine semantics.

## chart transforms / UI display
- Path: `src/app/scoring/page.tsx`
  - Responsibility: Scoring form, forecast dashboard, chart rendering, observed/inferred/forecasted explanation surface, drill-down tables.
  - Why it matters: Controls whether uncertainty and provenance are communicated or blurred.
- Path: `src/lib/scoring/chartTransforms.ts`
  - Responsibility: Time-range filtering for chart series.
  - Why it matters: Affects interpretation of trend persistence and short-window behavior.

---

# 2. Core code excerpts

## offer identity / fingerprint generation
- File: `src/lib/metrics/offerIdentity.ts`
- Function: `buildOfferIdentity`

```ts
export function buildOfferIdentity(input: OfferIdentityInput): OfferIdentityResult {
  if (input.offerExternalId && input.offerExternalId.trim().length > 0) {
    return {
      offerExternalId: input.offerExternalId,
      fingerprint: `ext:${input.source}:${input.offerExternalId.trim()}`,
      strategy: "external_id",
    };
  }

  const normalizedType = normalizeOfferType(input.offerType);
  const common = [
    input.source,
    input.gpuName.trim().toLowerCase(),
    String(input.numGpus),
    normalizedType,
    String(input.gpuRamGb ?? "na"),
    String(input.cpuCores ?? "na"),
    String(input.ramGb ?? "na"),
    input.verified == null ? "na" : input.verified ? "v1" : "v0",
    round(input.reliabilityScore, 3),
    round(input.pricePerHour, 4),
    round(input.inetDownMbps, 0),
    round(input.inetUpMbps, 0),
  ];

  if (input.machineId != null) {
    return {
      offerExternalId: null,
      fingerprint: `mach:${hashSegments([`m${input.machineId}`, ...common])}`,
      strategy: "machine_signature",
    };
  }

  if (input.hostId != null) {
    return {
      offerExternalId: null,
      fingerprint: `host:${hashSegments([`h${input.hostId}`, ...common])}`,
      strategy: "host_signature",
    };
  }

  return {
    offerExternalId: null,
    fingerprint: `weak:${hashSegments(common)}`,
    strategy: "weak_signature",
  };
}
```

- What it is doing:
  - Prefer stable external offer ID if available.
  - Otherwise create signature hash from source + hardware + reliability/price/network fields.
  - Uses stronger strategy tiers: machine -> host -> weak signature.
  - Includes price/reliability in fingerprint, making identity sensitive to edits when no external ID.
- TODO/FIXME signals: none in code.

---

## lifecycle stitching / reappearance detection
- File: `scripts/recomputeMetrics.ts`
- Functions: `inferOfferFingerprint` + main lifecycle loop in `main`

```ts
function inferOfferFingerprint(offer: Offer, source: string): string {
  if (offer.offerFingerprint && offer.offerFingerprint.trim().length > 0) {
    return offer.offerFingerprint;
  }

  return buildOfferIdentity({
    source,
    offerExternalId: offer.offerExternalId ?? offer.offerId,
    machineId: offer.machineId,
    hostId: offer.hostId,
    gpuName: offer.gpuName,
    numGpus: offer.numGpus,
    offerType: offer.offerType,
    gpuRamGb: offer.gpuRamGb,
    cpuCores: offer.cpuCores,
    ramGb: offer.ramGb,
    reliabilityScore: offer.reliabilityScore,
    verified: offer.verified,
    pricePerHour: offer.pricePerHour,
    inetDownMbps: offer.inetDownMbps,
    inetUpMbps: offer.inetUpMbps,
  }).fingerprint;
}

// in main loop
const existing = lifecycleMap.get(lifecycleKey);
if (!existing) {
  lifecycleMap.set(lifecycleKey, { ... firstSeenAt/segment init ... });
} else {
  const gap = i - existing.lastSeenSnapshotIndex;
  if (gap > 1) {
    existing.disappearanceCount += 1;
    existing.reappearanceCount += 1;
    existing.lastStatus = "reappeared";
    existing.segments.push({ ... endedBy: "disappeared" ... });
    existing.currentSegmentStart = snapshot.capturedAt;
  }

  existing.totalVisibleSnapshots += 1;
  existing.totalVisibleHours += Math.max(0, Math.min(hoursSinceLastSeen, 1.5));
  ...
  existing.lastStatus = "active";
}
```

- What it is doing:
  - Guarantees every offer has an identity, reusing persisted fingerprint when present.
  - Reappearance is inferred from snapshot index gap (`gap > 1`) for same fingerprint.
  - Closes a segment on disappearance and opens a new one on reappearance.
  - Caps per-step visible-hour accumulation to `1.5` hours to limit irregular snapshot interval distortion.
- TODO/FIXME signals: none.

---

## persistent disappearance logic
- File: `scripts/recomputeMetrics.ts`
- Function block: per-cohort transition computation in `main`

```ts
const nextSet = nextIdentityByCohort.get(cohort) ?? new Set<string>();
for (const id of prevSet) {
  if (!currentSet.has(id)) {
    disappearedOffers += 1;
    if (!nextSet.has(id)) persistentDisappearedOffers += 1;
  }
}
```

- What it is doing:
  - Defines disappearance from `prev -> current` absence.
  - Defines persistence using one-step lookahead (`current missing` and `also missing in next snapshot`).
  - Prevents immediate one-bucket flicker from being treated as persistent consumption.
- TODO/FIXME signals: none.

---

## cohort pressure calculation
- File: `src/lib/metrics/intelligence.ts`
- Function: `computeCohortPressureScore`

```ts
const supplyTightnessScore = clamp(
  0.45 * input.persistentDisappearanceRate +
    0.2 * input.disappearedRate +
    0.15 * (1 - input.newOfferRate) +
    0.2 * input.lowBandPersistentDisappearedRate,
  0,
  1,
);

const machineDepthScore = clamp(safeDiv(input.uniqueMachineCount, 24));
const concentrationPenalty = clamp(input.machineConcentrationShareTop3);
const concentrationScore = 1 - concentrationPenalty;
const priceSignal = clamp((input.medianPriceChangePct + 0.08) / 0.2);
const availabilitySignal = clamp(1 - input.rentableShare);

const rawPressure =
  100 *
  clamp(
    0.34 * supplyTightnessScore +
      0.16 * priceSignal +
      0.16 * availabilitySignal +
      0.18 * machineDepthScore +
      0.16 * concentrationScore,
    0,
    1,
  );

const pressureAcceleration = rawPressure - input.priorPressure;
const pressurePersistence = mean([rawPressure, input.priorPressure, input.priorPressure2]);
```

- What it is doing:
  - Builds tightness from persistent disappearance, gross disappearance, low new supply, and low-price-band persistence.
  - Balances pressure with price move, available share, machine depth, concentration penalty.
  - Outputs pressure level, acceleration, and short persistence average.
- TODO/FIXME signals: none.

---

## state classification
- File: `src/lib/metrics/intelligence.ts`
- Function: `classifyCohortState`

```ts
if (input.dataDepthScore < 35) return "thin-data";
if (input.noiseScore > 60 || input.reappearedRate > 0.35) return "volatile";
if (input.cohortPressureScore >= 72 && input.persistentDisappearanceRate >= 0.2) return "tight";
if (input.cohortPressureScore >= 58 || input.pressureAcceleration >= 4) return "tightening";
if (input.cohortPressureScore <= 38 && input.persistentDisappearanceRate < 0.08) return "oversupplied";
return "balanced";
```

- What it is doing:
  - Prioritizes insufficient depth and volatility guardrails before tight/oversupplied states.
  - Requires both high pressure and minimum persistence for `tight`.
  - Allows acceleration-only trigger into `tightening`.
- TODO/FIXME signals: none.

---

## confidence calculation
- File: `src/lib/metrics/intelligence.ts`
- Function: `computeStateConfidence`

```ts
const historyScore = clamp(safeDiv(input.historyDepth, 48));
const machineScore = clamp(safeDiv(input.machineCount, 16));
const qualityPenalty = clamp((input.noiseScore + input.reappearedRate * 100) / 140);

return 100 * clamp(
  0.4 * (input.dataDepthScore / 100) +
  0.3 * historyScore +
  0.3 * machineScore -
  0.35 * qualityPenalty,
  0,
  1,
);
```

- What it is doing:
  - Combines current depth, historical depth, and machine count.
  - Penalizes noisy/reappearing cohorts.
  - Normalizes to 0-100.
- TODO/FIXME signals: none.

---

## config-family shrinkage
- File: `src/lib/metrics/intelligence.ts`
- Function: `shrinkTowardsFamily`

```ts
const weight = clamp(safeDiv(effectiveSampleSize, effectiveSampleSize + 24));
return exactValue * weight + familyValue * (1 - weight);
```

- File: `scripts/recomputeMetrics.ts`
- Function block: post-bucket shrinkage application

```ts
bucket.configVsFamilyPressureDelta = bucket.cohortPressureScore - family.cohortPressureScore;
bucket.configVsFamilyPriceDelta = ...
bucket.configVsFamilyHazardDelta = bucket.persistentDisappearanceRate - family.persistentDisappearanceRate;

bucket.cohortPressureScore = shrinkTowardsFamily(
  bucket.cohortPressureScore,
  family.cohortPressureScore,
  bucket.totalOffers,
);
```

- What it is doing:
  - Stores raw exact-vs-family deltas, then shrinks exact pressure toward family by sample size.
  - Uses `totalOffers` as effective sample size.
  - Shrinkage currently applied to pressure in recompute; hazard stays as delta feature but not fully shrunk there.
- TODO/FIXME signals: none.

---

## utilization estimation
- File: `src/lib/metrics/intelligence.ts`
- Function: `estimateExpectedUtilization` (distribution path)

```ts
const stateBase = { tight: 0.72, tightening: 0.58, balanced: 0.46, oversupplied: 0.29, volatile: 0.38, "thin-data": 0.34 };

const meanUtil = clamp(
  stateBase[input.cohortState] +
    (input.pressure - 50) / 180 -
    input.relativePricePosition * 0.28 +
    (input.machineDepthScore - 50) / 300 +
    (input.concentrationScore - 50) / 350 +
    input.configVsFamilyHazardDelta / 120 +
    reliability,
  0.03,
  0.97,
);

const spread = clamp(0.25 - input.confidenceScore / 400 + Math.abs(input.relativePricePosition) * 0.08, 0.07, 0.32);

return {
  expectedUtilization: meanUtil,
  expectedUtilizationLow: clamp(meanUtil - spread),
  expectedUtilizationHigh: clamp(meanUtil + spread),
  pUtilizationAbove25: clamp((meanUtil - 0.12) / 0.88),
  pUtilizationAbove50: clamp((meanUtil - 0.35) / 0.65),
  pUtilizationAbove75: clamp((meanUtil - 0.62) / 0.38),
};
```

- What it is doing:
  - Uses state-based baseline and additive adjustments for pressure/price/depth/concentration/hazard/reliability.
  - Confidence controls uncertainty band width.
  - Converts mean utilization to threshold probabilities by linear ramps.
- TODO/FIXME signals: none.

---

## price-position vs consumption logic
- File: `src/lib/metrics/intelligence.ts`
- Function: `estimateConsumptionProbability`

```ts
const stateBias = { tight: 0.9, tightening: 0.5, balanced: 0.1, oversupplied: -0.6, volatile: -0.1, "thin-data": -0.25 };

const relPricePenalty = input.relativePricePosition * 1.25;
const reliability = input.reliabilityScore == null ? 0 : (input.reliabilityScore - 0.95) / 0.08;
const horizonScale = Math.log(Math.max(2, input.hours)) / Math.log(12);

const z = stateBias[input.cohortState] +
  (input.pressure - 50) / 20 -
  relPricePenalty +
  reliability +
  0.28 * horizonScale;

return clamp(logistic(z));
```

- What it is doing:
  - Logistic consumption probability with state, pressure, relative price, reliability, and horizon effects.
  - Higher relative price penalizes consumption speed; longer horizon increases probability.
  - Uses exact-cohort-relative price in scoring flow.
- TODO/FIXME signals: none.

---

## forecast probability generation
- File: `src/lib/metrics/intelligence.ts`
- Function: `forecastProbabilitiesFromState`

```ts
const base =
  tightBiasByState[input.state] +
  (input.pressure - 50) / 22 +
  input.pressureAcceleration / 18 +
  input.configVsFamilyDelta / 20;

const confidenceDamp = 0.35 + 0.65 * (input.confidenceScore / 100);

const pTight24h = clamp(logistic(base) * confidenceDamp + 0.08 * (1 - confidenceDamp));
const pTight72h = clamp(logistic(base * 0.82) * confidenceDamp + 0.1 * (1 - confidenceDamp));
const pTight7d = clamp(logistic(base * 0.62) * confidenceDamp + 0.12 * (1 - confidenceDamp));

const priceLift = clamp(logistic((input.pressure - 55) / 18 + input.pressureAcceleration / 20) * confidenceDamp);
const pPriceUp24h = clamp(0.15 + priceLift * 0.7);
const pPriceDown24h = clamp(0.15 + (1 - priceLift) * 0.45);
const pPriceFlat24h = clamp(1 - pPriceUp24h - pPriceDown24h);
```

- What it is doing:
  - Creates tightness/price-direction probabilities from state + pressure dynamics.
  - Dampens extremes when confidence is low, pulling toward prior-like floors.
  - Uses hand-tuned coefficients, not learned calibration tables yet.
- TODO/FIXME signals: none.

---

## recommendation logic
- File: `src/lib/metrics/intelligence.ts`
- Function: `buildRecommendationDistribution` (via `buildRecommendation`)

```ts
if (input.pPaybackWithinTarget >= 0.7 && input.expectedUtilization >= 0.6 && input.confidenceScore >= 65) return Buy;
if (input.pPaybackWithinTarget >= 0.55 && input.expectedUtilization >= 0.5 && input.confidenceScore >= 50) return Buy if discounted;
if (input.confidenceScore < 45 || input.cohortState === "thin-data") return Speculative;
if (input.downsideRisk > 0.6 || input.concentrationRisk > 0.55 || input.cohortState === "oversupplied") return Avoid;
return Watch;
```

- What it is doing:
  - Uses threshold policy on payback/utilization/confidence then risk overrides.
  - Explicitly downgrades thin-data to `Speculative`.
  - Concentration and downside can force `Avoid`.
- TODO/FIXME signals: none.

---

## risk flag generation
- File: `src/lib/scoring/marketScore.ts`
- Function block: `scenarioForecast.create`

```ts
riskFlags: {
  oversupply: state === "oversupplied",
  concentration: (latestExact.machineConcentrationShareTop3 ?? 0) > 0.6,
  thinData: state === "thin-data",
  volatile: state === "volatile",
  weakPriceSupport: forecast.pPriceDown24h > forecast.pPriceUp24h,
},
```

- What it is doing:
  - Sets binary risk flags based on state and thresholded concentration/price-direction.
  - Flags are simple booleans; no graded severity.
- TODO/FIXME signals: none.

---

## API payload shaping for observed / inferred / forecasted sections
- File: `src/lib/scoring/marketScore.ts`
- Function: `scoreScenarioWithMarket` return payload

```ts
explanation: {
  observed: [
    `Visible offers: ${latestExact.totalOffers}`,
    `Unique machines: ${latestExact.uniqueMachines ?? 0}`,
    `Median visible price: ...`,
  ],
  inferred: [
    `Cohort state inferred as ${state}`,
    `Persistent disappearance rate ...`,
    `Config-vs-family pressure delta ...`,
  ],
  forecasted: [
    `P(tight 24h) ...`,
    `P(consumed in 24h) ...`,
    `Expected utilization ...`,
  ],
  risks: [ ... ],
},
```

- File: `src/app/scoring/page.tsx`
- UI section: “Observed vs Inferred vs Forecasted”

```tsx
<h3>Observed vs Inferred vs Forecasted</h3>
<p>Observed</p>
<ul>{result.explanation.observed?.map(...)} </ul>
<p>Inferred</p>
<ul>{result.explanation.inferred?.map(...)} </ul>
<p>Forecasted</p>
<ul>{result.explanation.forecasted?.map(...)} </ul>
```

- What it is doing:
  - Payload explicitly separates these categories.
  - UI renders each category separately, preserving epistemic separation at presentation layer.
- TODO/FIXME signals: none.

---

# 3. Actual formulas and decision rules

## cohort pressure
Implemented in `computeCohortPressureScore`:
- `supplyTightness = clamp(0.45*persistentDisappearanceRate + 0.2*disappearedRate + 0.15*(1-newOfferRate) + 0.2*lowBandPersistentDisappearedRate, 0,1)`
- `machineDepth = clamp(uniqueMachineCount / 24, 0,1)`
- `concentrationScore = 1 - clamp(machineConcentrationShareTop3,0,1)`
- `priceSignal = clamp((medianPriceChangePct + 0.08)/0.2, 0,1)`
- `availabilitySignal = clamp(1-rentableShare, 0,1)`
- `cohortPressure = 100 * clamp(0.34*supplyTightness + 0.16*priceSignal + 0.16*availabilitySignal + 0.18*machineDepth + 0.16*concentrationScore,0,1)`
- `pressureAcceleration = cohortPressure - priorPressure`
- `pressurePersistence = mean([cohortPressure, priorPressure, priorPressure2])`

Clamps/floors/caps:
- Every subcomponent clamped to `[0,1]`.
- Final pressure clamped then scaled to `[0,100]`.

Neutral/defaults:
- Missing prior pressures default to `50` in recompute.

## supply tightness
Implemented as component above:
- `supplyTightnessScore(0..100) = 100 * clamp(0.45*persistent + 0.2*disappeared + 0.15*(1-newRate) + 0.2*lowBandPersistent, 0,1)`

## concentration
- Pressure path: `concentrationScore(0..100) = 100*(1 - clamp(machineConcentrationShareTop3))`
- Concentration shares computed from machine/host ID counts in `countTopShares`:
  - `top1 = largestCount/totalOffers`
  - `top3 = (largest3CountsSum)/totalOffers`

## persistence / acceleration
- `pressureAcceleration = currentPressure - priorPressure`
- `pressurePersistence = mean([currentPressure, priorPressure, priorPressure2])`
- Persistent disappearance per bucket:
  - `persistentDisappearanceRate = persistentDisappearedOffers / max(prevCount,1)`
  - `persistentDisappearedOffers` requires absence in current and in next snapshot.

## confidence score
Implemented in `computeStateConfidence`:
- `historyScore = clamp(historyDepth/48)`
- `machineScore = clamp(machineCount/16)`
- `qualityPenalty = clamp((noiseScore + reappearedRate*100)/140)`
- `confidence = 100*clamp(0.4*(dataDepth/100) + 0.3*historyScore + 0.3*machineScore - 0.35*qualityPenalty,0,1)`

Data depth in recompute:
- `dataDepth = 100*min(1, 0.55*(totalOffers/50) + 0.25*(uniqueMachines/20) + 0.2*(historyLen/48))`

Noise score:
- `noise = 100*clamp(2.4*std(disappearedRateSeries) + std(priceSeries) + 1.2*mean(reappearedRateSeries),0,1.2)`

## shrinkage weighting
- `weight = clamp(effectiveSampleSize/(effectiveSampleSize+24))`
- `shrunkValue = exact*weight + family*(1-weight)`

Where used:
- Recompute: pressure is shrunk with `effectiveSampleSize = bucket.totalOffers`.
- Scoring runtime: shrunk pressure and shrunk hazard are recomputed from latest exact/family rows.

## utilization
Distribution path (`estimateExpectedUtilizationDistribution`):
- `meanUtil = clamp(stateBase + (pressure-50)/180 - relPrice*0.28 + (machineDepthScore-50)/300 + (concentrationScore-50)/350 + hazardDelta/120 + reliabilityAdj, 0.03, 0.97)`
- `spread = clamp(0.25 - confidence/400 + abs(relPrice)*0.08, 0.07, 0.32)`
- `low/high = clamp(meanUtil ± spread)`
- Threshold probabilities linearized:
  - `pAbove25 = clamp((meanUtil-0.12)/0.88)`
  - `pAbove50 = clamp((meanUtil-0.35)/0.65)`
  - `pAbove75 = clamp((meanUtil-0.62)/0.38)`

## consumption probability by price position
`estimateConsumptionProbability`:
- `z = stateBias + (pressure-50)/20 - (relativePricePosition*1.25) + reliabilityAdj + 0.28*log_base12(max(2,hours))`
- `pConsumedWithinH = clamp(sigmoid(z))`

Defaults:
- Reliability missing -> `0` adjustment.

## payback probability / economics distribution
`estimateRoiContextDistribution`:
- `dailyPowerCost = (watts/1000)*24*electricityCost`
- `revenue = utilMean*price*24` (plus low/high variants)
- `margin = revenue - dailyPowerCost` (plus low/high)
- `paybackMonths = hardwareCost / margin / 30.4375` if `margin > 0`, else `null`
- `targetGap = (targetMonths - paybackMonths) / max(targetMonths,1)`
- `pPaybackWithinTarget = clamp(sigmoid(targetGap*4))`
- Override: if low-bound payback exists and `<= target`, set probability floor `>=0.7`.

## recommendation thresholds / logic
`buildRecommendationDistribution`:
- `Buy`: `pPayback>=0.70 && expectedUtil>=0.60 && confidence>=65`
- `Buy if discounted`: `pPayback>=0.55 && expectedUtil>=0.50 && confidence>=50`
- `Speculative`: `confidence<45 || state==thin-data`
- `Avoid`: `downsideRisk>0.6 || concentrationRisk>0.55 || state==oversupplied`
- else `Watch`

Downside risk in scoring:
- `downsideRisk = clamp(1 - pPaybackWithinTarget + (state==oversupplied ? 0.15 : 0))`

---

# 4. Thin-data and churn handling audit

## thin cohorts
- Code location: `src/lib/metrics/intelligence.ts::classifyCohortState`, `if dataDepthScore < 35 => thin-data`.
- Code location: `buildRecommendationDistribution`, thin-data forces `Speculative`.
- Code location: `scripts/recomputeMetrics.ts`, pressure shrinkage with sample-size weighting.
- Assessment: **partial-to-strong**.
  - Strong: explicit thin-data state + recommendation downgrade.
  - Partial: no explicit “structurally non-inferable” state; only depth threshold.

## low machine depth
- Code location: `computeCohortPressureScore`, `machineDepthScore = clamp(uniqueMachineCount/24)`.
- Code location: `computeStateConfidence`, machine count contributes to confidence.
- Assessment: **partial**.
  - Present as continuous signal; no hard regime rejection for extreme low machine count.

## repeated disappearance/reappearance
- Code location: `scripts/recomputeMetrics.ts`, `reappearedOffers += 1` when new in current but seen before.
- Code location: `classifyCohortState`, `reappearedRate > 0.35 => volatile`.
- Code location: `computeStateConfidence`, reappearance penalizes confidence.
- Assessment: **strong** for state/confidence gating; **partial** for deeper censoring.

## high churn with flat supply
- Code location: `computeNoiseScore` (std of disappeared rate + price + mean reappears).
- Code location: `classifyCohortState` volatile threshold.
- Assessment: **partial**.
  - Detects noisy regimes; does not explicitly detect “high churn but zero net contraction” as separate non-demand regime.

## non-persistent disappearance
- Code location: recompute persistent logic via one-step lookahead.
- Assessment: **partial**.
  - Better than raw disappearances, but one-step lookahead may be too short for relisting delay dynamics.

## anomaly / noisy regime detection
- Code location: `computeNoiseScore` + `classifyCohortState` (`noiseScore > 60` -> volatile).
- Assessment: **partial**.
  - Noise metric exists; thresholds are hand-tuned and uncalibrated by source.

## low-confidence shrinkage
- Code location: `shrinkTowardsFamily` in recompute and runtime scoring.
- Code location: `forecastProbabilitiesFromState`, low confidence dampens probabilities toward floors.
- Assessment: **strong for smoothing**, **partial for inferability gating**.
  - Low confidence dampens but still outputs probabilities and recommendations.

---

# 5. Known risk points in current implementation

1. Risk: Fingerprint sensitivity to mutable fields (price/reliability/network)
- Severity: high
- File/function: `src/lib/metrics/offerIdentity.ts::buildOfferIdentity`
- Why it matters: price edit can create new fingerprint when no external ID, inflating churn/reappearance and biasing hazard.
- Evidence: `common` includes rounded `pricePerHour`, `reliabilityScore`, `inetDown/Up`.

2. Risk: Persistent disappearance uses only one future snapshot
- Severity: high
- File/function: `scripts/recomputeMetrics.ts` persistent logic (`if !nextSet.has(id)`) 
- Why it matters: delayed reappearance beyond one bucket is treated as persistent consumption.
- Evidence: single-step lookahead set from immediate `nextSnapshot` only.

3. Risk: Recompute does full table delete/rebuild
- Severity: medium
- File/function: `scripts/recomputeMetrics.ts` `deleteMany({})` on trend/forecast/backtest/lifecycle tables.
- Why it matters: operational fragility, race windows, and no incremental audit trail by run.
- Evidence: sequential global deletes then full inserts.

4. Risk: Forecast probabilities are hand-tuned and confidence-damped, not cohort-calibrated
- Severity: medium
- File/function: `forecastProbabilitiesFromState`
- Why it matters: smooth outputs may underreact/overreact depending on regime and source specifics.
- Evidence: fixed coefficients and floors (`+0.08`, `+0.10`, `+0.12`).

5. Risk: Utilization thresholds derived from linear ramps over mean utilization
- Severity: medium
- File/function: `estimateExpectedUtilizationDistribution`
- Why it matters: may produce overconfident threshold probabilities from weakly informed mean.
- Evidence: `pAboveXX` computed from affine mappings, not empirical calibration.

6. Risk: Recommendation policy can be decisive even in structurally noisy markets
- Severity: medium
- File/function: `buildRecommendationDistribution`
- Why it matters: no explicit “non-inferable” override; volatile/thin states can still produce actionable labels via thresholds.
- Evidence: only thin-data has explicit branch; volatile handled indirectly through economics/confidence.

7. Risk: Backtest “consumption” label is simplistic
- Severity: medium
- File/function: `scripts/recomputeMetrics.ts` backtest generation
- Why it matters: realized target for consumption may not match modeled event definition.
- Evidence: `realizedConsumption = future.totalOffers < row.totalOffers || future.persistentDisappearanceRate > row.persistentDisappearanceRate`.

8. Risk: Legacy metrics API still uses legacy regime/recommendation pipeline
- Severity: medium
- File/function: `src/app/api/metrics/route.ts`
- Why it matters: parallel policy surfaces can disagree with predictive engine and confuse reviewers/users.
- Evidence: uses `classifyMarketRegime` + legacy `buildRecommendation` overload + legacy utilization/ROI path.

9. Risk: Legacy score remains persisted and returned
- Severity: low-medium
- File/function: `src/lib/scoring/marketScore.ts`, `src/lib/scoring/score.ts`, `src/app/api/scoring/route.ts`
- Why it matters: mixed signal between probability recommendation and weighted heuristic score.
- Evidence: `scenarioScore.create` still executed and compatibility fields returned.

10. Risk: “Structurally non-inferable market” state missing
- Severity: high
- File/function: across classifier/recommendation design
- Why it matters: model may issue probabilities where observability assumptions fail.
- Evidence: state enum has `thin-data` and `volatile`, but no explicit non-inferable regime tied to hard output suppression.

---

# 6. Snapshot-to-forecast trace

Representative dataflow path:

1. Raw snapshot ingestion
- `scripts/collectSnapshots.ts::main`
- Collects offers (`resolveOffers`), builds identity with `buildOfferIdentity`, logs collisions, writes `MarketSnapshot` + `Offer`.

2. Identity/lifecycle stitching
- `scripts/recomputeMetrics.ts::main`
- Loads snapshots ordered by time/source.
- Uses `inferOfferFingerprint` to map each offer to logical identity.
- Updates `lifecycleMap` and segments, counts disappearances/reappearances.

3. Cohort metrics
- `scripts/recomputeMetrics.ts` cohort loops:
  - build exact + family cohort sets
  - compute continuing/new/disappeared/reappeared/persistent disappeared
  - compute machine/host depth + concentration + entry/exit
  - compute price stats and band disappearances

4. Pressure/state/confidence
- `computeCohortPressureScore` (from `src/lib/metrics/intelligence.ts`)
- `computeNoiseScore`
- `classifyCohortState`
- `computeStateConfidence`

5. Config-family shrinkage + forecast rows
- `scripts/recomputeMetrics.ts` applies `shrinkTowardsFamily` for pressure.
- Generates `CohortForecast` rows with `forecastProbabilitiesFromState` (24/72/168h).
- Generates `ForecastBacktest` realized labels.

6. Scenario recommendation
- `src/lib/scoring/marketScore.ts::scoreScenarioWithMarket`
- Reads exact/family trend windows.
- infers/falls back for state and confidence if null.
- infers listing price with cascade.
- computes shrunk pressure/hazard, consumption probs, utilization distribution, economics distribution.
- applies `buildRecommendation`.
- persists `HardwareScenario`, `ScenarioForecast`, `ScenarioScore`.

7. API response
- `src/app/api/scoring/route.ts::POST`
- Validates payload and returns full predictive payload + legacy compatibility fields.

8. Chart data/UI
- `src/lib/scoring/marketScore.ts` builds `visuals.*` arrays.
- `src/app/scoring/page.tsx` renders charts and tables; separates `observed` / `inferred` / `forecasted` from payload.
- Range filtering applied by `src/lib/scoring/chartTransforms.ts::filterSeriesByRangeHours`.

---

# 7. Review-ready file bundle

## send first
1. `src/lib/metrics/intelligence.ts`
2. `scripts/recomputeMetrics.ts`
3. `src/lib/scoring/marketScore.ts`
4. `src/lib/metrics/offerIdentity.ts`
5. `prisma/schema.prisma`

## send second
1. `scripts/collectSnapshots.ts`
2. `src/app/api/scoring/route.ts`
3. `scripts/backtestForecasts.ts`
4. `scripts/debugOfferIdentity.ts`
5. `tests/predictiveEngine.test.ts`

## optional if deeper UI review is needed
1. `src/app/scoring/page.tsx`
2. `src/lib/scoring/chartTransforms.ts`
3. `src/app/api/metrics/route.ts` (legacy endpoint divergence review)

---

# 8. Exact commands to reproduce

From repo root:

```bash
# 0) Install
npm install

# 1) Generate Prisma client
npm run db:generate

# 2) Run DB migrations
npm run db:migrate

# 3) Ingest data (mock)
npm run collect

# 4) Ingest live source (if env configured)
INGEST_MODE=vast npm run collect

# 5) Recompute lifecycle metrics + forecasts + backtests
npm run recompute

# 6) Pipeline status
npm run status

# 7) Identity debug diagnostics
npm run debug:identity
# Optional: source and sample size
tsx scripts/debugOfferIdentity.ts vast-live 5000

# 8) Backtest summary / calibration diagnostics
npm run backtest

# 9) Run tests
npm test

# 10) Build
npm run build

# 11) Run dev server
npm run dev
```

If environment variables are in `env.local`:

```bash
set -a; source env.local; set +a
```

---

# 9. Open questions the code cannot answer yet

1. Structural non-inferability
- Missing explicit regime for “cannot infer demand from visibility behavior” (e.g., unstable listing identity ecosystem, sparse/irregular snapshots, high relist latency).

2. Churn-dominated cohorts with flat net supply
- Current noise/volatile handling exists, but no dedicated policy that suppresses tightness/consumption probabilities when churn appears mostly mechanical.

3. Relist delay modeling
- Persistent disappearance uses one-snapshot lookahead; code does not estimate source-specific relist delay distribution.

4. Identity quality quantification
- Collision diagnostics exist, but there is no persisted identity quality score per source/cohort used to downweight forecasts.

5. Weak-signal price-position curve flattening
- Consumption curve always produced via logistic function; no explicit flattening under low confidence/high volatility.

6. Hazard censoring
- Offer lifecycles tracked, but no explicit right-censor handling in hazard estimation pipeline beyond simple duration accounting.

7. Recommendation calibration
- Backtests target tight/price/consumption proxies; recommendation outcome calibration (e.g., realized ROI hit-rate by recommendation bucket) is missing.

8. Cross-source heterogeneity
- Coefficients are global and hand-tuned; no source-level calibration parameters even though source behavior can differ.

9. Incremental recompute and run provenance
- Full delete/rebuild process lacks run IDs, versioned snapshots of model outputs, and incremental auditability.

10. Legacy/predictive surface consistency
- Legacy score/regime endpoints remain active, so users/reviewers can receive inconsistent policy outputs across routes.
