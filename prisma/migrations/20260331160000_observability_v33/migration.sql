ALTER TABLE "GpuTrendAggregate"
  ADD COLUMN "observationCount" INTEGER,
  ADD COLUMN "observationsPerOffer" DOUBLE PRECISION,
  ADD COLUMN "medianPollGapMinutes" DOUBLE PRECISION,
  ADD COLUMN "maxPollGapMinutes" DOUBLE PRECISION,
  ADD COLUMN "coverageRatio" DOUBLE PRECISION,
  ADD COLUMN "offerSeenSpanMinutes" DOUBLE PRECISION,
  ADD COLUMN "cohortObservationDensityScore" DOUBLE PRECISION,
  ADD COLUMN "labelabilityScore" DOUBLE PRECISION,
  ADD COLUMN "futureWindowCoverage12h" DOUBLE PRECISION,
  ADD COLUMN "futureWindowCoverage24h" DOUBLE PRECISION,
  ADD COLUMN "futureWindowCoverage72h" DOUBLE PRECISION,
  ADD COLUMN "samplingQualityScore" DOUBLE PRECISION,
  ADD COLUMN "lifecycleObservabilityScore" DOUBLE PRECISION,
  ADD COLUMN "insufficientSampling" BOOLEAN;

ALTER TABLE "OfferLifecycle"
  ADD COLUMN "seenCount" INTEGER,
  ADD COLUMN "cumulativeVisibleMinutes" DOUBLE PRECISION,
  ADD COLUMN "offerSeenSpanMinutes" DOUBLE PRECISION,
  ADD COLUMN "firstMissingAt" TIMESTAMP(3),
  ADD COLUMN "reappearedAt" TIMESTAMP(3),
  ADD COLUMN "gapDurationMinutes" DOUBLE PRECISION,
  ADD COLUMN "visibilitySegmentCount" INTEGER,
  ADD COLUMN "estimatedConsumedAt" TIMESTAMP(3),
  ADD COLUMN "insufficientObservation" BOOLEAN;

ALTER TABLE "ForecastBacktest"
  ADD COLUMN "consumptionLabelQuality" TEXT;
