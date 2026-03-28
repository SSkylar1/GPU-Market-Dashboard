-- Predictive engine v3: identity robustness, inferability metrics, and backtest segmentation
ALTER TABLE "Offer"
  ADD COLUMN "stableOfferFingerprint" TEXT,
  ADD COLUMN "versionFingerprint" TEXT,
  ADD COLUMN "identityStrategy" TEXT,
  ADD COLUMN "identityQualityScore" DOUBLE PRECISION;

CREATE INDEX "Offer_stableOfferFingerprint_idx" ON "Offer"("stableOfferFingerprint");
CREATE INDEX "Offer_versionFingerprint_idx" ON "Offer"("versionFingerprint");

ALTER TABLE "OfferLifecycle"
  ADD COLUMN "stableOfferFingerprint" TEXT,
  ADD COLUMN "latestVersionFingerprint" TEXT,
  ADD COLUMN "identityStrategy" TEXT,
  ADD COLUMN "identityQualityScore" DOUBLE PRECISION,
  ADD COLUMN "mutationCount" INTEGER NOT NULL DEFAULT 0;

ALTER TABLE "GpuTrendAggregate"
  ADD COLUMN "temporaryMissingRate" DOUBLE PRECISION,
  ADD COLUMN "reappearedShortGapRate" DOUBLE PRECISION,
  ADD COLUMN "reappearedLongGapRate" DOUBLE PRECISION,
  ADD COLUMN "medianReappearanceDelayBuckets" DOUBLE PRECISION,
  ADD COLUMN "persistentDisappearanceRateN" DOUBLE PRECISION,
  ADD COLUMN "churnAdjustedDisappearanceRate" DOUBLE PRECISION,
  ADD COLUMN "churnScore" DOUBLE PRECISION,
  ADD COLUMN "signalStrengthScore" DOUBLE PRECISION,
  ADD COLUMN "inferabilityScore" DOUBLE PRECISION,
  ADD COLUMN "identityQualityScore" DOUBLE PRECISION;

ALTER TABLE "ForecastBacktest"
  ADD COLUMN "realizedTightSustained" BOOLEAN,
  ADD COLUMN "confidenceBucket" TEXT,
  ADD COLUMN "inferabilityBucket" TEXT,
  ADD COLUMN "stateAtPrediction" TEXT;
