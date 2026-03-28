ALTER TABLE "Offer"
  ADD COLUMN IF NOT EXISTS "source" TEXT,
  ADD COLUMN IF NOT EXISTS "capturedAt" TIMESTAMP(3),
  ADD COLUMN IF NOT EXISTS "offerExternalId" TEXT,
  ADD COLUMN IF NOT EXISTS "offerFingerprint" TEXT,
  ADD COLUMN IF NOT EXISTS "dlperf" DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS "timeRemaining" DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS "geolocation" JSONB,
  ADD COLUMN IF NOT EXISTS "relativePriceToCohort" DOUBLE PRECISION;

UPDATE "Offer" o
SET
  "source" = s."source",
  "capturedAt" = s."capturedAt"
FROM "MarketSnapshot" s
WHERE o."snapshotId" = s."id"
  AND (o."source" IS NULL OR o."capturedAt" IS NULL);

ALTER TABLE "Offer"
  ALTER COLUMN "source" SET NOT NULL,
  ALTER COLUMN "capturedAt" SET NOT NULL;

CREATE INDEX IF NOT EXISTS "Offer_source_capturedAt_idx" ON "Offer"("source", "capturedAt");
CREATE INDEX IF NOT EXISTS "Offer_offerExternalId_idx" ON "Offer"("offerExternalId");
CREATE INDEX IF NOT EXISTS "Offer_offerFingerprint_idx" ON "Offer"("offerFingerprint");

CREATE TABLE IF NOT EXISTS "OfferLifecycle" (
  "id" TEXT NOT NULL,
  "source" TEXT NOT NULL,
  "offerFingerprint" TEXT NOT NULL,
  "offerExternalId" TEXT,
  "gpuName" TEXT NOT NULL,
  "numGpus" INTEGER NOT NULL,
  "offerType" TEXT NOT NULL,
  "machineId" INTEGER,
  "hostId" INTEGER,
  "firstSeenAt" TIMESTAMP(3) NOT NULL,
  "lastSeenAt" TIMESTAMP(3) NOT NULL,
  "totalVisibleSnapshots" INTEGER NOT NULL,
  "totalVisibleHours" DOUBLE PRECISION NOT NULL,
  "disappearanceCount" INTEGER NOT NULL,
  "reappearanceCount" INTEGER NOT NULL,
  "longestContinuousVisibleHours" DOUBLE PRECISION NOT NULL,
  "latestKnownPricePerHour" DOUBLE PRECISION,
  "latestKnownReliabilityScore" DOUBLE PRECISION,
  "firstKnownPricePerHour" DOUBLE PRECISION,
  "minObservedPricePerHour" DOUBLE PRECISION,
  "maxObservedPricePerHour" DOUBLE PRECISION,
  "priceEditCount" INTEGER NOT NULL,
  "lastStatus" TEXT NOT NULL,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "OfferLifecycle_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX IF NOT EXISTS "OfferLifecycle_source_offerFingerprint_key" ON "OfferLifecycle"("source", "offerFingerprint");
CREATE INDEX IF NOT EXISTS "OfferLifecycle_source_gpuName_numGpus_offerType_firstSeenAt_idx"
  ON "OfferLifecycle"("source", "gpuName", "numGpus", "offerType", "firstSeenAt");

CREATE TABLE IF NOT EXISTS "OfferLifecycleSegment" (
  "id" TEXT NOT NULL,
  "lifecycleId" TEXT NOT NULL,
  "segmentStartAt" TIMESTAMP(3) NOT NULL,
  "segmentEndAt" TIMESTAMP(3),
  "durationHours" DOUBLE PRECISION,
  "endedBy" TEXT NOT NULL,
  "startPricePerHour" DOUBLE PRECISION,
  "endPricePerHour" DOUBLE PRECISION,
  "medianPricePerHour" DOUBLE PRECISION,
  "startRentable" BOOLEAN,
  "endRentable" BOOLEAN,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "OfferLifecycleSegment_pkey" PRIMARY KEY ("id")
);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'OfferLifecycleSegment_lifecycleId_fkey'
  ) THEN
    ALTER TABLE "OfferLifecycleSegment"
      ADD CONSTRAINT "OfferLifecycleSegment_lifecycleId_fkey"
      FOREIGN KEY ("lifecycleId") REFERENCES "OfferLifecycle"("id") ON DELETE CASCADE ON UPDATE CASCADE;
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS "OfferLifecycleSegment_lifecycleId_segmentStartAt_idx"
  ON "OfferLifecycleSegment"("lifecycleId", "segmentStartAt");

ALTER TABLE "GpuTrendAggregate"
  ADD COLUMN IF NOT EXISTS "cohortKey" TEXT,
  ADD COLUMN IF NOT EXISTS "uniqueMachines" INTEGER,
  ADD COLUMN IF NOT EXISTS "uniqueHosts" INTEGER,
  ADD COLUMN IF NOT EXISTS "continuingOffers" INTEGER,
  ADD COLUMN IF NOT EXISTS "newOffers" INTEGER,
  ADD COLUMN IF NOT EXISTS "disappearedOffers" INTEGER,
  ADD COLUMN IF NOT EXISTS "reappearedOffers" INTEGER,
  ADD COLUMN IF NOT EXISTS "persistentDisappearedOffers" INTEGER,
  ADD COLUMN IF NOT EXISTS "reappearedRate" DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS "persistentDisappearanceRate" DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS "machineEntryRate" DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS "machineExitRate" DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS "hostEntryRate" DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS "hostExitRate" DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS "maxPrice" DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS "priceCv" DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS "lowBandPersistentDisappearedRate" DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS "midBandPersistentDisappearedRate" DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS "highBandPersistentDisappearedRate" DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS "supplyTightnessScore" DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS "machineDepthScore" DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS "concentrationScore" DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS "cohortPressureScore" DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS "pressureAcceleration" DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS "pressurePersistence" DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS "state" TEXT,
  ADD COLUMN IF NOT EXISTS "stateConfidence" DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS "dataDepthScore" DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS "noiseScore" DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS "configVsFamilyPressureDelta" DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS "configVsFamilyPriceDelta" DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS "configVsFamilyHazardDelta" DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS "machineConcentrationShareTop1" DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS "machineConcentrationShareTop3" DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS "hostConcentrationShareTop1" DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS "hostConcentrationShareTop3" DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS "machinePersistenceRate" DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS "hostPersistenceRate" DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS "newMachineEntryRate" DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS "disappearingMachineRate" DOUBLE PRECISION;

ALTER TABLE "GpuTrendAggregate"
  ALTER COLUMN "numGpus" DROP NOT NULL,
  ALTER COLUMN "offerType" DROP NOT NULL;

UPDATE "GpuTrendAggregate"
SET "cohortKey" = concat_ws('::', "gpuName", COALESCE("numGpus"::text, 'combined'), COALESCE("offerType", 'combined'))
WHERE "cohortKey" IS NULL;

ALTER TABLE "GpuTrendAggregate"
  ALTER COLUMN "cohortKey" SET NOT NULL;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'GpuTrendAggregate_gpuName_numGpus_offerType_bucketStartUtc_source_key'
  ) THEN
    ALTER TABLE "GpuTrendAggregate"
      DROP CONSTRAINT "GpuTrendAggregate_gpuName_numGpus_offerType_bucketStartUtc_source_key";
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'GpuTrendAggregate_source_cohortKey_bucketStartUtc_key'
  ) THEN
    ALTER TABLE "GpuTrendAggregate"
      ADD CONSTRAINT "GpuTrendAggregate_source_cohortKey_bucketStartUtc_key"
      UNIQUE ("source", "cohortKey", "bucketStartUtc");
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS "GpuTrendAggregate_source_gpuName_numGpus_offerType_bucketStartUtc_idx"
  ON "GpuTrendAggregate"("source", "gpuName", "numGpus", "offerType", "bucketStartUtc");

CREATE TABLE IF NOT EXISTS "CohortForecast" (
  "id" TEXT NOT NULL,
  "source" TEXT NOT NULL,
  "gpuName" TEXT NOT NULL,
  "numGpus" INTEGER,
  "offerType" TEXT,
  "bucketStartUtc" TIMESTAMP(3) NOT NULL,
  "forecastHorizonHours" INTEGER NOT NULL,
  "pTight" DOUBLE PRECISION NOT NULL,
  "pBalanced" DOUBLE PRECISION NOT NULL,
  "pOversupplied" DOUBLE PRECISION NOT NULL,
  "pPriceUp" DOUBLE PRECISION NOT NULL,
  "pPriceFlat" DOUBLE PRECISION NOT NULL,
  "pPriceDown" DOUBLE PRECISION NOT NULL,
  "expectedPressure" DOUBLE PRECISION NOT NULL,
  "expectedPressureLow" DOUBLE PRECISION NOT NULL,
  "expectedPressureHigh" DOUBLE PRECISION NOT NULL,
  "expectedVisibleSupply" DOUBLE PRECISION NOT NULL,
  "expectedVisibleSupplyLow" DOUBLE PRECISION NOT NULL,
  "expectedVisibleSupplyHigh" DOUBLE PRECISION NOT NULL,
  "expectedMedianPrice" DOUBLE PRECISION,
  "expectedMedianPriceLow" DOUBLE PRECISION,
  "expectedMedianPriceHigh" DOUBLE PRECISION,
  "confidenceScore" DOUBLE PRECISION NOT NULL,
  "calibrationVersion" TEXT NOT NULL,
  "modelVersion" TEXT NOT NULL,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "CohortForecast_pkey" PRIMARY KEY ("id")
);

CREATE INDEX IF NOT EXISTS "CohortForecast_source_gpuName_bucketStartUtc_idx"
  ON "CohortForecast"("source", "gpuName", "bucketStartUtc");
CREATE INDEX IF NOT EXISTS "CohortForecast_source_gpuName_numGpus_offerType_bucketStartUtc_idx"
  ON "CohortForecast"("source", "gpuName", "numGpus", "offerType", "bucketStartUtc");
CREATE UNIQUE INDEX IF NOT EXISTS "CohortForecast_key_unique"
  ON "CohortForecast"("source", "gpuName", "numGpus", "offerType", "bucketStartUtc", "forecastHorizonHours", "modelVersion");

CREATE TABLE IF NOT EXISTS "ScenarioForecast" (
  "id" TEXT NOT NULL,
  "hardwareScenarioId" TEXT NOT NULL,
  "modelVersion" TEXT NOT NULL,
  "source" TEXT NOT NULL,
  "gpuName" TEXT NOT NULL,
  "numGpus" INTEGER,
  "offerType" TEXT,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "pUtilizationAbove25" DOUBLE PRECISION NOT NULL,
  "pUtilizationAbove50" DOUBLE PRECISION NOT NULL,
  "pUtilizationAbove75" DOUBLE PRECISION NOT NULL,
  "expectedUtilization" DOUBLE PRECISION NOT NULL,
  "expectedUtilizationLow" DOUBLE PRECISION NOT NULL,
  "expectedUtilizationHigh" DOUBLE PRECISION NOT NULL,
  "expectedDailyRevenue" DOUBLE PRECISION NOT NULL,
  "expectedDailyRevenueLow" DOUBLE PRECISION NOT NULL,
  "expectedDailyRevenueHigh" DOUBLE PRECISION NOT NULL,
  "expectedDailyMargin" DOUBLE PRECISION NOT NULL,
  "expectedDailyMarginLow" DOUBLE PRECISION NOT NULL,
  "expectedDailyMarginHigh" DOUBLE PRECISION NOT NULL,
  "expectedPaybackMonths" DOUBLE PRECISION,
  "expectedPaybackMonthsLow" DOUBLE PRECISION,
  "expectedPaybackMonthsHigh" DOUBLE PRECISION,
  "pPaybackWithinTarget" DOUBLE PRECISION NOT NULL,
  "pScenarioOutperformingGpuFamilyMedian" DOUBLE PRECISION NOT NULL,
  "confidenceScore" DOUBLE PRECISION NOT NULL,
  "recommendation" TEXT NOT NULL,
  "recommendationReasonPrimary" TEXT NOT NULL,
  "recommendationReasonSecondary" TEXT NOT NULL,
  "riskFlags" JSONB NOT NULL,
  "explanation" JSONB NOT NULL,
  CONSTRAINT "ScenarioForecast_pkey" PRIMARY KEY ("id")
);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'ScenarioForecast_hardwareScenarioId_fkey'
  ) THEN
    ALTER TABLE "ScenarioForecast"
      ADD CONSTRAINT "ScenarioForecast_hardwareScenarioId_fkey"
      FOREIGN KEY ("hardwareScenarioId") REFERENCES "HardwareScenario"("id") ON DELETE CASCADE ON UPDATE CASCADE;
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS "ScenarioForecast_hardwareScenarioId_idx" ON "ScenarioForecast"("hardwareScenarioId");
CREATE INDEX IF NOT EXISTS "ScenarioForecast_source_gpuName_createdAt_idx" ON "ScenarioForecast"("source", "gpuName", "createdAt");

CREATE TABLE IF NOT EXISTS "ForecastBacktest" (
  "id" TEXT NOT NULL,
  "source" TEXT NOT NULL,
  "gpuName" TEXT NOT NULL,
  "numGpus" INTEGER,
  "offerType" TEXT,
  "predictionBucketStartUtc" TIMESTAMP(3) NOT NULL,
  "horizonHours" INTEGER NOT NULL,
  "predictedPTight" DOUBLE PRECISION NOT NULL,
  "realizedTight" BOOLEAN NOT NULL,
  "predictedPPriceUp" DOUBLE PRECISION NOT NULL,
  "realizedPriceUp" BOOLEAN NOT NULL,
  "predictedConsumptionProb" DOUBLE PRECISION NOT NULL,
  "realizedConsumption" BOOLEAN NOT NULL,
  "calibrationBucket" TEXT NOT NULL,
  "modelVersion" TEXT NOT NULL,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "ForecastBacktest_pkey" PRIMARY KEY ("id")
);

CREATE INDEX IF NOT EXISTS "ForecastBacktest_source_gpuName_predictionBucketStartUtc_idx"
  ON "ForecastBacktest"("source", "gpuName", "predictionBucketStartUtc");
CREATE INDEX IF NOT EXISTS "ForecastBacktest_source_gpuName_numGpus_offerType_horizonHours_idx"
  ON "ForecastBacktest"("source", "gpuName", "numGpus", "offerType", "horizonHours");
