ALTER TABLE "GpuTrendAggregate"
  ADD COLUMN "timeDepthScore" DOUBLE PRECISION,
  ADD COLUMN "crossSectionDepthScore" DOUBLE PRECISION;

ALTER TABLE "ForecastBacktest"
  ADD COLUMN "predictedConsumptionProbRaw" DOUBLE PRECISION,
  ADD COLUMN "predictedConsumptionProbCalibrated" DOUBLE PRECISION,
  ADD COLUMN "realizedConsumptionLegacy" BOOLEAN,
  ADD COLUMN "consumptionLabelCensored" BOOLEAN NOT NULL DEFAULT false,
  ADD COLUMN "timeToReappearanceBuckets" DOUBLE PRECISION,
  ADD COLUMN "timeToReappearanceHours" DOUBLE PRECISION,
  ADD COLUMN "realizedConsumedWithin12h" BOOLEAN,
  ADD COLUMN "realizedConsumedWithin24h" BOOLEAN,
  ADD COLUMN "realizedConsumedWithin72h" BOOLEAN;
