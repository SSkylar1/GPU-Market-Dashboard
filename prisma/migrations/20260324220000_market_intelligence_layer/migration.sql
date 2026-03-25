ALTER TABLE "GpuRollup"
  ADD COLUMN "p10Price" DOUBLE PRECISION;

ALTER TABLE "GpuTrendAggregate"
  ADD COLUMN "p10Price" DOUBLE PRECISION,
  ADD COLUMN "newOfferCount" INTEGER,
  ADD COLUMN "disappearedOfferCount" INTEGER,
  ADD COLUMN "newOfferRate" DOUBLE PRECISION,
  ADD COLUMN "disappearedRate" DOUBLE PRECISION,
  ADD COLUMN "netSupplyChange" INTEGER,
  ADD COLUMN "medianPriceChange" DOUBLE PRECISION,
  ADD COLUMN "rentableShareChange" DOUBLE PRECISION,
  ADD COLUMN "marketPressureScore" DOUBLE PRECISION,
  ADD COLUMN "marketPressurePriceComponent" DOUBLE PRECISION,
  ADD COLUMN "marketPressureChurnComponent" DOUBLE PRECISION,
  ADD COLUMN "marketPressureSupplyComponent" DOUBLE PRECISION,
  ADD COLUMN "marketPressureAvailabilityComponent" DOUBLE PRECISION,
  ADD COLUMN "lowBandDisappearedCount" INTEGER,
  ADD COLUMN "midBandDisappearedCount" INTEGER,
  ADD COLUMN "highBandDisappearedCount" INTEGER,
  ADD COLUMN "lowBandDisappearedRate" DOUBLE PRECISION,
  ADD COLUMN "midBandDisappearedRate" DOUBLE PRECISION,
  ADD COLUMN "highBandDisappearedRate" DOUBLE PRECISION;
