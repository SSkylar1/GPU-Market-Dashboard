ALTER TABLE "Offer"
  ADD COLUMN "offerType" TEXT;

CREATE INDEX "Offer_gpuName_numGpus_offerType_idx"
  ON "Offer"("gpuName", "numGpus", "offerType");

CREATE TABLE "GpuChurnAggregate" (
  "id" TEXT NOT NULL,
  "snapshotId" TEXT NOT NULL,
  "previousSnapshotId" TEXT NOT NULL,
  "source" TEXT NOT NULL,
  "sourceQueryHash" TEXT,
  "gpuName" TEXT NOT NULL,
  "numGpus" INTEGER NOT NULL,
  "offerType" TEXT NOT NULL DEFAULT 'unknown',
  "bucketStartUtc" TIMESTAMP(3) NOT NULL,
  "newOfferCount" INTEGER NOT NULL,
  "disappearedOfferCount" INTEGER NOT NULL,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL,

  CONSTRAINT "GpuChurnAggregate_pkey" PRIMARY KEY ("id"),
  CONSTRAINT "GpuChurnAggregate_snapshotId_fkey"
    FOREIGN KEY ("snapshotId") REFERENCES "MarketSnapshot"("id")
    ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE INDEX "GpuChurnAggregate_bucketStartUtc_idx"
  ON "GpuChurnAggregate"("bucketStartUtc");

CREATE INDEX "GpuChurnAggregate_gpuName_bucketStartUtc_idx"
  ON "GpuChurnAggregate"("gpuName", "bucketStartUtc");

CREATE INDEX "GpuChurnAggregate_sourceQueryHash_bucketStartUtc_idx"
  ON "GpuChurnAggregate"("sourceQueryHash", "bucketStartUtc");

CREATE UNIQUE INDEX "GpuChurnAggregate_source_gpuName_numGpus_offerType_bucketStartUtc_key"
  ON "GpuChurnAggregate"("source", "gpuName", "numGpus", "offerType", "bucketStartUtc");
