ALTER TABLE "GpuRollup"
  ADD COLUMN "numGpus" INTEGER NOT NULL DEFAULT 1,
  ADD COLUMN "offerType" TEXT NOT NULL DEFAULT 'unknown';

DROP INDEX "GpuRollup_snapshotId_gpuName_key";
CREATE INDEX "GpuRollup_gpuName_numGpus_offerType_idx"
  ON "GpuRollup"("gpuName", "numGpus", "offerType");
CREATE UNIQUE INDEX "GpuRollup_snapshotId_gpuName_numGpus_offerType_key"
  ON "GpuRollup"("snapshotId", "gpuName", "numGpus", "offerType");

ALTER TABLE "GpuTrendAggregate"
  ADD COLUMN "numGpus" INTEGER NOT NULL DEFAULT 1,
  ADD COLUMN "offerType" TEXT NOT NULL DEFAULT 'unknown';

DROP INDEX "GpuTrendAggregate_gpuName_bucketStartUtc_source_key";
DROP INDEX "GpuTrendAggregate_gpuName_bucketStartUtc_idx";

CREATE INDEX "GpuTrendAggregate_gpuName_numGpus_offerType_bucketStartUtc_idx"
  ON "GpuTrendAggregate"("gpuName", "numGpus", "offerType", "bucketStartUtc");

CREATE UNIQUE INDEX "GpuTrendAggregate_gpuName_numGpus_offerType_bucketStartUtc_source_key"
  ON "GpuTrendAggregate"("gpuName", "numGpus", "offerType", "bucketStartUtc", "source");
