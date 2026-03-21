-- AlterTable
ALTER TABLE "MarketSnapshot"
ADD COLUMN "ingestMode" TEXT,
ADD COLUMN "sourceQueryHash" TEXT,
ADD COLUMN "sourceQuery" JSONB;

-- CreateTable
CREATE TABLE "GpuTrendAggregate" (
    "id" TEXT NOT NULL,
    "snapshotId" TEXT NOT NULL,
    "source" TEXT NOT NULL,
    "gpuName" TEXT NOT NULL,
    "bucketStartUtc" TIMESTAMP(3) NOT NULL,
    "snapshotCount" INTEGER NOT NULL,
    "totalOffers" INTEGER NOT NULL,
    "rentableOffers" INTEGER NOT NULL,
    "rentedOffers" INTEGER NOT NULL,
    "impliedUtilization" DOUBLE PRECISION NOT NULL,
    "availabilityRatio" DOUBLE PRECISION NOT NULL,
    "minPrice" DOUBLE PRECISION,
    "medianPrice" DOUBLE PRECISION,
    "p90Price" DOUBLE PRECISION,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "GpuTrendAggregate_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "MarketSnapshot_sourceQueryHash_idx" ON "MarketSnapshot"("sourceQueryHash");

-- CreateIndex
CREATE INDEX "GpuTrendAggregate_bucketStartUtc_idx" ON "GpuTrendAggregate"("bucketStartUtc");

-- CreateIndex
CREATE INDEX "GpuTrendAggregate_gpuName_bucketStartUtc_idx" ON "GpuTrendAggregate"("gpuName", "bucketStartUtc");

-- CreateIndex
CREATE UNIQUE INDEX "GpuTrendAggregate_gpuName_bucketStartUtc_source_key" ON "GpuTrendAggregate"("gpuName", "bucketStartUtc", "source");

-- AddForeignKey
ALTER TABLE "GpuTrendAggregate"
ADD CONSTRAINT "GpuTrendAggregate_snapshotId_fkey"
FOREIGN KEY ("snapshotId") REFERENCES "MarketSnapshot"("id") ON DELETE CASCADE ON UPDATE CASCADE;
