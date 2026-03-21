-- AlterTable
ALTER TABLE "HardwareScenario" ADD COLUMN "notes" TEXT;

-- AlterTable
ALTER TABLE "MarketSnapshot" ADD COLUMN "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP;

-- AlterTable
ALTER TABLE "Offer"
ALTER COLUMN "numGpus" SET DEFAULT 1,
ALTER COLUMN "rentable" SET NOT NULL,
ALTER COLUMN "rentable" SET DEFAULT false,
ALTER COLUMN "rented" SET NOT NULL,
ALTER COLUMN "rented" SET DEFAULT false;

-- CreateIndex
CREATE UNIQUE INDEX "GpuRollup_snapshotId_gpuName_key" ON "GpuRollup"("snapshotId", "gpuName");

-- CreateIndex
CREATE INDEX "HardwareScenario_gpuName_idx" ON "HardwareScenario"("gpuName");

-- AddForeignKey
ALTER TABLE "GpuRollup"
ADD CONSTRAINT "GpuRollup_snapshotId_fkey"
FOREIGN KEY ("snapshotId") REFERENCES "MarketSnapshot"("id") ON DELETE CASCADE ON UPDATE CASCADE;
