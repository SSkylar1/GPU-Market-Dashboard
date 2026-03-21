-- CreateSchema
CREATE SCHEMA IF NOT EXISTS "public";

-- CreateTable
CREATE TABLE "MarketSnapshot" (
    "id" TEXT NOT NULL,
    "source" TEXT NOT NULL,
    "capturedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "MarketSnapshot_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "Offer" (
    "id" TEXT NOT NULL,
    "snapshotId" TEXT NOT NULL,
    "offerId" TEXT NOT NULL,
    "gpuName" TEXT NOT NULL,
    "numGpus" INTEGER NOT NULL DEFAULT 1,
    "gpuRamGb" INTEGER,
    "cpuCores" INTEGER,
    "ramGb" INTEGER,
    "diskGb" INTEGER,
    "inetUpMbps" DOUBLE PRECISION,
    "inetDownMbps" DOUBLE PRECISION,
    "rentable" BOOLEAN NOT NULL DEFAULT false,
    "rented" BOOLEAN NOT NULL DEFAULT false,
    "verified" BOOLEAN,
    "reliabilityScore" DOUBLE PRECISION,
    "pricePerHour" DOUBLE PRECISION,
    "rawJson" JSONB,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "Offer_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "GpuRollup" (
    "id" TEXT NOT NULL,
    "snapshotId" TEXT NOT NULL,
    "gpuName" TEXT NOT NULL,
    "totalOffers" INTEGER NOT NULL,
    "rentableOffers" INTEGER NOT NULL,
    "rentedOffers" INTEGER NOT NULL,
    "impliedUtilization" DOUBLE PRECISION NOT NULL,
    "minPrice" DOUBLE PRECISION,
    "medianPrice" DOUBLE PRECISION,
    "p90Price" DOUBLE PRECISION,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "GpuRollup_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "HardwareScenario" (
    "id" TEXT NOT NULL,
    "gpuName" TEXT NOT NULL,
    "gpuCount" INTEGER NOT NULL,
    "assumedPowerWatts" INTEGER NOT NULL,
    "assumedHardwareCost" DOUBLE PRECISION NOT NULL,
    "electricityCostPerKwh" DOUBLE PRECISION NOT NULL,
    "targetPaybackMonths" INTEGER NOT NULL,
    "notes" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "HardwareScenario_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ScenarioScore" (
    "id" TEXT NOT NULL,
    "scenarioId" TEXT NOT NULL,
    "demandScore" DOUBLE PRECISION NOT NULL,
    "competitionScore" DOUBLE PRECISION NOT NULL,
    "priceStrengthScore" DOUBLE PRECISION NOT NULL,
    "efficiencyScore" DOUBLE PRECISION NOT NULL,
    "overallScore" DOUBLE PRECISION NOT NULL,
    "recommendation" TEXT NOT NULL,
    "recommendedPriceLow" DOUBLE PRECISION,
    "recommendedPriceTarget" DOUBLE PRECISION,
    "recommendedPriceHigh" DOUBLE PRECISION,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "ScenarioScore_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "MarketSnapshot_capturedAt_idx" ON "MarketSnapshot"("capturedAt");

-- CreateIndex
CREATE INDEX "Offer_snapshotId_idx" ON "Offer"("snapshotId");

-- CreateIndex
CREATE INDEX "Offer_gpuName_idx" ON "Offer"("gpuName");

-- CreateIndex
CREATE INDEX "Offer_offerId_idx" ON "Offer"("offerId");

-- CreateIndex
CREATE INDEX "Offer_rentable_idx" ON "Offer"("rentable");

-- CreateIndex
CREATE INDEX "Offer_rented_idx" ON "Offer"("rented");

-- CreateIndex
CREATE INDEX "GpuRollup_snapshotId_idx" ON "GpuRollup"("snapshotId");

-- CreateIndex
CREATE INDEX "GpuRollup_gpuName_idx" ON "GpuRollup"("gpuName");

-- CreateIndex
CREATE UNIQUE INDEX "GpuRollup_snapshotId_gpuName_key" ON "GpuRollup"("snapshotId", "gpuName");

-- CreateIndex
CREATE INDEX "HardwareScenario_gpuName_idx" ON "HardwareScenario"("gpuName");

-- CreateIndex
CREATE INDEX "ScenarioScore_scenarioId_idx" ON "ScenarioScore"("scenarioId");

-- AddForeignKey
ALTER TABLE "Offer" ADD CONSTRAINT "Offer_snapshotId_fkey" FOREIGN KEY ("snapshotId") REFERENCES "MarketSnapshot"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "GpuRollup" ADD CONSTRAINT "GpuRollup_snapshotId_fkey" FOREIGN KEY ("snapshotId") REFERENCES "MarketSnapshot"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "ScenarioScore" ADD CONSTRAINT "ScenarioScore_scenarioId_fkey" FOREIGN KEY ("scenarioId") REFERENCES "HardwareScenario"("id") ON DELETE CASCADE ON UPDATE CASCADE;

