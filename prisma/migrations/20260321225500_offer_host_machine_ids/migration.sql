ALTER TABLE "Offer"
  ADD COLUMN "hostId" INTEGER,
  ADD COLUMN "machineId" INTEGER;

CREATE INDEX "Offer_hostId_idx" ON "Offer"("hostId");
CREATE INDEX "Offer_machineId_idx" ON "Offer"("machineId");
