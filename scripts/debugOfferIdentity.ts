import "dotenv/config";
import { PrismaClient } from "../src/generated/prisma/client";
import { PrismaPg } from "@prisma/adapter-pg";
import { Pool } from "pg";
import { buildOfferIdentity, detectFingerprintCollisions } from "../src/lib/metrics/offerIdentity";

const connectionString = process.env.DATABASE_URL;
if (!connectionString) {
  throw new Error("DATABASE_URL is not set");
}

const prisma = new PrismaClient({
  adapter: new PrismaPg(new Pool({ connectionString })),
});

async function main() {
  const source = process.argv[2] ?? "vast-live";
  const take = Number(process.argv[3] ?? 5000);

  const offers = await prisma.offer.findMany({
    where: { source },
    orderBy: { capturedAt: "desc" },
    take,
    select: {
      id: true,
      source: true,
      capturedAt: true,
      offerId: true,
      offerExternalId: true,
      offerFingerprint: true,
      stableOfferFingerprint: true,
      versionFingerprint: true,
      identityStrategy: true,
      identityQualityScore: true,
      machineId: true,
      hostId: true,
      gpuName: true,
      numGpus: true,
      offerType: true,
      gpuRamGb: true,
      cpuCores: true,
      ramGb: true,
      reliabilityScore: true,
      verified: true,
      pricePerHour: true,
      inetDownMbps: true,
      inetUpMbps: true,
    },
  });

  const generated = offers.map((offer) => {
    const identity = buildOfferIdentity({
      source: offer.source,
      offerExternalId: offer.offerExternalId ?? offer.offerId,
      machineId: offer.machineId,
      hostId: offer.hostId,
      gpuName: offer.gpuName,
      numGpus: offer.numGpus,
      offerType: offer.offerType,
      gpuRamGb: offer.gpuRamGb,
      cpuCores: offer.cpuCores,
      ramGb: offer.ramGb,
      reliabilityScore: offer.reliabilityScore,
      verified: offer.verified,
      pricePerHour: offer.pricePerHour,
      inetDownMbps: offer.inetDownMbps,
      inetUpMbps: offer.inetUpMbps,
    });

    return {
      persistedStable: offer.stableOfferFingerprint ?? offer.offerFingerprint,
      persistedVersion: offer.versionFingerprint,
      generatedStable: identity.stableOfferFingerprint,
      generatedVersion: identity.versionFingerprint,
      strategy: offer.identityStrategy ?? identity.strategy,
      quality: offer.identityQualityScore ?? identity.identityQualityScore,
      signature: `${offer.gpuName}:${offer.numGpus}:${offer.offerType ?? "unknown"}:${offer.machineId ?? "na"}:${offer.hostId ?? "na"}:${offer.gpuRamGb ?? "na"}`,
      externalId: offer.offerExternalId ?? offer.offerId,
      stable: offer.stableOfferFingerprint ?? identity.stableOfferFingerprint,
      version: offer.versionFingerprint ?? identity.versionFingerprint,
      capturedAt: offer.capturedAt,
      id: offer.id,
    };
  });

  const stableMismatches = generated.filter(
    (entry) => entry.persistedStable != null && entry.persistedStable.length > 0 && entry.persistedStable !== entry.generatedStable,
  );
  const versionMismatches = generated.filter(
    (entry) => entry.persistedVersion != null && entry.persistedVersion.length > 0 && entry.persistedVersion !== entry.generatedVersion,
  );

  const collisions = detectFingerprintCollisions(
    generated.map((entry) => ({ fingerprint: entry.stable, signature: entry.signature })),
  );

  const strategyCounts = new Map<string, number>();
  for (const entry of generated) {
    strategyCounts.set(entry.strategy, (strategyCounts.get(entry.strategy) ?? 0) + 1);
  }

  const splitByExternal = new Map<string, Set<string>>();
  for (const entry of generated) {
    const set = splitByExternal.get(entry.externalId) ?? new Set<string>();
    set.add(entry.stable);
    splitByExternal.set(entry.externalId, set);
  }
  const splitExternalIds = [...splitByExternal.values()].filter((set) => set.size > 1).length;

  const groupedByStable = new Map<string, Array<{ version: string; capturedAt: Date }>>();
  for (const entry of generated) {
    const rows = groupedByStable.get(entry.stable) ?? [];
    rows.push({ version: entry.version, capturedAt: entry.capturedAt });
    groupedByStable.set(entry.stable, rows);
  }
  let mutationTransitions = 0;
  let mutationDenominator = 0;
  for (const rows of groupedByStable.values()) {
    rows.sort((a, b) => a.capturedAt.getTime() - b.capturedAt.getTime());
    for (let i = 1; i < rows.length; i += 1) {
      mutationDenominator += 1;
      if (rows[i].version !== rows[i - 1].version) mutationTransitions += 1;
    }
  }
  const mutationRate = mutationDenominator === 0 ? 0 : mutationTransitions / mutationDenominator;

  console.log(`Source=${source} sampled_offers=${offers.length}`);
  console.log(`Stable fingerprint mismatches=${stableMismatches.length}`);
  console.log(`Version fingerprint mismatches=${versionMismatches.length}`);
  console.log(`Generated collisions=${collisions.size} (${((collisions.size / Math.max(generated.length, 1)) * 100).toFixed(2)}%)`);
  console.log(`Suspected split rate=${((splitExternalIds / Math.max(splitByExternal.size, 1)) * 100).toFixed(2)}%`);
  console.log(`Mutation rate=${(mutationRate * 100).toFixed(2)}%`);
  console.log(`Identity quality mean=${(generated.reduce((acc, row) => acc + row.quality, 0) / Math.max(generated.length, 1)).toFixed(3)}`);
  console.log(
    `Strategy usage: external=${((100 * (strategyCounts.get("external_id") ?? 0)) / Math.max(generated.length, 1)).toFixed(1)}% machine=${((100 * (strategyCounts.get("machine_signature") ?? 0)) / Math.max(generated.length, 1)).toFixed(1)}% host=${((100 * (strategyCounts.get("host_signature") ?? 0)) / Math.max(generated.length, 1)).toFixed(1)}% weak=${((100 * (strategyCounts.get("weak_signature") ?? 0)) / Math.max(generated.length, 1)).toFixed(1)}%`,
  );

  if (stableMismatches.length > 0) {
    console.log("Top mismatch examples:");
    for (const row of stableMismatches.slice(0, 10)) {
      console.log(`${row.id} persistedStable=${row.persistedStable} generatedStable=${row.generatedStable}`);
    }
  }

  if (collisions.size > 0) {
    console.log("Top collision examples:");
    let shown = 0;
    for (const [fingerprint, signatures] of collisions.entries()) {
      console.log(`${fingerprint} => ${Array.from(signatures).slice(0, 3).join(" | ")}`);
      shown += 1;
      if (shown >= 10) break;
    }
  }
}

main()
  .catch((error) => {
    console.error(error);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
