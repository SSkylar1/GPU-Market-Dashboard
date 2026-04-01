export type PollingTier = "high-priority" | "general" | "long-tail";

export type PollingTierWindow = {
  minMinutes: number;
  maxMinutes: number;
  targetMinutes: number;
};

export type PollingConfig = {
  sourceDefaults: Record<string, PollingTier>;
  sourceOverrides: Record<string, PollingTierWindow>;
  tierWindows: Record<PollingTier, PollingTierWindow>;
  gpuFamilyTierOverrides: Record<string, PollingTier>;
};

const DEFAULT_TIER_WINDOWS: Record<PollingTier, PollingTierWindow> = {
  "high-priority": { minMinutes: 2, maxMinutes: 5, targetMinutes: 3 },
  general: { minMinutes: 5, maxMinutes: 10, targetMinutes: 7 },
  "long-tail": { minMinutes: 10, maxMinutes: 20, targetMinutes: 15 },
};

function normalizeGpuFamily(name: string): string {
  return name.trim().toLowerCase();
}

function parseMap(input: string | undefined): Record<string, string> {
  if (!input || input.trim().length === 0) return {};
  return input
    .split(",")
    .map((chunk) => chunk.trim())
    .filter(Boolean)
    .reduce<Record<string, string>>((acc, chunk) => {
      const [key, value] = chunk.split(":").map((part) => part.trim());
      if (!key || !value) return acc;
      acc[key] = value;
      return acc;
    }, {});
}

export function loadPollingConfig(): PollingConfig {
  const sourceDefaultMap = parseMap(process.env.POLLING_SOURCE_DEFAULTS);
  const gpuTierMap = parseMap(process.env.POLLING_GPU_FAMILY_TIERS);
  const sourceOverrides = parseMap(process.env.POLLING_SOURCE_OVERRIDES);

  const sourceDefaults: Record<string, PollingTier> = {};
  for (const [source, tier] of Object.entries(sourceDefaultMap)) {
    if (tier === "high-priority" || tier === "general" || tier === "long-tail") {
      sourceDefaults[source] = tier;
    }
  }

  const gpuFamilyTierOverrides: Record<string, PollingTier> = {};
  for (const [family, tier] of Object.entries(gpuTierMap)) {
    if (tier === "high-priority" || tier === "general" || tier === "long-tail") {
      gpuFamilyTierOverrides[normalizeGpuFamily(family)] = tier;
    }
  }

  const sourceOverrideWindows: Record<string, PollingTierWindow> = {};
  for (const [source, encoded] of Object.entries(sourceOverrides)) {
    const [minRaw, maxRaw, targetRaw] = encoded.split("/").map((v) => Number(v));
    if (!Number.isFinite(minRaw) || !Number.isFinite(maxRaw)) continue;
    sourceOverrideWindows[source] = {
      minMinutes: Math.max(1, minRaw),
      maxMinutes: Math.max(minRaw, maxRaw),
      targetMinutes: Number.isFinite(targetRaw) ? Math.max(minRaw, Math.min(maxRaw, targetRaw)) : Math.round((minRaw + maxRaw) / 2),
    };
  }

  return {
    sourceDefaults,
    sourceOverrides: sourceOverrideWindows,
    tierWindows: DEFAULT_TIER_WINDOWS,
    gpuFamilyTierOverrides,
  };
}

export function resolvePollingTier(
  source: string,
  gpuName: string | null,
  config: PollingConfig,
): PollingTier {
  if (gpuName) {
    const maybeTier = config.gpuFamilyTierOverrides[normalizeGpuFamily(gpuName)];
    if (maybeTier) return maybeTier;
  }
  return config.sourceDefaults[source] ?? "general";
}

export function getPollingWindowForTier(
  source: string,
  tier: PollingTier,
  config: PollingConfig,
): PollingTierWindow {
  return config.sourceOverrides[source] ?? config.tierWindows[tier];
}
