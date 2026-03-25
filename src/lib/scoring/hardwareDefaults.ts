export type HardwareDefaults = {
  assumedPowerWatts: number;
  assumedHardwareCost: number;
};

export function estimatePowerAndCost(gpuName: string): HardwareDefaults {
  const upper = gpuName.toUpperCase();

  if (upper.includes("H200")) return { assumedPowerWatts: 700, assumedHardwareCost: 30000 };
  if (upper.includes("H100")) return { assumedPowerWatts: 700, assumedHardwareCost: 25000 };
  if (upper.includes("A100")) return { assumedPowerWatts: 400, assumedHardwareCost: 12000 };
  if (upper.includes("A40")) return { assumedPowerWatts: 300, assumedHardwareCost: 4500 };
  if (upper.includes("L4")) return { assumedPowerWatts: 80, assumedHardwareCost: 2200 };
  if (upper.includes("5090")) return { assumedPowerWatts: 575, assumedHardwareCost: 2600 };
  if (upper.includes("5080")) return { assumedPowerWatts: 420, assumedHardwareCost: 1800 };
  if (upper.includes("5070")) return { assumedPowerWatts: 300, assumedHardwareCost: 1200 };
  if (upper.includes("5060")) return { assumedPowerWatts: 220, assumedHardwareCost: 800 };
  if (upper.includes("4090")) return { assumedPowerWatts: 450, assumedHardwareCost: 2500 };
  if (upper.includes("4080")) return { assumedPowerWatts: 320, assumedHardwareCost: 1400 };
  if (upper.includes("4070")) return { assumedPowerWatts: 285, assumedHardwareCost: 900 };
  if (upper.includes("3090")) return { assumedPowerWatts: 350, assumedHardwareCost: 700 };
  return { assumedPowerWatts: 350, assumedHardwareCost: 1500 };
}
