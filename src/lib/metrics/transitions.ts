export type TransitionInput = {
  previousIds: Set<string>;
  currentIds: Set<string>;
  seenBefore: Set<string>;
};

export type TransitionOutput = {
  continuing: Set<string>;
  added: Set<string>;
  disappeared: Set<string>;
  reappeared: Set<string>;
};

export function computeSnapshotTransition(input: TransitionInput): TransitionOutput {
  const continuing = new Set<string>();
  const added = new Set<string>();
  const disappeared = new Set<string>();
  const reappeared = new Set<string>();

  for (const id of input.currentIds) {
    if (input.previousIds.has(id)) {
      continuing.add(id);
    } else {
      added.add(id);
      if (input.seenBefore.has(id)) {
        reappeared.add(id);
      }
    }
  }

  for (const id of input.previousIds) {
    if (!input.currentIds.has(id)) {
      disappeared.add(id);
    }
  }

  return { continuing, added, disappeared, reappeared };
}

export function segmentLifecyclePresence(
  timeline: Array<{ at: Date; visible: boolean; pricePerHour?: number | null }>,
) {
  const segments: Array<{ start: Date; end: Date; durationHours: number; endedBy: "disappeared" | "still_active" }> = [];
  let currentStart: Date | null = null;

  for (let i = 0; i < timeline.length; i += 1) {
    const point = timeline[i];
    const next = i < timeline.length - 1 ? timeline[i + 1] : null;

    if (point.visible && currentStart == null) {
      currentStart = point.at;
    }

    if (point.visible && (!next || !next.visible) && currentStart != null) {
      const end = point.at;
      segments.push({
        start: currentStart,
        end,
        durationHours: (end.getTime() - currentStart.getTime()) / 1000 / 3600,
        endedBy: next ? "disappeared" : "still_active",
      });
      currentStart = null;
    }
  }

  return segments;
}

export function classifyDisappearanceOutcome(input: {
  id: string;
  futureBuckets: Array<Set<string>>;
  shortGapMaxBuckets?: number;
}): "persistently_disappeared" | "temporarily_missing" | "reappeared_short_gap" | "reappeared_long_gap" {
  const shortGapMax = input.shortGapMaxBuckets ?? 2;
  let reappearDelay: number | null = null;
  for (let i = 0; i < input.futureBuckets.length; i += 1) {
    if (input.futureBuckets[i]?.has(input.id)) {
      reappearDelay = i + 1;
      break;
    }
  }
  if (reappearDelay == null) return "persistently_disappeared";
  if (reappearDelay <= shortGapMax) return "reappeared_short_gap";
  return "reappeared_long_gap";
}

export function summarizeReappearanceGaps(gaps: number[], shortGapMaxBuckets = 2) {
  if (gaps.length === 0) {
    return {
      medianDelayBuckets: null,
      shortGapReappearanceRate: 0,
      longGapReappearanceRate: 0,
    };
  }
  const sorted = [...gaps].sort((a, b) => a - b);
  const medianDelayBuckets = sorted[Math.floor(sorted.length / 2)] ?? null;
  const shortGap = gaps.filter((gap) => gap <= shortGapMaxBuckets).length;
  const longGap = gaps.length - shortGap;
  return {
    medianDelayBuckets,
    shortGapReappearanceRate: shortGap / gaps.length,
    longGapReappearanceRate: longGap / gaps.length,
  };
}
