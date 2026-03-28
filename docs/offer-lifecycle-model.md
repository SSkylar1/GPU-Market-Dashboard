# Offer Lifecycle Model

## Identity strategy

Priority order:
1. stable external offer id (`offerExternalId`) when available
2. machine signature hash
3. host signature hash
4. weak signature hash (fallback)

Fingerprinting is intentionally conservative to reduce false merges.
Collision diagnostics are logged during collection.

## Lifecycle entities

- `Offer`: raw per-snapshot offer observations
- `OfferLifecycle`: one row per logical offer identity
- `OfferLifecycleSegment`: one row per continuous visibility segment

## Key inferred lifecycle behaviors

- newly seen
- continuing
- disappeared
- reappeared
- persistent disappeared (not immediately returning)
- price edit frequency
- continuous visibility duration

## Why this works

We avoid direct rented-offer assumptions. Instead, repeated disappearance/reappearance/persistence patterns provide a robust proxy for consumption pressure.
