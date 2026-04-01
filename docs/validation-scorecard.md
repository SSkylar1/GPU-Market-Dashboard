# Validation Scorecard v3.2

Generated: 2026-03-29T06:24:40.177Z
Model: predictive-v3.2
Calibration: consumption-cal-v2

## Core Brier
- Tight: 0.0511
- Price-up: 0.2354
- Consumption 12h: raw=0.2394, calibrated=0.1864, legacy-proxy=0.2444, usable=53405, censored=0
- Consumption 24h: raw=0.2359, calibrated=0.1266, legacy-proxy=0.2444, usable=47338, censored=0
- Consumption 72h: raw=0.2337, calibrated=0.0660, legacy-proxy=0.2462, usable=23016, censored=0

## Inferability Distribution
- Total cohorts: 127
- Exactly 0: 127
- 0-10: 0
- 10-25: 0
- 25-50: 0
- 50-75: 0
- 75-100: 0

## Representative Cohorts
- churn-heavy: vast-live::RTX 6000Ada::combined::combined
  state=non-inferable, confidence=7.4, inferability=0.0, timeDepth=100.0, crossSectionDepth=5.2
  usable/censored 12h=467/0, 24h=435/0, 72h=315/0
- thin-cleaner: vast-live::RTX 4070S::1::on-demand
  state=non-inferable, confidence=7.4, inferability=0.0, timeDepth=100.0, crossSectionDepth=4.9
  usable/censored 12h=94/0, 24h=79/0, 72h=29/0
- healthier-deeper: vast-live::RTX 5090::combined::combined
  state=non-inferable, confidence=17.4, inferability=0.0, timeDepth=100.0, crossSectionDepth=29.8
  usable/censored 12h=2203/0, 24h=2042/0, 72h=1403/0

## Recommendation Outcomes (Scenario Forecasts)
- Speculative: n=14, avgUtil=40.8%, avgHit=55.0%, avgConfidence=13.4, avgPaybackMonths=21.18
- Avoid: n=10, avgUtil=18.4%, avgHit=17.7%, avgConfidence=15.9, avgPaybackMonths=98.58
