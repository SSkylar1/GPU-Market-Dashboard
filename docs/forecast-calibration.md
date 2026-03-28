# Forecast Calibration and Backtesting

## Stored artifacts

`CohortForecast`
- forecast probabilities by horizon and cohort bucket

`ForecastBacktest`
- predicted vs realized outcomes:
  - tightness
  - price direction
  - consumption proxy

## Metrics

- Brier score
- reliability buckets (predicted probability buckets vs realized frequency)

## Commands

- `npm run backtest`
  - computes Brier and calibration outputs over recent backtest rows
- `npm run debug:identity`
  - inspects fingerprint mismatches/collisions and identity quality

## Calibration workflow

1. generate forecasts during recompute
2. write forward labels as outcomes are observed
3. evaluate calibration buckets
4. tune deterministic transforms or add isotonic/logistic calibration layer in v3
