# Greyhound Market Research – BAC Quant Engine

## Overview

This project investigates potential inefficiencies in the UK greyhound betting market using Betfair SP (BSP) data.

The objective is not prediction, but **identifying structural edges** and validating them using robust quantitative methods.

---

## Dataset

- ~828,000 runner records
- ~138,000 races
- 18 UK tracks
- Time range: 2018 – 2024

All data was:
- Cleaned
- Standardised
- Filtered to UK-only
- Validated for completeness

---

## Research Process

1. Data ingestion and cleaning  
2. UK-only filtering  
3. Baseline profitability analysis  
4. Stability testing (yearly + monthly)  
5. Candidate system construction  
6. Walk-forward validation (train/test split)  
7. Equity curve generation  
8. Visualisation  

---

## Key Findings

### 1. Market-Level Edge

**BSP 8.00–13.00**

- ROI: ~+1.0%
- Bets: 174,000+
- Positive in most years
- Strong out-of-sample performance

👉 Suggests structural inefficiency in mid-range odds.

---

### 2. Track/Trap Edges

**Romford Trap 3**
- ROI: ~+6.0%
- Bets: 11,000+
- Stable across train/test

**Harlow Trap 1**
- ROI: ~+5.1%
- Bets: ~8,900
- Improved performance post-2022

**Henlow Trap 2**
- ROI: ~+13.5%
- Smaller sample, higher variance

---

## Walk-Forward Results

Train: 2018–2021  
Test: 2022–2024  

| System | Train ROI | Test ROI |
|--------|----------|---------|
| BSP 8–13 | +0.18% | **+1.41%** |
| Romford Trap 3 | +7.47% | +5.60% |
| Harlow Trap 1 | +1.13% | **+6.47%** |
| Henlow Trap 2 | +17.70% | +9.63% |

---

## Risk Characteristics

Example:

**BSP 8–13**
- Profit: +1739 pts
- Max drawdown: -857 pts

**Romford Trap 3**
- Profit: +672 pts
- Max drawdown: -231 pts

👉 Highlights importance of risk-adjusted evaluation.

---

## Visual Outputs

See `/reports/greyhounds/charts/` for:

- Equity curves
- Drawdown curves
- Combined system comparison

---

## Key Insight

> Edge is not about prediction — it is about structure, persistence, and survivability.

---

## Next Steps

- Combine systems into portfolios
- Add commission modelling
- Introduce Kelly / fractional staking
- Expand to horse racing + football markets
- Integrate into live betting framework

---

## Author

Ben  
BAC Quant Engine Project