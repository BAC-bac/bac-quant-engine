# BAC Quant Engine — Project Blueprint

## 1. Overview

BAC Quant Engine is a unified research platform designed to analyse financial and sports markets using systematic, data-driven approaches.

The goal is not prediction, but understanding:

- market behaviour
- regime shifts
- risk structures
- edge durability

The project focuses on building robust systems rather than fragile strategies.

---

## 2. Core Philosophy

The research is guided by the following principles:

- Robustness over optimisation
- Risk management over prediction
- Convexity over consistency
- Regime awareness over static models
- Data-driven decision making
- Reproducible research pipelines

---

## 3. System Architecture

The system is divided into several layers:

### Data Layer
Responsible for collecting and storing raw data.

- market_data (Forex, indices, macro)
- sports_data (greyhound, horse racing, football)

### Processing Layer
Responsible for cleaning and preparing data.

- scripts
- config
- data standardisation pipelines

### Research Layer
Where analysis and experimentation occurs.

- notebooks
- exploratory data analysis (EDA)
- feature engineering

### Strategy Layer
Where models and ideas are formalised.

- trading strategies
- betting systems
- regime models

### Backtesting Layer
Where strategies are evaluated.

- vectorbt
- custom backtest engines
- simulation frameworks

### Risk Layer
Controls risk exposure and survival.

- FTMO constraints
- drawdown control
- position sizing
- volatility targeting

### Output Layer
Where results are communicated.

- dashboards
- reports
- exports

---

## 4. Initial Modules (Phase 1)

The first phase will focus on integrating existing work:

### Module 1 — Greyhound Data Pipeline
- ingest tips data
- ingest results data
- clean and standardise
- merge into master dataset
- validate data integrity

### Module 2 — Forex Data Pipeline
- connect to MetaTrader5
- collect OHLCV data
- apply TA-Lib indicators
- store in structured format

### Module 3 — Indicator Research Engine
- test individual indicators
- evaluate predictive power
- store results for comparison

---

## 5. Phase 2 — Strategy Development

- build rule-based strategies
- test across symbols and timeframes
- evaluate robustness
- compare performance across regimes

---

## 6. Phase 3 — Regime Detection

- volatility regimes
- trend vs range detection
- structural breaks
- macro overlays

---

## 7. Phase 4 — Risk & Portfolio Engine

- position sizing models
- drawdown control systems
- portfolio allocation (risk parity, etc.)
- FTMO constraint modelling

---

## 8. Phase 5 — Reporting & Visualisation

- dashboards (Streamlit or similar)
- performance summaries
- research reports
- exportable analytics

---

## 9. Long-Term Goals

- build a fully automated research pipeline
- develop robust, scalable strategies
- create a professional quant portfolio
- support potential career opportunities
- explore monetisation opportunities

---

## 10. Current Focus

Current priority:

1. Build clean data pipelines
2. Integrate existing datasets
3. Establish reliable research workflows
4. Begin structured backtesting