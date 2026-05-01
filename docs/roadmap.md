# BAC Quant Engine — Roadmap

## Stage 0 — Foundation

Status: In progress

Goals:

- Create project repository
- Add professional README
- Add project blueprint
- Add roadmap
- Define research principles
- Establish folder standards

Deliverables:

- README.md
- docs/project_blueprint.md
- docs/roadmap.md
- docs/research_principles.md

---

## Stage 1 — Greyhound Research Module

Status: Planned

Goals:

- Bring existing greyhound data work into BAC Quant Engine
- Standardise raw, interim, curated, and report outputs
- Build repeatable ingestion pipeline
- Merge tips and results
- Produce first performance report

Deliverables:

- sports_data/greyhounds/
- scripts/greyhounds/
- research/greyhounds/
- reports/greyhounds/
- config/greyhound_tracks.yaml

---

## Stage 2 — Horse Racing Research Module

Status: Planned

Goals:

- Integrate Betfair BSP horse racing data
- Analyse win and place markets
- Study BSP ranges, streaks, drawdowns, and ROI
- Produce reusable diagnostics

Deliverables:

- sports_data/horse_racing/
- scripts/horse_racing/
- research/horse_racing/
- reports/horse_racing/

---

## Stage 3 — Forex Market Data Module

Status: Planned

Goals:

- Connect to MetaTrader5
- Pull OHLCV data across symbols and timeframes
- Apply technical indicators
- Store enriched market datasets

Deliverables:

- market_data/forex/
- scripts/forex/
- config/symbols.yaml
- config/timeframes.yaml

---

## Stage 4 — Indicator Research Engine

Status: Planned

Goals:

- Test TA-Lib indicators individually
- Score predictive behaviour
- Compare symbols and timeframes
- Prepare indicator rankings

Deliverables:

- strategies/indicators/
- research/indicator_tests/
- reports/indicator_rankings/

---

## Stage 5 — Strategy Backtesting Engine

Status: Planned

Goals:

- Build reusable backtesting templates
- Test financial and sports strategies consistently
- Record metrics such as ROI, drawdown, strike rate, Sharpe, expectancy, and profit factor

Deliverables:

- backtesting/
- reports/backtests/
- research/backtests/

---

## Stage 6 — Regime Engine

Status: Planned

Goals:

- Detect volatility, trend, range, and stress regimes
- Compare strategy performance by regime
- Build regime-aware filters

Deliverables:

- strategies/regime_models/
- research/regime_detection/
- reports/regime_analysis/

---

## Stage 7 — Risk Engine

Status: Planned

Goals:

- Model position sizing
- Apply drawdown rules
- Simulate FTMO-style constraints
- Build convexity-aware risk controls

Deliverables:

- risk_engine/
- reports/risk/
- docs/risk_methodology.md

---

## Stage 8 — Dashboards and Reporting

Status: Planned

Goals:

- Build Streamlit dashboards
- Visualise strategy results
- Summarise reports for GitHub and portfolio use

Deliverables:

- dashboards/
- reports/final/
- docs/dashboard_guide.md

---

## Stage 9 — Public Portfolio Layer

Status: Planned

Goals:

- Prepare selected modules for public GitHub
- Write Medium/LinkedIn posts linked to project milestones
- Build employability narrative

Deliverables:

- content/medium/
- content/linkedin/
- docs/portfolio_notes.md