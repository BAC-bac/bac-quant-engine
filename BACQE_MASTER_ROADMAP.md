# BACQE Master Roadmap

Last updated: 2026-06-07

## Current Mission

Build BACQE into a stable quantitative research platform before expanding into live trading automation.

Current priority:

1. Finish Microstructure Branch v1
2. Keep Sentinel monitoring stable
3. Continue live tick data collection
4. Avoid opening new major branches until Microstructure v1 is reviewed

---

## Branch Status Overview

| Branch | Status | Priority | Current Phase | Next Action |
|---|---|---:|---|---|
| Microstructure | Active | High | Signal research / monitoring | Review latest outputs and fix missing spread features |
| Sentinel | Active | High | Infrastructure health monitoring | Keep running daily and extend checks gradually |
| Regime Engine | Stable | Medium | Maintenance | Revisit after Microstructure v1 |
| Data Registry | Stable | Medium | Weekly monitoring | Check weekly registry output |
| Information Data | Paused | Medium | Macro data expansion | Retry later after Microstructure |
| Dukascopy Research | Future | Low | Not started | Delay until live tick collection matures |
| Sports Data | Paused | Low | Separate research branch | Revisit later |

---

# 1. Microstructure Branch

## Status

Active.

## Goal

Create a stable microstructure research framework using live FTMO tick data.

This branch is complete v1 when BACQE can:

- collect tick data reliably
- generate tick bars
- generate tick imbalance bars
- generate research datasets
- calculate features
- generate candidates
- assess candidate stability
- monitor candidates
- report health through Sentinel

## Already Built

- Tick data capture
- Multi-symbol tick collection
- Tick audit
- Tick dataset summary
- Tick bars
- Time bars comparison
- Volume bars using tick proxy
- Signed ticks
- Tick imbalance bars
- Research datasets
- Candidate generation
- Candidate monitoring

## Known Issues

- Signal factory reported missing spread feature: `spread_range`
- Candidate quality needs reviewing
- Live candidate monitor needs interpretation
- More OOS tick history required

## Next Actions

1. Run latest Sentinel summary
2. Run latest Microstructure monitor
3. Review any missing feature errors
4. Fix missing spread feature handling
5. Re-run signal factory
6. Re-run candidate monitor
7. Commit stable version

## Completion Criteria

Microstructure v1 can be marked complete when:

- latest Sentinel report passes
- tick collection is still active
- no critical missing feature errors
- candidate registry builds successfully
- live candidate monitor produces usable output
- all outputs are documented
- at least 30 days of live tick data has been collected

---

# 2. Sentinel Branch

## Status

Active.

## Goal

Provide health monitoring for BACQE infrastructure.

## Already Built

- Storage check
- Network check
- MT5 check
- Scheduler check
- Sentinel summary report

## Next Actions

1. Continue daily checks during kitchen works
2. Add tick collection health check
3. Add latest file timestamp check
4. Add disk usage growth check

---

# 3. Regime Engine

## Status

Stable.

## Goal

Classify market regimes and support strategy routing.

## Already Built

- Incremental runner
- Profiling
- Recent refresh
- Signal router
- Strategy mapping
- Strategy performance by regime
- Router validation
- Optimisation audit
- Incremental ledger
- Change detector
- Merge planner
- Missing base investigation

## Next Actions

Paused until Microstructure v1 is further along.

---

# 4. Data Registry

## Status

Stable.

## Goal

Maintain an inventory and quality profile of BACQE datasets.

## Next Actions

1. Continue weekly scheduled run
2. Review failed files periodically
3. Use registry to support future dashboards

---

# 5. Information Data

## Status

Paused.

## Goal

Collect macro, economic, and cross-asset data for future BACQE features.

## Next Actions

Paused until Microstructure branch is stabilised.

---

# 6. Dukascopy Historical Tick Research

## Status

Future.

## Goal

Download historical tick data for deeper microstructure backtesting.

## Decision

Do not start yet.

Reason:

Live tick collection is currently more valuable because it supports OOS testing and FTMO-specific behaviour.

Revisit after:

- Microstructure v1 is complete
- 30+ days live tick data collected
- storage and symbol expansion plan confirmed

---

# Current Next Commit

## Commit Target

Review Microstructure branch and fix missing spread feature issue.

## Commit Message Draft

```text
Improve microstructure signal factory feature validation
```

# Build Log

## 2026-06-07

### Infrastructure

- Created BACQE_MASTER_ROADMAP.md
- Added branch status tracking
- Added completion criteria for major BACQE branches

### Sentinel

- Ran Sentinel Summary
- Overall status: WARNING
- Critical checks: 0
- Average health score: 86.69

### Microstructure

Issue identified:
- Signal Factory failing due to missing derived spread features

Actions taken:
- Reviewed script 34 (microstructure_signal_factory.py)
- Verified derived spread feature generation function
- Confirmed function execution after dataset loading
- Re-ran signal factory

Results:
- Signal candidates: 25
- Signal results: 75
- Status counts: {'ok': 75}
- Missing spread feature issue resolved

Live Candidate Monitor:
- Registry rows: 28
- Monitor rows: 18
- Ledger rows: 36
- Statuses: {'ok': 18}
- OOS labels: {'oos_tracking_ok': 18}

Outcome:
- Microstructure branch remains stable
- Candidate monitoring operational
- OOS tracking operational

Next Target:
- Review candidate registry quality
- Review strongest signal candidates
- Continue OOS data collection

## 2026-06-07 - Script 47 Candidate Validation Review

- Added `47_microstructure_candidate_validation_review.py`.
- Validated 28 candidate registry rows.
- Validation labels:
  - `validation_pass_primary`: 9
  - `validation_pass_secondary`: 5
  - `investigate_too_good_to_trust`: 1
  - `reject_low_sample`: 3
  - `reject_concentration_risk`: 10
- Best current candidate group:
  - `eurusd_exclude_bad_days`
  - 594 trades
  - clean risk flags
  - forensic pass
- Main research warning:
  - Several Asia-session candidates remain promising but have `few_unique_dates` and `single_session` risk flags.