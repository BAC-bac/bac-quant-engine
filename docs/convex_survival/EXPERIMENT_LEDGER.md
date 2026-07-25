# BACQE Convex Survival Experiment Ledger

## B00 — Baseline

Status: Completed

Purpose:

Establish the original reference behaviour of the Convex Survival Engine.

Headline result:

- six trades over the baseline research period;
- highly selective behaviour;
- bounded drawdown;
- insufficient opportunity frequency for the intended FTMO use case.

Decision:

Retain as permanent reference configuration.

---

## E01 — Breakout Confirmation Bars

Change:

`InpBreakoutConfirmBars: 2 -> 1`

Result:

- trade count increased;
- false breakouts increased;
- Profit Factor deteriorated;
- drawdown increased.

Decision:

Rejected as a standalone improvement.

---

## E02 — ADX Threshold 22

Change:

`InpMinADX: 27 -> 22`

Result:

No behavioural change from baseline.

Decision:

Rejected.

Interpretation:

ADX was not an active bottleneck within the tested architecture.

---

## E03 — ADX Threshold 15

Change:

`InpMinADX: 27 -> 15`

Result:

No behavioural change from baseline.

Decision:

Rejected.

Interpretation:

Further evidence that ADX was not the active limiting condition.

---

## Gate 3 — Research Instrumentation

Engine:

`BACQE_Convex_Survival_Engine_v0_1_1b_RESEARCH_INSTRUMENTATION`

Status:

CSV creation successfully demonstrated.

Output:

`BACQE_Convex_Research_CSV_PATH_TEST_EURUSD.csv`

Purpose:

Capture filter-level evidence for accepted and rejected observations.

Decision:

Proceed to BACQE Python ingestion and validation before further parameter
experimentation.