# BACQE Dukascopy Historical Tick Research

## Objective

Investigate the use of Dukascopy historical tick data as an independent validation source for BACQE microstructure research.

## Initial Scope

Symbol:

* EURUSD

Research Goals:

* Acquire historical bid/ask tick data
* Normalise data into BACQE schema
* Build Tick Bars
* Build Tick Imbalance Bars (TIBs)
* Build Volume Imbalance Bars (VIBs) where possible
* Build Dollar Imbalance Bars (DIBs) where possible
* Replay surviving Script 48 EURUSD candidates
* Evaluate edge persistence across larger historical samples

## Current Status

* Research branch created
* Folder structure created
* Awaiting evaluation of Dukascopy data acquisition methods

## Questions

1. Is an account required?
2. What historical depth is available?
3. What file format is provided?
4. Can downloads be automated?
5. What storage footprint should be expected?
