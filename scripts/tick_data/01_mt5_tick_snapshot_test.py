from datetime import datetime, timedelta

import MetaTrader5 as mt5
import pandas as pd

# ============================================================
# INITIALISE MT5
# ============================================================

if not mt5.initialize():
    raise RuntimeError("MT5 initialize() failed")

symbol = "GBPUSD"

# ============================================================
# GET RECENT TICKS
# ============================================================

utc_to = datetime.utcnow()
utc_from = utc_to - timedelta(minutes=1)

ticks = mt5.copy_ticks_range(
    symbol,
    utc_from,
    utc_to,
    mt5.COPY_TICKS_ALL
)

if ticks is None or len(ticks) == 0:
    print("No ticks received.")
    mt5.shutdown()
    quit()

# ============================================================
# CONVERT TO DATAFRAME
# ============================================================

df = pd.DataFrame(ticks)

df["time"] = pd.to_datetime(df["time"], unit="s")

print("\nTick Snapshot:")
print(df.head())

print("\nRows collected:", len(df))

print("\nColumns:")
print(df.columns.tolist())

# ============================================================
# SHUTDOWN
# ============================================================

mt5.shutdown()