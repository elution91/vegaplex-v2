"""Show today's iv_history rows to verify write-through is working."""
import sqlite3

c = sqlite3.connect("C:/Projects/vegaplex-v2/backend/analytics/skew_history.db")

print("Today's iv_history rows:")
print("-" * 60)
rows = c.execute(
    "SELECT symbol, date, atm_iv, source FROM iv_history "
    "WHERE date = date('now') ORDER BY symbol LIMIT 20"
).fetchall()

if not rows:
    print("  (none yet — restart backend and scan radar first)")
else:
    for sym, dt, iv, src in rows:
        print(f"  {sym:6}  {dt}  iv={iv:.4f}  source={src}")

print()
print("By source (today only):")
for src, n in c.execute(
    "SELECT source, COUNT(*) FROM iv_history WHERE date = date('now') GROUP BY source"
):
    print(f"  {src}: {n}")
