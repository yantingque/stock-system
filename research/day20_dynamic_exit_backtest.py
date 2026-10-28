"""
項目4：Day20 動態延長持有機制 —— 設計與回測

規則：
  - 每輪倉位（跟項目3「推薦追蹤」開倉邏輯一致：selection_log 達標那天開倉）
    持有到第 20 個交易日為「檢查點」。
  - 檢查點當天看 3 個訊號（全部只用「檢查點當天及之前」的資料，不偷看未來）：
      1. 近5日動能轉負：close.pct_change(5) < 0
      2. 近5日量能萎縮：5日均量 < 20日均量
      3. 跌破短期均線：close < 20日均線
    任一訊號觸發 → 當天賣出結算；全部沒觸發 → 續抱5個交易日，到下一個檢查點
    （25、30、35...）再檢查一次，最多到第 40 個交易日強制賣出（封頂）。
  - 跟「固定20日結算」（項目3現在的規則）比較：同一批倉位，分別套用
    「固定20日賣出」vs「動態延長」，看報酬率、勝率、賣出時機分布有沒有差異。

2026-10 回測結果（48輪可比較倉位，stock_data.db 全歷史）：
  固定20日：平均6.71%／中位數6.17%／勝率72.9%／std11.43%／報酬std比0.587
  動態延長：平均6.03%／中位數4.96%／勝率70.8%／std11.57%／報酬std比0.522
  → 動態延長在「每一個」指標上都比固定20日結算差。
  只有 20.8%(10/48) 的倉位真的被延長（3個訊號都沒觸發，持續續抱）；
  這10輪裡，延長後的報酬平均4.03%，反而比「原本20日就賣」的7.29%更差，
  只有40%的延長案例是賺的（即延長幫倒忙的比例比幫上忙還高）。

  可能的原因：第20天「三個訊號都沒觸發」代表這段時間走勢一路平順向上，
  這種「已經順暢上漲一段、還沒出現任何轉弱訊號」的股票，接下來反而更可能
  是均值回歸（漲多後修正），而不是順勢延續——用「沒有壞訊號」當作「會繼續
  上漲」的理由，這個假設本身可能就有問題。

  結論：不建議採用。樣本量不大（只有10輪真正延長），這個結論不算鐵證，
  但方向一致地全面變差（不是漲跌互現），沒有理由冒這個風險上線。先保留
  這份回測，如果之後想重新設計延長條件（例如改成「出現正向訊號才延長」
  而不是「沒有負向訊號就延長」），可以回來參考這次的做法跟資料管線。
"""
import os
import sqlite3
import numpy as np
import pandas as pd

DB_PATH = os.environ.get("DB_PATH", "stock_data.db")
CHECK_START, CHECK_STEP, CHECK_CAP = 20, 5, 40

conn = sqlite3.connect(DB_PATH)
all_dates = pd.read_sql("SELECT DISTINCT date FROM daily_price ORDER BY date", conn)["date"].tolist()
date_pos = {d: i for i, d in enumerate(all_dates)}

glog = pd.read_sql("""SELECT as_of, stock_id, grade FROM selection_log
                       WHERE grade != '-' ORDER BY stock_id, as_of""", conn)

# ---- 預先算好每檔股票每天的 3 個訊號 + 收盤價序列（向量化，供之後逐倉位查表）----
sig_by_stock = {}
for sid in glog["stock_id"].unique():
    px = pd.read_sql("SELECT date, close, volume FROM daily_price WHERE stock_id=? ORDER BY date",
                      conn, params=(sid,))
    px = px[px["close"] > 0].reset_index(drop=True)
    if len(px) < 60:
        continue
    px["date"] = pd.to_datetime(px["date"])
    px["mom5_neg"] = px["close"].pct_change(5) < 0
    vol5 = px["volume"].rolling(5).mean()
    vol20 = px["volume"].rolling(20).mean()
    px["vol_shrink"] = vol5 < vol20
    ma20 = px["close"].rolling(20).mean()
    px["below_ma20"] = px["close"] < ma20
    px["any_trigger"] = px["mom5_neg"] | px["vol_shrink"] | px["below_ma20"]
    sig_by_stock[sid] = px.set_index("date")

def close_on_or_before(sid, d):
    row = conn.execute("""SELECT date, close FROM daily_price
                          WHERE stock_id=? AND date<=? ORDER BY date DESC LIMIT 1""", (sid, d)).fetchone()
    return row if row else (None, None)

positions = []  # 先重建「開倉時間」清單，邏輯跟項目3完全一致（結算後才能開下一輪）
for sid, sub in glog.groupby("stock_id"):
    if sid not in sig_by_stock:
        continue
    open_until = None
    for entry_date, grade in sub[["as_of", "grade"]].itertuples(index=False):
        if open_until is not None and entry_date <= open_until:
            continue
        if entry_date not in date_pos:
            continue
        idx_entry = date_pos[entry_date]
        # 先算「固定20日」的結算日，決定這輪倉位的 open_until（跟項目3邏輯一致）
        target_idx = idx_entry + CHECK_START
        if target_idx >= len(all_dates):
            open_until = "9999-12-31"
            continue
        _, entry_price = close_on_or_before(sid, entry_date)
        if entry_price is None:
            continue
        positions.append({"stock_id": sid, "entry_date": entry_date, "entry_price": entry_price,
                          "idx_entry": idx_entry, "first_grade": grade})
        # 用「最晚可能的結算日」(第40天) 當作 open_until，避免跟動態延長的倉位重疊
        cap_idx = idx_entry + CHECK_CAP
        open_until = all_dates[cap_idx] if cap_idx < len(all_dates) else "9999-12-31"
conn.close()

print(f"共 {len(positions)} 輪可評估的倉位（兩種規則都用同一批倉位比較）")

rows = []
for pos in positions:
    sid, entry_price, idx_entry = pos["stock_id"], pos["entry_price"], pos["idx_entry"]
    sig = sig_by_stock[sid]

    # ---- 規則A：固定20日結算 ----
    fixed_idx = idx_entry + CHECK_START
    if fixed_idx >= len(all_dates):
        continue
    fixed_date = all_dates[fixed_idx]
    fixed_close = sig["close"].get(pd.Timestamp(fixed_date))
    if fixed_close is None or pd.isna(fixed_close):
        continue
    fixed_ret = (fixed_close / entry_price - 1) * 100

    # ---- 規則B：動態延長（20開始，每5天檢查一次，任一訊號觸發就賣，封頂40）----
    check_idx = idx_entry + CHECK_START
    exit_idx, exit_reason = None, None
    while True:
        if check_idx >= len(all_dates):
            break
        check_date = pd.Timestamp(all_dates[check_idx])
        row = sig.loc[check_date] if check_date in sig.index else None
        if row is None or pd.isna(row["any_trigger"]):
            exit_idx, exit_reason = check_idx, "資料不足提前結算"
            break
        if row["any_trigger"] or check_idx - idx_entry >= CHECK_CAP:
            exit_idx = check_idx
            exit_reason = "觸發訊號" if row["any_trigger"] else "滿40日封頂"
            break
        check_idx += CHECK_STEP
    if exit_idx is None:
        continue
    dyn_date = all_dates[exit_idx]
    dyn_close = sig["close"].get(pd.Timestamp(dyn_date))
    if dyn_close is None or pd.isna(dyn_close):
        continue
    dyn_ret = (dyn_close / entry_price - 1) * 100
    held_days = exit_idx - idx_entry

    rows.append({"股票代號": sid, "進場日": pos["entry_date"], "固定20日報酬%": round(fixed_ret, 2),
                "動態報酬%": round(dyn_ret, 2), "動態持有天數": held_days, "動態賣出原因": exit_reason})

df = pd.DataFrame(rows)
print(f"\n實際配對比較的倉位數: {len(df)}")
print(f"\n=== 固定20日 vs 動態延長 整體比較 ===")
for col in ["固定20日報酬%", "動態報酬%"]:
    s = df[col]
    print(f"{col}: 平均={s.mean():.2f}%  中位數={s.median():.2f}%  勝率={(s>0).mean()*100:.1f}%  "
          f"std={s.std():.2f}%  平均/std(類Sharpe)={s.mean()/s.std():.3f}")

print(f"\n動態延長中「延長超過20天」的比例: {(df['動態持有天數']>20).mean()*100:.1f}%  "
      f"({(df['動態持有天數']>20).sum()}/{len(df)})")
print(f"動態賣出原因分布:\n{df['動態賣出原因'].value_counts()}")

ext = df[df["動態持有天數"] > 20]
print(f"\n=== 只看「真的被延長」的 {len(ext)} 輪：延長後 vs 原本20日就賣，差多少 ===")
print(f"延長後動態報酬: 平均={ext['動態報酬%'].mean():.2f}%  "
      f"原本20日報酬(同一批倉位): 平均={ext['固定20日報酬%'].mean():.2f}%")
print(f"延長有幫助(動態>固定)的比例: {(ext['動態報酬%']>ext['固定20日報酬%']).mean()*100:.1f}%")

df.to_csv("/tmp/claude-0/-home-claude/912d107c-c025-502f-a11a-3b3a9c30936e/scratchpad/day20ext/compare.csv", index=False)
print("\n已存檔：compare.csv")
