# -*- coding: utf-8 -*-
"""
quarterly_ic_check.py —— 規則系統子分數的季度校準工具

用途：每季手動跑一次（不是自動排程，校準結果要人工確認才套用到
daily_run.py 的 PROPOSAL_PARAMS["PTS"]），檢查 10 個評分子項目
（score_technical_v1 / score_fundamental_v1 的每一項）跟「訊號發生後
20 個交易日報酬率」的關係是否還成立。

做法：
  1. 用 stock_data.db 全歷史，向量化重現每個子項目的訊號（跟正式選股
     邏輯一致：技術面用 rolling，基本面用 effective_date <= as_of 的
     asof 合併，不偷看未來）。
  2. 算全樣本 IC、依季度拆分看穩不穩定、多元回歸控制共線性。
  3. 用一個時間切點切 train/test，只用 train 的結果決定「候選新權重」，
     在 test（完全沒看過的資料）上驗證總分的 IC 跟頭尾十分位報酬率
     是否真的變好——這一步是避免「用同一批資料決定權重又拿來證明權重
     有效」的循環驗證陷阱。

校準決策原則（避免追逐單次噪音）：
  - 只有「全樣本 + train-only + 多元回歸」三個檢查都一致、且都統計
    顯著的項目，才調整權重；方向不穩定、或只在其中一兩個檢查顯著的
    項目，這次先不動，留給下一次校準週期再觀察。
  - 調整時優先考慮「歸零」明顯的負向項目，而不是急著把釋出的權重
    重新分配給別的項目——重新分配是更大的假設，要更多次校準都一致
    才做。

校準紀錄（人工維護，調整過權重時請在這裡加一筆，並同步寫一筆到
daily_run.py 的 PROPOSAL_PARAMS 上方註解）：
  - 2026-10：第 1 次校準。rsi/roe/inst 三項全樣本、train-only、多元
    回歸都一致判定為負向貢獻，歸零（15→85滿分，門檻等比例下調到
    51/68）。其餘 7 項證據不夠穩定，先不動。Out-of-sample（切點
    2024-07-01）：總分 IC 0.080→0.091，頂部10%分組報酬 5.89%→6.41%。

用法：
    python calibration/quarterly_ic_check.py [--db DB_PATH] [--cutoff 2024-07-01]
"""
import argparse
import os
import sqlite3

import numpy as np
import pandas as pd
from scipy import stats

try:
    import statsmodels.api as sm
except ImportError:
    sm = None

FWD_DAYS = 20
MA_LONG, CROSS_FAST, CROSS_SLOW, EVENT_WINDOW = 240, 20, 60, 5
RSI_LOW, RSI_HIGH = 40, 60
YIELD_MIN, REV_YOY_MIN, ROE_MIN, INST_STREAK = 4.0, 0.10, 0.10, 3

SUBITEMS = ["站上年線", "黃金交叉", "MACD翻正", "RSI區間", "量價配合",
            "殖利率", "月營收YoY", "EPS連兩季升", "ROE", "法人連買"]

# 目前生產環境的權重（daily_run.py PROPOSAL_PARAMS["PTS"]），每次校準前
# 先手動同步這裡，確保比較基準是「現在真的在跑的那一版」。
CURRENT_PTS = {"站上年線": 15, "黃金交叉": 15, "MACD翻正": 10, "RSI區間": 0, "量價配合": 5,
               "殖利率": 15, "月營收YoY": 15, "EPS連兩季升": 10, "ROE": 0, "法人連買": 0}


def build_panel(db_path):
    """重現 10 個子項目訊號 + forward 20 日報酬率，回傳完整面板 DataFrame。"""
    conn = sqlite3.connect(db_path)
    stock_ids = pd.read_sql("SELECT DISTINCT stock_id FROM daily_price", conn)["stock_id"].tolist()

    val_all = pd.read_sql("SELECT stock_id, date, dividend_yield FROM valuation", conn)
    rev_all = pd.read_sql(
        "SELECT stock_id, revenue_year, revenue_month, revenue, effective_date FROM monthly_revenue", conn)
    eps_all = pd.read_sql(
        "SELECT stock_id, date, value, effective_date FROM financial_statement WHERE type='EPS'", conn)
    inc_all = pd.read_sql(
        "SELECT stock_id, date, value, effective_date FROM financial_statement WHERE type='IncomeAfterTaxes'", conn)
    eq_all = pd.read_sql(
        "SELECT stock_id, date, value, effective_date FROM balance_sheet "
        "WHERE type='EquityAttributableToOwnersOfParent'", conn)
    inst_all = pd.read_sql(
        "SELECT stock_id, date, name, net FROM institutional "
        "WHERE name IN ('Foreign_Investor','Investment_Trust')", conn)

    all_rows = []
    for sid in stock_ids:
        px = pd.read_sql("SELECT date, close, volume FROM daily_price WHERE stock_id=? ORDER BY date",
                          conn, params=(sid,))
        # 資料清理：close<=0 是明顯的資料錯誤（停牌/來源當天掛0），丟掉避免污染計算。
        px = px[px["close"] > 0].reset_index(drop=True)
        if len(px) < MA_LONG + FWD_DAYS + 5:
            continue
        px["date"] = pd.to_datetime(px["date"])
        close, vol = px["close"], px["volume"]

        ma_long = close.rolling(MA_LONG).mean()
        px["站上年線"] = (close > ma_long).astype(float)

        ma_f = close.rolling(CROSS_FAST).mean()
        ma_s = close.rolling(CROSS_SLOW).mean()
        cross = ((ma_f.shift(1) <= ma_s.shift(1)) & (ma_f > ma_s)).astype(int)
        px["黃金交叉"] = cross.rolling(EVENT_WINDOW).max()

        ema12, ema26 = close.ewm(span=12, adjust=False).mean(), close.ewm(span=26, adjust=False).mean()
        macd = ema12 - ema26
        hist = macd - macd.ewm(span=9, adjust=False).mean()
        turn = ((hist.shift(1) <= 0) & (hist > 0)).astype(int)
        px["MACD翻正"] = turn.rolling(EVENT_WINDOW).max()

        delta = close.diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = (-delta.clip(upper=0)).rolling(14).mean()
        rsi = 100 - 100 / (1 + gain / loss)
        px["RSI區間"] = ((rsi >= RSI_LOW) & (rsi <= RSI_HIGH)).astype(float)

        px["量價配合"] = ((close.diff() > 0) & (vol.diff() > 0)).astype(float)
        px["fwd_ret"] = close.shift(-FWD_DAYS) / close - 1
        px = px.sort_values("date").reset_index(drop=True)

        v = val_all[val_all.stock_id == sid].copy()
        if not v.empty:
            v["date"] = pd.to_datetime(v["date"])
            px = pd.merge_asof(px, v[["date", "dividend_yield"]].sort_values("date"), on="date")
            px["殖利率"] = (px["dividend_yield"] > YIELD_MIN).astype(float)
        else:
            px["殖利率"] = np.nan

        r = rev_all[rev_all.stock_id == sid].copy()
        if not r.empty:
            r["eff"] = pd.to_datetime(r["effective_date"])
            r = r.sort_values(["revenue_year", "revenue_month"])
            rev_map = {(row.revenue_year, row.revenue_month): row.revenue for row in r.itertuples()}
            r["yoy"] = [(row.revenue / rev_map.get((row.revenue_year - 1, row.revenue_month), np.nan) - 1)
                        if rev_map.get((row.revenue_year - 1, row.revenue_month), 0) else np.nan
                        for row in r.itertuples()]
            r["月營收YoY"] = (r["yoy"] > REV_YOY_MIN).astype(float)
            r = r.dropna(subset=["eff"]).sort_values("eff")
            px = pd.merge_asof(px, r[["eff", "月營收YoY"]].rename(columns={"eff": "date"}), on="date")
        else:
            px["月營收YoY"] = np.nan

        e = eps_all[eps_all.stock_id == sid].copy().sort_values("date")
        if len(e) >= 3:
            e["eff"] = pd.to_datetime(e["effective_date"])
            e = e.sort_values("eff")
            v3 = e["value"].values
            flag = np.zeros(len(e))
            for i in range(2, len(e)):
                if v3[i - 2] < v3[i - 1] < v3[i]:
                    flag[i] = 1.0
            e["EPS連兩季升"] = flag
            e = e.dropna(subset=["eff"]).sort_values("eff")
            px = pd.merge_asof(px, e[["eff", "EPS連兩季升"]].rename(columns={"eff": "date"}), on="date")
        else:
            px["EPS連兩季升"] = np.nan

        inc = inc_all[inc_all.stock_id == sid][["date", "effective_date", "value"]].rename(columns={"value": "inc"})
        eq = eq_all[eq_all.stock_id == sid][["date", "effective_date", "value"]].rename(columns={"value": "eq"})
        if not inc.empty and not eq.empty:
            roe_df = pd.merge(inc, eq, on=["date", "effective_date"], how="inner")
            if not roe_df.empty:
                roe_df["roe"] = roe_df["inc"] * 4 / roe_df["eq"]
                roe_df["ROE"] = (roe_df["roe"] > ROE_MIN).astype(float)
                roe_df["eff"] = pd.to_datetime(roe_df["effective_date"])
                roe_df = roe_df.dropna(subset=["eff"]).sort_values("eff")
                px = pd.merge_asof(px, roe_df[["eff", "ROE"]].rename(columns={"eff": "date"}), on="date")
            else:
                px["ROE"] = np.nan
        else:
            px["ROE"] = np.nan

        inst = inst_all[inst_all.stock_id == sid].copy()
        if not inst.empty:
            inst["date"] = pd.to_datetime(inst["date"])
            piv = inst.pivot_table(index="date", columns="name", values="net").sort_index()
            flags = pd.Series(False, index=piv.index)
            for col in piv.columns:
                s = piv[col]
                streak_ok = (s > 0).rolling(INST_STREAK).apply(lambda x: x.all(), raw=True).fillna(0).astype(bool)
                flags = flags | streak_ok
            inst_flag = flags.rename("法人連買").reset_index()
            inst_flag["法人連買"] = inst_flag["法人連買"].astype(float)
            px = pd.merge_asof(px, inst_flag.sort_values("date"), on="date")
        else:
            px["法人連買"] = np.nan

        px["stock_id"] = sid
        px["quarter"] = px["date"].dt.quarter
        all_rows.append(px)

    panel = pd.concat(all_rows, ignore_index=True)
    return panel.dropna(subset=["fwd_ret"])


def print_ic_report(panel):
    print(f"總樣本數: {len(panel)}  股票數: {panel['stock_id'].nunique()}")

    print("\n=== 全樣本 IC（子項目訊號 vs 20日後報酬率）===")
    overall = []
    for item in SUBITEMS:
        sub = panel.dropna(subset=[item])
        if sub[item].nunique() < 2:
            continue
        ic, p = stats.pointbiserialr(sub[item], sub["fwd_ret"])
        hit, miss = sub.loc[sub[item] == 1, "fwd_ret"], sub.loc[sub[item] == 0, "fwd_ret"]
        overall.append({"子項目": item, "n_hit": len(hit), "IC": round(ic, 4), "p": round(p, 4),
                        "命中平均報酬%": round(hit.mean() * 100, 2), "未命中平均報酬%": round(miss.mean() * 100, 2)})
    overall_df = pd.DataFrame(overall).sort_values("IC", ascending=False)
    print(overall_df.to_string(index=False))

    print("\n=== 依季度拆分 IC（檢查是否穩定）===")
    rows = []
    for item in SUBITEMS:
        sub = panel.dropna(subset=[item])
        for q in [1, 2, 3, 4]:
            s = sub[sub["quarter"] == q]
            if s[item].nunique() < 2 or len(s) < 100:
                continue
            ic, _ = stats.pointbiserialr(s[item], s["fwd_ret"])
            rows.append({"子項目": item, "季": q, "IC": round(ic, 4)})
    if rows:
        pvt = pd.DataFrame(rows).pivot(index="子項目", columns="季", values="IC")
        print(pvt.reindex(overall_df["子項目"]).to_string())

    if sm is not None:
        print("\n=== 多元回歸（控制共線性後的淨效果）===")
        reg_df = panel.dropna(subset=SUBITEMS + ["fwd_ret"]).copy()
        X = sm.add_constant(reg_df[SUBITEMS].astype(float))
        model = sm.OLS(reg_df["fwd_ret"].astype(float), X).fit(
            cov_type="cluster", cov_kwds={"groups": reg_df["date"]})
        print(model.summary().tables[1])
    return overall_df


def oos_check(panel, cutoff, candidate_pts, current_pts=CURRENT_PTS):
    """用 cutoff 切 train/test，在 test（candidate_pts 設計時沒看過的資料）
    上比較 current_pts vs candidate_pts 的總分 IC 跟頭尾十分位報酬率。"""
    cutoff = pd.Timestamp(cutoff)
    test = panel[panel["date"] >= cutoff].dropna(subset=SUBITEMS + ["fwd_ret"]).copy()
    print(f"\n=== Out-of-sample 驗證（切點 {cutoff.date()}，測試集 n={len(test)}）===")
    for name, pts in [("目前權重", current_pts), ("候選新權重", candidate_pts)]:
        score = sum(test[item] * w for item, w in pts.items())
        ic, p = stats.pearsonr(score, test["fwd_ret"])
        top = test.loc[score >= score.quantile(0.9), "fwd_ret"]
        bot = test.loc[score <= score.quantile(0.1), "fwd_ret"]
        print(f"{name}（滿分{sum(pts.values())}）：IC={ic:.4f} (p={p:.4f})  "
              f"頂部10%平均報酬={top.mean()*100:.2f}%  底部10%平均報酬={bot.mean()*100:.2f}%")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=os.environ.get("DB_PATH", "stock_data.db"))
    ap.add_argument("--cutoff", default="2024-07-01", help="train/test 切點，用來驗證候選新權重")
    args = ap.parse_args()

    panel = build_panel(args.db)
    print_ic_report(panel)
    oos_check(panel, args.cutoff, candidate_pts=CURRENT_PTS)  # 跟自己比較，當作每次校準的基準線
    print("\n若這次校準要調整權重：先在這個檔案上方修改 CURRENT_PTS 旁另開一個候選版本，\n"
          "手動呼叫 oos_check(panel, cutoff, candidate_pts=你的新版本) 驗證過，\n"
          "確認 out-of-sample 真的變好，才去改 daily_run.py 的 PROPOSAL_PARAMS，\n"
          "並記得回來更新本檔案最上方的「校準紀錄」跟 CURRENT_PTS。")
