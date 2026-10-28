# -*- coding: utf-8 -*-
"""
daily_run.py — 多策略自動選股系統：每日管線（排程執行版）

由 daily_pipeline.ipynb 的執行邏輯轉出，內容一對一相同：
資料更新（雙路徑日期防呆）→ 評分選股 → Google Sheet 上傳 → LINE 推播。

維護方式：策略與邏輯的修改在 notebook 開發測試，確認後同步到本檔，
commit + push，雲端隔天自動採用新版。
"""

# ========== 1. 匯入與設定 ==========

# ============ 匯入套件 ============
import os                           # Python 內建：路徑檢查
import sqlite3                      # Python 內建：SQLite 資料庫
import time                         # Python 內建：sleep 用
from datetime import datetime, timedelta, date

import pandas as pd                 # 第三方：表格運算
import requests                     # 第三方：HTTP 請求
from FinMind.data import DataLoader # 第三方：台股資料

# ============ 載入設定檔 ============
# 從 config.py 匯入所有設定（DB_PATH、FINMIND_TOKEN、LINE_*、GSHEET_* ...）。
# config.py 採「環境變數優先」：GitHub Actions 由 Secrets 注入，
# 本機則放一個 config_local.py 提供實際值（詳見 config.py 內的說明）。
from config import *

# 建立 FinMind 下載器（建立當下就會連線登入，需在有網路環境執行）
api = DataLoader(token=FINMIND_TOKEN)


def now_taipei():
    """回傳「台灣時間」的現在時刻。

    重要：GitHub Actions 的雲端機器時區是 UTC（比台灣慢 8 小時），
    直接用 datetime.now() 會讓「15:00 後才走當日路徑」等判斷全部錯亂。
    所以一律用 zoneinfo 明確取 Asia/Taipei——本機（本來就在台灣時區）
    和雲端跑出來的行為因此完全一致。
    """
    from zoneinfo import ZoneInfo               # Python 3.9+ 內建時區資料庫
    return datetime.now(ZoneInfo("Asia/Taipei")).replace(tzinfo=None)


print("設定載入完成，資料庫：", DB_PATH)

# ========== 2. 資料庫工具 ==========

def clean_number(value):
    """證交所字串數字 → float（"1,234.56"/"--"/"" → 數字或 None）。"""
    if isinstance(value, (int, float)):
        return float(value)
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if text in ("", "--", "-", "N/A"):
        return None
    try:
        return float(text)
    except ValueError:
        return None


def upsert_dataframe(df, table, columns):
    """DataFrame 寫入資料表（INSERT OR REPLACE，主鍵重複覆蓋）。回傳筆數。"""
    if df is None or df.empty:
        return 0
    conn = sqlite3.connect(DB_PATH)
    placeholders = ",".join(["?"] * len(columns))
    sql = f"INSERT OR REPLACE INTO {table} ({','.join(columns)}) VALUES ({placeholders})"
    rows = list(df[columns].itertuples(index=False, name=None))
    conn.executemany(sql, rows)
    conn.commit()
    conn.close()
    return len(rows)


def get_latest_date(table, stock_id=None, date_column="date"):
    """查某表（可指定股票）最新一筆日期。無資料回傳 None。"""
    conn = sqlite3.connect(DB_PATH)
    if stock_id:
        cur = conn.execute(
            f"SELECT MAX({date_column}) FROM {table} WHERE stock_id = ?", (stock_id,))
    else:
        cur = conn.execute(f"SELECT MAX({date_column}) FROM {table}")
    result = cur.fetchone()[0]
    conn.close()
    return result


def next_day(date_str):
    """'YYYY-MM-DD' → 隔天的 'YYYY-MM-DD'。"""
    return (datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")


def ensure_extra_tables():
    """建立整合版新增的幾張表（已存在則跳過）。"""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stock_info (
            stock_id   TEXT PRIMARY KEY,
            stock_name TEXT
        )""")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS run_log (
            run_at   TEXT,               -- 執行時刻（含時分秒）
            as_of    TEXT,               -- 對應的交易日
            stage    TEXT,               -- 站名：update / select / gsheet / push
            status   TEXT,               -- ok / fail / skip
            message  TEXT,
            PRIMARY KEY (run_at, stage)
        )""")
    # 2026-10 新增：大盤指數（目前只存加權指數 TAIEX），獨立成一張表，不混進
    # daily_price——避免「指數」被 get_pool_ids() 誤當成股票拿去選股評分。
    conn.execute("""
        CREATE TABLE IF NOT EXISTS benchmark_index (
            index_id TEXT NOT NULL,      -- 目前只有 'TAIEX'（發行量加權股價指數）
            date     TEXT NOT NULL,
            close    REAL,
            PRIMARY KEY (index_id, date)
        )""")
    conn.commit()
    conn.close()


def log_stage(as_of, stage, status, message=""):
    """寫一筆執行紀錄到 run_log。"""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("INSERT OR REPLACE INTO run_log VALUES (?,?,?,?,?)",
                 (now_taipei().strftime("%Y-%m-%d %H:%M:%S"), as_of, stage, status, str(message)[:500]))
    conn.commit()
    conn.close()


def stage_done(as_of, stage):
    """查某交易日的某一站是否已成功執行過（防重複推播用）。"""
    conn = sqlite3.connect(DB_PATH)
    n = conn.execute(
        "SELECT COUNT(*) FROM run_log WHERE as_of = ? AND stage = ? AND status = 'ok'",
        (as_of, stage)).fetchone()[0]
    conn.close()
    return n > 0


def get_pool_ids():
    """股票池 = 資料庫裡有股價資料的股票。"""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT DISTINCT stock_id FROM daily_price ORDER BY stock_id", conn)
    conn.close()
    return df["stock_id"].tolist()


def ensure_stock_names(stock_ids):
    """確保 stock_info 表有股票池所有股票的名稱；缺漏時補抓一次（1 次請求）。"""
    conn = sqlite3.connect(DB_PATH)
    have = set(pd.read_sql("SELECT stock_id FROM stock_info", conn)["stock_id"])
    conn.close()
    missing = [sid for sid in stock_ids if sid not in have]
    if missing:
        info = api.taiwan_stock_info().drop_duplicates(subset="stock_id")
        info = info[info["stock_id"].isin(stock_ids)]
        upsert_dataframe(info.rename(columns={"stock_name": "stock_name"}),
                         "stock_info", ["stock_id", "stock_name"])
        print(f"補齊股票名稱 {len(info)} 檔")


def get_name_map():
    """回傳 {代號: 名稱} 字典，推播與顯示用。"""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT stock_id, stock_name FROM stock_info", conn)
    conn.close()
    return dict(zip(df["stock_id"], df["stock_name"]))


ensure_extra_tables()
print("工具與資料表就緒")

# ========== 3. 資料更新（雙路徑日期防呆） ==========

# ---------- 當日路徑：證交所快速更新（日期已被 T86 確認） ----------

def fetch_t86(query_date):
    """以日期參數查詢 T86 三大法人。回傳 payload dict；非交易日/未公布時 stat 不是 OK。"""
    url = "https://www.twse.com.tw/rwd/zh/fund/T86"
    params = {"date": query_date.strftime("%Y%m%d"),
              "selectType": "ALLBUT0999", "response": "json"}
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fresh_update_from_twse(trade_date, t86_payload, stock_ids):
    """證交所快速更新：股價+快照、估值、法人。trade_date 已由 T86 確認。"""
    d = trade_date.strftime("%Y-%m-%d")

    # ---- 股價 + 全市場快照 ----
    resp = requests.get("https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL",
                        timeout=30)
    resp.raise_for_status()
    data = resp.json()
    rows, snap = [], []
    for item in data:
        sid = item.get("Code")
        close = clean_number(item.get("ClosingPrice"))
        tv = clean_number(item.get("TradeValue"))
        if sid and close is not None and tv is not None:
            snap.append({"date": d, "stock_id": sid, "close": close, "trade_value": tv})
        if sid in stock_ids:
            rows.append({"stock_id": sid, "date": d,
                         "open": clean_number(item.get("OpeningPrice")),
                         "high": clean_number(item.get("HighestPrice")),
                         "low": clean_number(item.get("LowestPrice")),
                         "close": close,
                         "volume": clean_number(item.get("TradeVolume"))})
    n1 = upsert_dataframe(pd.DataFrame(rows), "daily_price",
                          ["stock_id", "date", "open", "high", "low", "close", "volume"])
    upsert_dataframe(pd.DataFrame(snap), "market_snapshot",
                     ["date", "stock_id", "close", "trade_value"])

    # ---- 估值 ----
    resp = requests.get("https://openapi.twse.com.tw/v1/exchangeReport/BWIBBU_ALL",
                        timeout=30)
    resp.raise_for_status()
    vrows = [{"stock_id": it.get("Code"), "date": d,
              "per": clean_number(it.get("PEratio")),
              "pbr": clean_number(it.get("PBratio")),
              "dividend_yield": clean_number(it.get("DividendYield"))}
             for it in resp.json() if it.get("Code") in stock_ids]
    n2 = upsert_dataframe(pd.DataFrame(vrows), "valuation",
                          ["stock_id", "date", "per", "pbr", "dividend_yield"])

    # ---- 法人（沿用剛才查 T86 拿到的 payload，不用再請求一次） ----
    fields = t86_payload["fields"]
    irows = []
    for raw in t86_payload["data"]:
        rec = dict(zip(fields, raw))
        sid = str(rec.get("證券代號", "")).strip()
        if sid not in stock_ids:
            continue
        for name, b, s in [
            ("Foreign_Investor", rec.get("外陸資買進股數(不含外資自營商)"),
                                  rec.get("外陸資賣出股數(不含外資自營商)")),
            ("Investment_Trust", rec.get("投信買進股數"), rec.get("投信賣出股數")),
            ("Dealer_self", rec.get("自營商買進股數(自行買賣)"),
                            rec.get("自營商賣出股數(自行買賣)")),
        ]:
            bn, sn = clean_number(b), clean_number(s)
            if bn is None and sn is None:
                continue
            irows.append({"stock_id": sid, "date": d, "name": name,
                          "buy": int(bn or 0), "sell": int(sn or 0),
                          "net": int((bn or 0) - (sn or 0))})
    n3 = upsert_dataframe(pd.DataFrame(irows), "institutional",
                          ["stock_id", "date", "name", "buy", "sell", "net"])
    print(f"當日路徑更新完成（{d}）：股價 {n1}、估值 {n2}、法人 {n3} 筆")


# ---------- 補跑路徑：FinMind 增量（日期自帶，缺幾天補幾天） ----------

def catchup_with_finmind(stock_ids):
    """FinMind 增量補抓股價/法人/估值。從各檔「資料庫最新日+1」抓到現在。"""
    fallback_start = "2020-01-01"          # 完全沒資料時的起點（正常不會用到）
    total = {"price": 0, "inst": 0, "val": 0}

    for i, sid in enumerate(stock_ids, start=1):
        try:
            # 股價
            latest = get_latest_date("daily_price", sid)
            start = next_day(latest) if latest else fallback_start
            df = api.taiwan_stock_daily(stock_id=sid, start_date=start)
            if not df.empty:
                df = df.rename(columns={"max": "high", "min": "low",
                                        "Trading_Volume": "volume"})
                total["price"] += upsert_dataframe(
                    df, "daily_price",
                    ["stock_id", "date", "open", "high", "low", "close", "volume"])
            time.sleep(0.3)

            # 法人
            latest = get_latest_date("institutional", sid)
            start = next_day(latest) if latest else fallback_start
            df = api.taiwan_stock_institutional_investors(stock_id=sid, start_date=start)
            if not df.empty:
                df["net"] = df["buy"] - df["sell"]
                total["inst"] += upsert_dataframe(
                    df, "institutional",
                    ["stock_id", "date", "name", "buy", "sell", "net"])
            time.sleep(0.3)

            # 估值
            latest = get_latest_date("valuation", sid)
            start = next_day(latest) if latest else fallback_start
            df = api.taiwan_stock_per_pbr(stock_id=sid, start_date=start)
            if not df.empty:
                df = df.rename(columns={"PER": "per", "PBR": "pbr"})
                total["val"] += upsert_dataframe(
                    df, "valuation",
                    ["stock_id", "date", "per", "pbr", "dividend_yield"])
            time.sleep(0.3)
        except Exception as e:
            print(f"  ⚠ {sid} 補抓失敗：{e}")

        if i % 20 == 0:
            print(f"  補跑進度 {i}/{len(stock_ids)}")

    print(f"補跑路徑完成：股價 {total['price']}、法人 {total['inst']}、估值 {total['val']} 筆")


# 2026-10 新增：大盤指數（TAIEX）補抓，供「推薦追蹤」報表算超額報酬用。
# ⚠ 尚未在有 FINMIND_TOKEN／可連外網路的環境實測過，部署前請先手動執行一次驗證：
#   python -c "from daily_run import update_benchmark_index; update_benchmark_index()"
def update_benchmark_index():
    """補抓大盤指數（目前只有 TAIEX）到 benchmark_index 表，從資料庫既有最新日期+1
    抓到現在；完全沒資料時從 2010-01-01 開始。失敗不應中斷主流程，呼叫端自行 try/except。
    """
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute(
        "SELECT MAX(date) FROM benchmark_index WHERE index_id='TAIEX'").fetchone()
    conn.close()
    latest = row[0] if row else None
    start = next_day(latest) if latest else "2010-01-01"

    df = api.taiwan_stock_daily(stock_id="TAIEX", start_date=start)
    if df.empty:
        print(f"大盤指數（TAIEX）無新資料（已從 {start} 查詢）")
        return 0

    df = df.rename(columns={"stock_id": "index_id"})
    df["index_id"] = "TAIEX"
    n = upsert_dataframe(df, "benchmark_index", ["index_id", "date", "close"])
    print(f"大盤指數（TAIEX）更新 {n} 筆（{start} 之後）")
    return n


# ---------- 低頻更新（月營收/季報，沿用原本的節奏） ----------

def lowfreq_update(stock_ids):
    """每月 10~13 號補月營收；財報公告期補損益表與資產負債表。"""
    today = now_taipei().date()

    def _eff_dates(df):
        rd = pd.to_datetime(df["date"])
        import numpy as np
        return (rd + pd.to_timedelta(np.where(rd.dt.month == 12, 90, 45), unit="D")
                ).dt.strftime("%Y-%m-%d")

    if 10 <= today.day <= 13:
        print("月營收更新期 → 補月營收...")
        for sid in stock_ids:
            try:
                df = api.taiwan_stock_month_revenue(stock_id=sid, start_date="2020-01-01")
                if df.empty:
                    continue
                def eff(row):
                    y, m = int(row["revenue_year"]), int(row["revenue_month"])
                    return f"{y+1}-01-10" if m == 12 else f"{y}-{str(m+1).zfill(2)}-10"
                df["effective_date"] = df.apply(eff, axis=1)
                upsert_dataframe(df, "monthly_revenue",
                    ["stock_id", "revenue_year", "revenue_month", "revenue", "effective_date"])
                time.sleep(0.3)
            except Exception as e:
                print(f"  ⚠ {sid} 月營收失敗：{e}")

    if today.month in (3, 5, 8, 11) and 14 <= today.day <= 17:
        print("財報公告期 → 補財報...")
        for sid in stock_ids:
            try:
                for fetch, table in [(api.taiwan_stock_financial_statement, "financial_statement"),
                                     (api.taiwan_stock_balance_sheet, "balance_sheet")]:
                    df = fetch(stock_id=sid, start_date="2020-01-01")
                    if not df.empty:
                        df["effective_date"] = _eff_dates(df)
                        upsert_dataframe(df, table,
                            ["stock_id", "date", "type", "value", "effective_date"])
                    time.sleep(0.3)
            except Exception as e:
                print(f"  ⚠ {sid} 財報失敗：{e}")


# ---------- 更新總調度（防呆的核心） ----------

def update_market_data(stock_ids):
    """依時間與資料狀態自動選擇當日/補跑路徑。回傳更新後資料庫的最新交易日。"""
    now = now_taipei()
    today = now.date()
    today_str = today.isoformat()

    # 2026-10 新增：大盤指數補抓，獨立 try/except，失敗不影響主流程
    try:
        update_benchmark_index()
    except Exception as e:
        print(f"⚠ 大盤指數更新失敗（不影響主流程）：{e}")

    # 防呆 0：今天的資料已經在庫裡 → 同日重跑，什麼都不用抓
    if get_latest_date("daily_price") == today_str:
        print("今日資料已存在，跳過更新（同日重跑）")
        return today_str

    # 防呆 1：當日路徑的三重確認——平日 + 15:00 後 + T86 正面確認
    if today.weekday() < 5 and now.hour >= 15:
        t86 = fetch_t86(today)
        if t86.get("stat") == "OK":
            fresh_update_from_twse(today, t86, stock_ids)
            lowfreq_update(stock_ids)
            return today_str
        print(f"T86 尚無今日資料（stat={t86.get('stat')}）→ 改走補跑路徑")

    # 防呆 2：補跑前先確認「有沒有缺口」——已是最新就不要浪費 300 次請求
    # expected = 目前理論上「最近一個已收盤的平日」：
    #   平日 15:00 後 → 今天；否則往回找最近的平日（週六日跳過）
    probe = today if (today.weekday() < 5 and now.hour >= 15) else today - timedelta(days=1)
    while probe.weekday() >= 5:            # 5=週六 6=週日 → 繼續往前
        probe -= timedelta(days=1)
    expected = probe.isoformat()

    latest_in_db = get_latest_date("daily_price")
    if latest_in_db and latest_in_db >= expected:
        # 資料已涵蓋到最近的平日 → 無缺口，直接沿用
        # （注意：expected 若剛好是國定假日，會走下面的補跑但抓回 0 筆，
        #   一次無害的空查詢，隔天就恢復正常）
        print(f"資料已是最新（{latest_in_db}），跳過補抓")
        return latest_in_db

    # 防呆 3：確認有缺口才補跑（隔日早上、假日後、連假後、斷網後）
    print("執行 FinMind 增量補跑（依資料庫缺口自動補齊，日期由 FinMind 提供）")
    catchup_with_finmind(stock_ids)
    lowfreq_update(stock_ids)
    return get_latest_date("daily_price")


print("資料更新函式就緒")

# ========== 4. 策略區（改選股方式改這裡） ==========

# ==================================================================
#                    ★ 策 略 區（要改選股方式改這裡）★
# ==================================================================

# ---- 推薦追蹤結算天數：每筆推薦固定持有這麼多個「交易日」後結算（不是日曆天）----
# 2026-10 起改為固定 20 個交易日結算（原本是「無限期累積到現在」），
# 理由：讓「推薦追蹤」跟回測的評估慣例（20 個交易日）一致，才能互相比較。
SETTLE_AFTER_DAYS = 20

# ---- 季度校準紀錄 ----
# 2026-10 第 1 次校準（calibration/quarterly_ic_check.py，用 2010~2026 全歷史
# + 2024-07 切 train/test 驗證過）：rsi、roe、inst 三項在「全樣本」「只用
# train」「控制其他項目的多元回歸」三個檢查都一致判定為負向貢獻（分數越高、
# 未來20日報酬率反而越低），歸零處理；其他 7 項證據不夠穩定（尤其 ma 在全
# 樣本顯著但 train-only 不顯著，疑似跟最近一段時期的大盤狀況有關），這次先
# 不動，留給下次校準再觀察，避免用單次結果追逐噪音。Out-of-sample 驗證：
# 新權重在完全沒看過的 2024-07 之後測試集，總分跟20日報酬率的相關係數從
# 0.080 提升到 0.091，頂部10%分組平均報酬 5.89%→6.41%。下次校準建議抓
# 2027 Q1（資料累積更多之後，可以重新檢查 ma/yield/vol/macd/cross 這幾項
# 證據是否變穩定）。

# ---- 提案 v1 的所有參數（門檻集中管理，不埋在程式裡） ----
PROPOSAL_PARAMS = {
    # 評級門檻（滿分從100降到85後，等比例下調，維持跟原來一樣的選股鬆緊度）
    "WATCHLIST_SCORE": 51,       # 觀察清單（原60，60/100*85）
    "STRONG_SCORE":    68,       # 強烈推薦（原80，80/100*85）
    # 技術面
    "MA_LONG": 240,              # 年線天數
    "CROSS_FAST": 20,            # 黃金交叉快線
    "CROSS_SLOW": 60,            # 黃金交叉慢線
    "EVENT_WINDOW": 5,           # 交叉/翻正事件的回看天數
    "RSI_LOW": 40, "RSI_HIGH": 60,
    # 基本面
    "YIELD_MIN": 4.0,            # 殖利率門檻（%）
    "REV_YOY_MIN": 0.10,         # 月營收年增門檻
    "ROE_MIN": 0.10,             # ROE 門檻
    "INST_STREAK": 3,            # 法人連買天數
    # 配分（rsi/roe/inst 2026-10 校準後歸零，理由見上方校準紀錄）
    "PTS": {"ma": 15, "cross": 15, "macd": 10, "rsi": 0, "vol": 5,
            "yield": 15, "rev": 15, "eps": 10, "roe": 0, "inst": 0},
}


def score_technical_v1(price_df, params):
    """技術面評分（提案 v1）。輸入股價 DataFrame，回傳 (得分, 明細 dict)。"""
    p = params
    detail = {}
    df = price_df
    if len(df) < p["MA_LONG"] + 5:
        return 0, {"資料不足": 0}
    close = df["close"]

    ma_long = close.rolling(p["MA_LONG"]).mean()
    detail["站上年線"] = p["PTS"]["ma"] if close.iloc[-1] > ma_long.iloc[-1] else 0

    ma_f = close.rolling(p["CROSS_FAST"]).mean()
    ma_s = close.rolling(p["CROSS_SLOW"]).mean()
    cross = (ma_f.shift(1) <= ma_s.shift(1)) & (ma_f > ma_s)
    detail["黃金交叉"] = p["PTS"]["cross"] if cross.tail(p["EVENT_WINDOW"]).any() else 0

    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    hist = macd - macd.ewm(span=9, adjust=False).mean()
    turn = (hist.shift(1) <= 0) & (hist > 0)
    detail["MACD翻正"] = p["PTS"]["macd"] if turn.tail(p["EVENT_WINDOW"]).any() else 0

    delta = close.diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()
    rsi = 100 - 100 / (1 + gain / loss)
    detail["RSI區間"] = (p["PTS"]["rsi"]
                        if p["RSI_LOW"] <= rsi.iloc[-1] <= p["RSI_HIGH"] else 0)

    up = close.iloc[-1] > close.iloc[-2]
    vol_up = df["volume"].iloc[-1] > df["volume"].iloc[-2]
    detail["量價配合"] = p["PTS"]["vol"] if (up and vol_up) else 0

    return sum(detail.values()), detail


def _q(sql, params_tuple):
    """小工具：查資料庫回傳 DataFrame。"""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql(sql, conn, params=params_tuple)
    conn.close()
    return df


def score_fundamental_v1(stock_id, as_of, params):
    """基本面評分（提案 v1）。所有查詢帶 effective_date <= as_of，不偷看未來。"""
    p = params
    detail = {}

    df = _q("""SELECT dividend_yield FROM valuation
               WHERE stock_id=? AND date<=? ORDER BY date DESC LIMIT 1""",
            (stock_id, as_of))
    y = df["dividend_yield"].iloc[0] if not df.empty else None
    detail["殖利率"] = p["PTS"]["yield"] if (y is not None and y > p["YIELD_MIN"]) else 0

    detail["月營收YoY"] = 0
    df = _q("""SELECT revenue_year, revenue_month, revenue FROM monthly_revenue
               WHERE stock_id=? AND effective_date<=?
               ORDER BY revenue_year DESC, revenue_month DESC LIMIT 1""",
            (stock_id, as_of))
    if not df.empty:
        latest = df.iloc[0]
        prev = _q("""SELECT revenue FROM monthly_revenue
                     WHERE stock_id=? AND revenue_year=? AND revenue_month=?""",
                  (stock_id, int(latest["revenue_year"]) - 1, int(latest["revenue_month"])))
        if not prev.empty and prev["revenue"].iloc[0] > 0:
            yoy = latest["revenue"] / prev["revenue"].iloc[0] - 1
            detail["月營收YoY"] = p["PTS"]["rev"] if yoy > p["REV_YOY_MIN"] else 0

    detail["EPS連兩季升"] = 0
    df = _q("""SELECT value FROM financial_statement
               WHERE stock_id=? AND type='EPS' AND effective_date<=?
               ORDER BY date DESC LIMIT 3""", (stock_id, as_of))
    if len(df) == 3:
        e_new, e_mid, e_old = df["value"].tolist()
        if e_old < e_mid < e_new:
            detail["EPS連兩季升"] = p["PTS"]["eps"]

    detail["ROE"] = 0
    inc = _q("""SELECT value FROM financial_statement
                WHERE stock_id=? AND type='IncomeAfterTaxes' AND effective_date<=?
                ORDER BY date DESC LIMIT 1""", (stock_id, as_of))
    eq = _q("""SELECT value FROM balance_sheet
               WHERE stock_id=? AND type='EquityAttributableToOwnersOfParent'
                 AND effective_date<=? ORDER BY date DESC LIMIT 1""", (stock_id, as_of))
    if not inc.empty and not eq.empty and eq["value"].iloc[0] > 0:
        roe = inc["value"].iloc[0] * 4 / eq["value"].iloc[0]
        detail["ROE"] = p["PTS"]["roe"] if roe > p["ROE_MIN"] else 0

    detail["法人連買"] = 0
    for who in ("Foreign_Investor", "Investment_Trust"):
        df = _q("""SELECT net FROM institutional
                   WHERE stock_id=? AND name=? AND date<=?
                   ORDER BY date DESC LIMIT ?""",
                (stock_id, who, as_of, p["INST_STREAK"]))
        if len(df) == p["INST_STREAK"] and (df["net"] > 0).all():
            detail["法人連買"] = p["PTS"]["inst"]
            break

    return sum(detail.values()), detail


# ---- 策略註冊表：新策略在這裡登記、用 ACTIVE_STRATEGY 切換 ----
STRATEGIES = {
    "proposal_v1": {
        "technical":   score_technical_v1,
        "fundamental": score_fundamental_v1,
        "params":      PROPOSAL_PARAMS,
    },
    # "my_strategy_v2": {...},        ← 未來的新策略登記在這裡
}

ACTIVE_STRATEGY = "proposal_v1"       # ← 切換策略只改這一行

print(f"策略區就緒，目前使用：{ACTIVE_STRATEGY}")

# ========== 5. 選股與儲存 ==========

def load_price(stock_id, as_of, lookback=400):
    """讀基準日（含）以前最近 lookback 個交易日的股價，由舊到新。"""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("""
        SELECT * FROM (
            SELECT date, open, high, low, close, volume FROM daily_price
            WHERE stock_id = ? AND date <= ? ORDER BY date DESC LIMIT ?
        ) ORDER BY date ASC""",
        conn, params=(stock_id, as_of, lookback))
    conn.close()
    return df


def select_stocks(as_of, stock_ids, strategy_name=None):
    """對股票池逐檔評分。回傳依總分排序的 DataFrame。"""
    strat = STRATEGIES[strategy_name or ACTIVE_STRATEGY]
    p = strat["params"]

    results = []
    for sid in stock_ids:
        price_df = load_price(sid, as_of)
        if len(price_df) < 60:
            continue
        t_score, t_detail = strat["technical"](price_df, p)
        f_score, f_detail = strat["fundamental"](sid, as_of, p)
        total = t_score + f_score
        grade = ("強烈推薦" if total >= p["STRONG_SCORE"]
                 else "觀察清單" if total >= p["WATCHLIST_SCORE"] else "-")
        results.append({"股票": sid, "基準日": as_of, "總分": total,
                        "技術面": t_score, "基本面": f_score, "評級": grade,
                        **t_detail, **f_detail})

    return (pd.DataFrame(results)
            .sort_values("總分", ascending=False)
            .reset_index(drop=True))


def save_selection(result_df):
    """寫入 selection_log（主鍵：基準日+股票，重跑覆蓋）。"""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""CREATE TABLE IF NOT EXISTS selection_log (
        as_of TEXT NOT NULL, stock_id TEXT NOT NULL,
        total INTEGER, tech INTEGER, fund INTEGER, grade TEXT,
        PRIMARY KEY (as_of, stock_id))""")
    rows = [(r["基準日"], r["股票"], int(r["總分"]), int(r["技術面"]),
             int(r["基本面"]), r["評級"]) for _, r in result_df.iterrows()]
    conn.executemany("INSERT OR REPLACE INTO selection_log VALUES (?,?,?,?,?,?)", rows)
    conn.commit()
    conn.close()


print("選股函式就緒")

# ========== 6. Google Sheet 與 LINE ==========

def upload_daily_selection(as_of):
    """上傳 as_of 當日達標股票到「每日選股紀錄」（防重複）。"""
    if not ENABLE_GSHEET:
        print("GSheet 停用，跳過")
        return

    # 設定檢查：金鑰路徑必須指到「檔案」本身，不能是資料夾
    # os.path.isfile 只對真正的檔案回 True（資料夾、不存在的路徑都是 False）
    if not os.path.isfile(GSHEET_KEY_PATH):
        hint = ("這個路徑是資料夾——請在後面接上金鑰檔名，例如 "
                r"r\"...\\service_account.json\""
                if os.path.isdir(GSHEET_KEY_PATH)
                else "找不到這個檔案——請確認 config.py 的 GSHEET_KEY_PATH")
        raise FileNotFoundError(f"GSheet 金鑰路徑錯誤：{GSHEET_KEY_PATH}\n{hint}")

    import gspread                    # 放函式內：停用時不需要裝這個套件
    gc = gspread.service_account(filename=GSHEET_KEY_PATH)
    sh = gc.open(GSHEET_NAME)

    def get_ws(title, header):
        try:
            return sh.worksheet(title)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=title, rows=1000, cols=len(header) + 2)
            ws.append_rows([header])
            return ws

    def _update_performance_summary(track_rows):
        """2026-10 report 改版新增：「績效總表」——只看「已結算」的倉位，
        依評級分組、抓最近 20 筆（滾動視窗）的表現，讓人一眼看出整體/各評級/
        最近一段時間的勝率跟平均報酬率，不用自己在「推薦追蹤」裡逐筆算。
        欄位對應 track_header：0股票代號 1狀態 2進場日 3進場價 4出場日 5出場價
        6報酬率% 7大盤同期% 8超額% 9持有天數 10首次評級 11異常標記"""
        settled = [r for r in track_rows if r[1] == "已結算"]

        def _stats(sub):
            n = len(sub)
            if n == 0:
                return [0, "", "", ""]
            rets = [r[6] for r in sub]
            win = sum(1 for x in rets if x > 0)
            return [n, round(win / n * 100, 1), round(sum(rets) / n, 2),
                    round((sorted(rets)[n // 2] if n % 2 else
                           (sorted(rets)[n // 2 - 1] + sorted(rets)[n // 2]) / 2), 2)]

        summary_header = ["分組", "筆數", "勝率%", "平均報酬%", "中位數報酬%"]
        summary_rows = [["整體（全部已結算）"] + _stats(settled)]
        for g in ["強烈推薦", "觀察清單"]:
            summary_rows.append([g] + _stats([r for r in settled if r[10] == g]))
        # 滾動視窗：按出場日排序取最近 N 筆，跟整體比較，看近期是不是變好/變差
        for n_recent in (20, 50):
            recent = sorted(settled, key=lambda r: r[4], reverse=True)[:n_recent]
            summary_rows.append([f"最近{n_recent}筆已結算"] + _stats(recent))
        n_anomaly = sum(1 for r in settled if r[11])
        summary_rows.append(["", "", "", "", ""])
        summary_rows.append([f"異常標記（|報酬率|>{40}%，建議人工核對）", n_anomaly, "", "", ""])

        summary_ws = get_ws("績效總表", summary_header)
        summary_ws.clear()
        summary_ws.update([summary_header] + summary_rows, value_input_option="USER_ENTERED")
        print(f"績效總表更新完成（已結算 {len(settled)} 筆）")

    daily_ws = get_ws("每日選股紀錄",
                      ["日期", "股票代號", "總分", "技術面", "基本面", "評級"])
    df = _q("""SELECT as_of, stock_id, total, tech, fund, grade FROM selection_log
               WHERE as_of = ? AND grade != '-' ORDER BY total DESC""", (as_of,))
    if df.empty:
        print("無達標股票，Sheet 不新增")
    else:
        values = daily_ws.get_all_values()
        seen = set((r[0], r[1]) for r in values[1:]) if len(values) > 1 else set()
        rows = [[r["as_of"], r["stock_id"], int(r["total"]), int(r["tech"]),
                 int(r["fund"]), r["grade"]]
                for _, r in df.iterrows() if (r["as_of"], r["stock_id"]) not in seen]
        if rows:
            daily_ws.append_rows(rows, value_input_option="USER_ENTERED")
        print(f"每日選股紀錄新增 {len(rows)} 列")

    # ---- 推薦追蹤（整表重算；2026-10 起改為「固定 20 個交易日結算」+ 大盤對照/異常標記）----
    # 設計：每次都從 selection_log 全歷史重新算起（不依賴 Sheet 上次留下的值），
    # 保證可重跑、不會因為中途改規則或補資料而跟舊邏輯的殘留值混在一起。
    #
    # 規則：一檔股票被評為達標（觀察清單/強烈推薦）那天開一個新倉位，
    # 持有到「滿 SETTLE_AFTER_DAYS 個交易日」那天結算（出場價＝那天收盤）；
    # 結算前都算「追蹤中」（用目前最新收盤算未實現報酬率）。
    # 結算後，若之後又被評為達標才會開下一輪新倉位——不會自動無限期延續，
    # 這樣同一檔股票可能在歷史上出現多輪獨立的 20 日交易紀錄。
    #
    # 2026-10 report 改版新增：
    #   大盤同期報酬%/超額報酬% —— 用 benchmark_index(TAIEX) 算同一段時間大盤漲跌，
    #     這樣才知道是真的選股有效，還是單純大盤那段時間在漲。benchmark_index
    #     目前還是空的（見 update_benchmark_index() 的說明，待你本機驗證過
    #     FinMind 抓取成功後才會有資料），沒資料時這兩欄留空，不會讓程式出錯。
    #   異常標記 —— |報酬率|>40% 的列印出來提醒人工核對，這類極端值常常是減資/
    #     分割等公司事件造成的價格不連續，不是真的交易績效（沿用之前分析
    #     Google Sheet 歷史資料時，3443/6669 兩檔抓到的同一個模式）。
    track_header = ["股票代號", "狀態", "進場日", "進場價", "出場日/最新日",
                    "出場價/最新價", "報酬率%", "大盤同期報酬%", "超額報酬%",
                    "持有交易日數", "首次評級", "異常標記"]
    track_ws = get_ws("推薦追蹤", track_header)

    conn = sqlite3.connect(DB_PATH)

    def close_on_or_before(sid, d):
        row = conn.execute("""SELECT date, close FROM daily_price
                              WHERE stock_id=? AND date<=? ORDER BY date DESC LIMIT 1""",
                           (sid, d)).fetchone()
        return row if row else (None, None)

    def benchmark_close_on_or_before(d):
        row = conn.execute("""SELECT close FROM benchmark_index
                              WHERE index_id='TAIEX' AND date<=? ORDER BY date DESC LIMIT 1""",
                           (d,)).fetchone()
        return row[0] if row else None

    ANOMALY_THRESHOLD = 40  # |報酬率|超過這個百分比就標記，門檻沿用之前分析歷史資料時的經驗值

    all_dates = pd.read_sql("SELECT DISTINCT date FROM daily_price ORDER BY date", conn)["date"].tolist()
    date_pos = {d: i for i, d in enumerate(all_dates)}
    glog = pd.read_sql("""SELECT as_of, stock_id, grade FROM selection_log
                          WHERE grade != '-' ORDER BY stock_id, as_of""", conn)

    rows, n_settled, n_open, n_anomaly = [], 0, 0, 0
    for sid, sub in glog.groupby("stock_id"):
        open_until = None   # None=沒有持倉；"9999-12-31"=持倉中但結算日還不確定；其他=已排定的結算日
        for entry_date, grade in sub[["as_of", "grade"]].itertuples(index=False):
            if open_until is not None and entry_date <= open_until:
                continue  # 還在同一輪持倉期間內，當天的推薦不開新倉

            if entry_date not in date_pos:
                continue
            idx_entry = date_pos[entry_date]
            target_idx = idx_entry + SETTLE_AFTER_DAYS

            _, entry_price = close_on_or_before(sid, entry_date)
            if entry_price is None:
                continue

            if target_idx < len(all_dates):
                settle_date = all_dates[target_idx]
                exit_date, exit_price = close_on_or_before(sid, settle_date)
                if exit_price is None:
                    open_until = "9999-12-31"   # 股票停牌等缺資料，先當成持倉中，下次再試
                    continue
                status = "已結算"
                disp_date, disp_price = exit_date, exit_price
                ret = round((exit_price / entry_price - 1) * 100, 2)
                held = SETTLE_AFTER_DAYS
                open_until = settle_date
                n_settled += 1
            else:
                status = "追蹤中"
                latest_date, latest_price = close_on_or_before(sid, "9999-12-31")
                disp_date, disp_price = latest_date, latest_price
                ret = round((latest_price / entry_price - 1) * 100, 2)
                held = date_pos.get(latest_date, idx_entry) - idx_entry
                open_until = "9999-12-31"
                n_open += 1

            bm_entry = benchmark_close_on_or_before(entry_date)
            bm_disp = benchmark_close_on_or_before(disp_date)
            if bm_entry and bm_disp:
                bm_ret = round((bm_disp / bm_entry - 1) * 100, 2)
                excess_ret = round(ret - bm_ret, 2)
            else:
                bm_ret, excess_ret = "", ""  # benchmark_index 還沒有資料時留空，不報錯

            anomaly = ""
            if abs(ret) > ANOMALY_THRESHOLD:
                anomaly = "⚠請核對(可能減資/分割)"
                n_anomaly += 1

            rows.append([sid, status, entry_date, entry_price, disp_date, disp_price,
                        ret, bm_ret, excess_ret, held, grade, anomaly])
    conn.close()

    # 排序：追蹤中（目前還看得到、可能還會變動）排前面，已結算（歷史定案）排後面；
    # 組內都按報酬率高到低。
    status_order = {"追蹤中": 0, "已結算": 1}
    rows.sort(key=lambda r: (status_order[r[1]], -r[6]))

    track_ws.clear()
    track_ws.update([track_header] + rows, value_input_option="USER_ENTERED")

    # 2026-10-28 修復：track_ws.clear() 只清內容、不清格式。這個 Sheet 歷史上
    # 不同版本的欄位配置用過好幾輪，如果同一個儲存格座標曾經被舊版寫過「日期」
    # 類型的值，Google Sheets 會把那個儲存格的格式記成日期，之後哪怕 USER_ENTERED
    # 寫進去的是股價這種單純數字，也會被「借用」殘留的日期格式顯示成一串奇怪的
    # 日期字串（底層數值其實是對的，只是顯示錯了——例如股價 545 被顯示成
    # 1901-06-28；股價若非整數，因為日期格式只看整數天，小數部分會被吃掉，
    # 所以報酬率%算出來會跟顯示的價格對不太起來，不是算錯，是顯示錯）。
    # 這裡每次更新後都強制把會寫數字的欄位重設回純數字格式，避免這個問題持續發生。
    numeric_cols = ["D", "F", "G", "H", "I", "J"]  # 進場價/出場價/報酬率%/大盤同期%/超額%/持有天數
    if rows:
        n_last_row = len(rows) + 1  # +1 因為第1列是表頭
        try:
            track_ws.batch_format([
                {"range": f"{col}2:{col}{n_last_row}",
                 "format": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"}}}
                for col in numeric_cols
            ])
        except Exception as e:
            print(f"⚠ 推薦追蹤欄位格式重設失敗（不影響資料本身，只是顯示可能還是怪怪的）：{e}")

    print(f"推薦追蹤更新：追蹤中 {n_open} 筆、已結算 {n_settled} 筆、異常標記 {n_anomaly} 筆")

    _update_performance_summary(rows)


def push_line_message(text):
    """LINE 推播（push，只送給單一 LINE_USER_ID）。停用或未設定時安靜跳過。
    只用在給管理者自己看的內部通知（例如管線執行異常），不要拿來發日常報告——
    家人朋友加這個官方帳號好友也收不到，因為 push 是點對點的，只認 LINE_USER_ID
    這一個人。要讓所有已加好友的人都收到，請改用下面的 broadcast_line_message。
    """
    if not ENABLE_LINE:
        print("LINE 停用，跳過推播")
        return True
    resp = requests.post(
        "https://api.line.me/v2/bot/message/push",
        headers={"Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
                 "Content-Type": "application/json"},
        json={"to": LINE_USER_ID, "messages": [{"type": "text", "text": text}]},
        timeout=15)
    if resp.status_code == 200:
        print("推播成功")
        return True
    print(f"推播失敗 HTTP {resp.status_code}：{resp.text}")
    return False


def broadcast_line_message(text):
    """LINE 廣播（broadcast，發給所有已加這個官方帳號好友的人，包含家人）。
    停用或未設定時安靜跳過。每日選股報告改用這支函式發送，這樣家人加好友後
    才會真的收得到；管線異常通知則維持用 push_line_message 只發給你自己。
    """
    if not ENABLE_LINE:
        print("LINE 停用，跳過推播")
        return True
    resp = requests.post(
        "https://api.line.me/v2/bot/message/broadcast",
        headers={"Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
                 "Content-Type": "application/json"},
        json={"messages": [{"type": "text", "text": text}]},
        timeout=15)
    if resp.status_code == 200:
        print("廣播成功")
        return True
    print(f"廣播失敗 HTTP {resp.status_code}：{resp.text}")
    return False


def notify_error(stage, err):
    """管線失敗時的自我回報（推播失敗也不讓程式炸掉）。"""
    try:
        push_line_message(f"⚠️ 選股系統執行異常\n站點：{stage}\n錯誤：{str(err)[:200]}")
    except Exception:
        pass


def compose_daily_message(as_of):
    """組合當日訊息（含股票名稱）。無達標回傳 (None, 0)。"""
    df = _q("""SELECT stock_id, total, grade FROM selection_log
               WHERE as_of = ? AND grade != '-' ORDER BY total DESC""", (as_of,))
    if df.empty:
        return None, 0
    names = get_name_map()
    lines = [f"📈 每日選股報告（{as_of}）"]
    for label, emoji in [("強烈推薦", "🔥"), ("觀察清單", "👀")]:
        part = df[df["grade"] == label]
        if part.empty:
            continue
        lines.append("")
        lines.append(f"{emoji} {label}")
        for _, r in part.iterrows():
            name = names.get(r["stock_id"], "")
            lines.append(f"　{r['stock_id']} {name}　{r['total']} 分")
    lines += ["", "⚠️ 系統訊號僅供研究參考，非投資建議"]
    if SHEET_URL:
        lines.append(f"📊 完整追蹤：{SHEET_URL}")
    return "\n".join(lines), len(df)


print("上傳與推播函式就緒")

# ========== 7. 管線總入口 ==========

def run_pipeline():
    """每日管線總入口。"""
    print("=" * 50)
    print("管線啟動", now_taipei().strftime("%Y-%m-%d %H:%M:%S"))
    print("=" * 50)

    ensure_extra_tables()          # 自我修復：資料庫若被重建/還原，自動補齊輔助表

    stock_ids = get_pool_ids()
    if not stock_ids:
        print("⚠ 資料庫沒有股票池——請先用 builder notebook 完成建置")
        return

    # ---- ① 資料更新 ----
    try:
        as_of = update_market_data(stock_ids)
        log_stage(as_of, "update", "ok")
    except Exception as e:
        log_stage("?", "update", "fail", e)
        notify_error("資料更新", e)
        print(f"✗ 資料更新失敗，中止：{e}")
        return

    # ---- ② 基準日 = 資料庫最新交易日（日期防呆在選股端的落實） ----
    print(f"評分基準日（資料庫最新交易日）：{as_of}")

    # ---- ③ 防重複推播 ----
    if stage_done(as_of, "push"):
        print(f"{as_of} 的訊號已推播過，本次結束（同日/假日重跑防呆）")
        return

    # ---- ④ 評分選股 ----
    try:
        ensure_stock_names(stock_ids)
        result = select_stocks(as_of, stock_ids)
        save_selection(result)
        n_hit = int((result["評級"] != "-").sum())
        log_stage(as_of, "select", "ok", f"{n_hit} 檔達標")
        print(f"評分完成：{len(result)} 檔，達標 {n_hit} 檔")

        # ---- 把選股結果直接印在 notebook 裡 ----
        names = get_name_map()
        show = result[result["評級"] != "-"].copy()      # 只列達標的
        if show.empty:
            print("（今日無達標股票）")
        else:
            # insert(位置, 欄名, 值)：在第 2 欄插入名稱欄
            show.insert(1, "名稱", show["股票"].map(names).fillna(""))
            print()
            print("─" * 46)
            print("今日選股結果")
            print("─" * 46)
            # to_string：完整印出表格；index=False 不顯示列號
            print(show[["股票", "名稱", "總分", "技術面", "基本面", "評級"]]
                  .to_string(index=False))
            print("─" * 46)
    except Exception as e:
        log_stage(as_of, "select", "fail", e)
        notify_error("評分選股", e)
        print(f"✗ 選股失敗，中止：{e}")
        return

    # ---- ⑤ Google Sheet（非關鍵站：失敗回報但繼續） ----
    try:
        upload_daily_selection(as_of)
        log_stage(as_of, "gsheet", "ok")
    except Exception as e:
        log_stage(as_of, "gsheet", "fail", e)
        notify_error("Google Sheet 上傳", e)
        print(f"⚠ Sheet 上傳失敗（繼續推播）：{e}")

    # ---- ⑥ LINE 推播 ----
    msg, n = compose_daily_message(as_of)
    if msg is None:
        log_stage(as_of, "push", "ok", "無達標不推播")
        print("今日無達標股票，依規格不推播")
    elif broadcast_line_message(msg):
        log_stage(as_of, "push", "ok", f"{n} 檔")
    else:
        log_stage(as_of, "push", "fail")

    print("=" * 50)
    print("管線結束")


# ================= 進入點 =================
# 「python daily_run.py」直接執行時才跑管線；被 import（例如未來回測引擎
# 借用這裡的函式）時不會自動執行——這是 Python 腳本的標準寫法。
if __name__ == "__main__":
    run_pipeline()
