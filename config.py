# -*- coding: utf-8 -*-
"""
config.py — 設定載入器（環境變數優先）

這個檔案「可以」放上 GitHub：裡面沒有任何秘密，只有讀取邏輯與無害的預設值。
秘密的實際值來自兩個地方，依環境自動切換：

  雲端（GitHub Actions）：repo 的 Settings → Secrets 設定的加密變數，
      由 workflow 以「環境變數」注入 → 下面的 os.environ.get 讀到它們
  本機（你的電腦）    ：同資料夾放一個 config_local.py（已被 .gitignore 排除，
      永遠不會上傳），在裡面直接指定值，會覆蓋這裡的預設

config_local.py 的寫法（把你原本 config_template 裡填好的值搬過來即可）：

    FINMIND_TOKEN = "你的token"
    LINE_CHANNEL_ACCESS_TOKEN = "你的token"
    LINE_USER_ID = "U開頭的ID"
    GSHEET_KEY_PATH = r"C:\\...\\service_account.json"
    SHEET_URL = "https://docs.google.com/spreadsheets/d/..."
    # 其餘（DB_PATH 等）沿用預設就不用寫
"""
import os

# os.environ 是「環境變數」字典——作業系統層級的變數，GitHub Actions 的
# Secrets 就是透過它交給程式。.get(名稱, 預設值)：讀不到就用預設值。

DB_PATH = os.environ.get("DB_PATH", "stock_data.db")

FINMIND_TOKEN = os.environ.get("FINMIND_TOKEN", "")

# ---- LINE ----
# 環境變數一律是字串，所以布林開關用「字串比對」轉換：
# "true"（不分大小寫）→ True，其他 → False
ENABLE_LINE = os.environ.get("ENABLE_LINE", "true").lower() == "true"
LINE_CHANNEL_ACCESS_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "")
LINE_USER_ID = os.environ.get("LINE_USER_ID", "")

# ---- Google Sheet ----
ENABLE_GSHEET = os.environ.get("ENABLE_GSHEET", "true").lower() == "true"
GSHEET_KEY_PATH = os.environ.get("GSHEET_KEY_PATH", "service_account.json")
GSHEET_NAME = os.environ.get("GSHEET_NAME", "選股系統追蹤")
SHEET_URL = os.environ.get("SHEET_URL", "")

# ---- 本機覆蓋：config_local.py 存在時，用它的值蓋掉上面的 ----
# try/except ImportError：檔案不存在（例如在雲端）就安靜跳過，不是錯誤
try:
    from config_local import *          # noqa: F401,F403
except ImportError:
    pass
