import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import sys
import argparse
import os
import re
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

# ログを即時出力（GitHub Actionsで見やすくするため）
sys.stdout.reconfigure(line_buffering=True)

# ==========================================
# 🔧 設定エリア（安全運転モード）
# ==========================================
MAX_RETRIES = 5
RETRY_INTERVAL = 5 # リトライ時の待機時間を長めに
MAX_WORKERS = 2    # 並列数を4→2に減らしてBAN回避

def get_session():
    session = requests.Session()
    # リトライ戦略の設定
    retries = Retry(
        total=MAX_RETRIES,
        backoff_factor=2,  # 待機時間を指数関数的に増やす (2s, 4s, 8s...)
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS, max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    })
    return session

def get_soup(session, url):
    """HTMLを取得してBeautifulSoupオブジェクトを返す（エラーハンドリング付き）"""
    try:
        # タイムアウトを少し長めに設定
        res = session.get(url, timeout=30)
        res.encoding = res.apparent_encoding
        if res.status_code == 200:
            return BeautifulSoup(res.text, 'html.parser')
    except Exception:
        pass
    return None

def clean_text(text):
    if not text: return ""
    return text.replace("\n", "").replace("\r", "").replace(" ", "").replace("\u3000", "").strip()

def scrape_race_data(session, jcd, rno, date_str):
    base_url = "https://www.boatrace.jp/owpc/pc/race"
    
    # 3つのページを取得
    soup_list = get_soup(session, f"{base_url}/racelist?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    soup_before = get_soup(session, f"{base_url}/beforeinfo?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    soup_res = get_soup(session, f"{base_url}/raceresult?rno={rno}&jcd={jcd:02d}&hd={date_str}")

    # 必須ページが取れない場合はスキップ
    if not all([soup_list, soup_before, soup_res]):
        return None

    try:
        # --- 基本情報（風速） ---
        wind = 0.0
        try:
            wind_elem = soup_before.find(string=re.compile("風速"))
            if wind_elem:
                data_elem = wind_elem.find_parent(class_="weather1_bodyUnit").select_one(".weather1_bodyUnitLabelData")
                if data_elem:
                    wind_text = clean_text(data_elem.text).replace("m", "")
                    wind = float(wind_text)
        except: pass 

        # --- 結果（1着フラグ） ---
        res1 = 0
        try:
            res_rows = soup_res.select(".is-p_1-1")
            if res_rows:
                rank_text = clean_text(res_rows[0].select("td")[1].text)
                if rank_text in ["1", "１"]:
                    res1 = 1
        except: pass

        # --- 直前情報（展示タイム） ---
        ex_times = {}
        for i in range(1, 7):
            ex_val = 0.0
            try:
                boat_cell = soup_before.select_one(f".is-boatColor{i}")
                if boat_cell:
                    tds = boat_cell.find_parent("tbody").select("td")
                    val_str = clean_text(tds[4].text)
                    if not val_str:
                        val_str = clean_text(tds[5].text)
                    
                    if val_str and val_str not in ["-", "0.00", "\xa0"]:
                        ex_val = float(val_str)
            except: pass
            ex_times[i] = ex_val

        # --- 出走表（勝率・モーター） ---
        row = {'date': date_str, 'jcd': jcd, 'rno': rno, 'wind': wind, 'res1': res1}
        
        for i in range(1, 7):
            wr_val = 0.0
            mo_val = 0.0
            
            try:
                boat_cell = soup_list.select_one(f".is-boatColor{i}")
                if boat_cell:
                    tds = boat_cell.find_parent("tbody").select("td")
                    
                    # 全国勝率
                    wr_text = tds[3].text
                    wr_match = re.findall(r"\d+\.\d+", wr_text)
                    if wr_match:
                        wr_val = float(wr_match[0])
                        
                    # モーター勝率
                    mo_text = tds[6].text
                    mo_match = re.findall(r"\d+\.\d+", mo_text)
                    if mo_match:
                        mo_val = float(mo_match[0])
            except: pass

            row[f'wr{i}'] = wr_val
            row[f'mo{i}'] = mo_val
            row[f'ex{i}'] = ex_times[i]

        return row

    except Exception as e:
        print(f"Error parsing {date_str} J{jcd} R{rno}: {e}")
        return None

def process_race_parallel(args):
    # 【重要】アクセス間隔をしっかり空ける（BAN対策）
    time.sleep(1.5) 
    return scrape_race_data(*args)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    args = parser.parse_args()
    
    os.makedirs("data", exist_ok=True)
    session = get_session()
    
    start_d = datetime.strptime(args.start, "%Y-%m-%d")
    end_d = datetime.strptime(args.end, "%Y-%m-%d")
    
    current = start_d
    all_results = []
    
    print(f"Starting scrape from {args.start} to {args.end}")
    
    while current <= end_d:
        d_str = current.strftime("%Y%m%d")
        print(f"Processing {d_str}...")
        
        tasks = []
        for jcd in range(1, 25):
            for rno in range(1, 13):
                tasks.append((session, jcd, rno, d_str))
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for res in executor.map(process_race_parallel, tasks):
                if res:
                    all_results.append(res)
                    
        current += timedelta(days=1)

    if all_results:
        filename = f"data/chunk_{args.start}.csv"
        df = pd.DataFrame(all_results)
        df.to_csv(filename, index=False)
        print(f"Saved {len(df)} rows to {filename}")
    else:
        print("No data found for this period.")
