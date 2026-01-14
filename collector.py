import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import sys
import argparse
import os
import re
import threading
import unicodedata
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

# ログ設定
sys.stdout.reconfigure(line_buffering=True)
print_lock = threading.Lock()

def safe_print(msg):
    with print_lock:
        print(msg)

# ==========================================
# ⚙️ 設定エリア
# ==========================================
MAX_RETRIES = 3       # 無駄なリトライを減らす
RETRY_INTERVAL = 3    
BAN_WAIT_TIME = 10
MAX_WORKERS = 16      # マトリックス分割しているので16で攻めてOK

def get_session():
    session = requests.Session()
    adapter = HTTPAdapter(
        pool_connections=MAX_WORKERS,
        pool_maxsize=MAX_WORKERS,
        max_retries=Retry(total=MAX_RETRIES, backoff_factor=1)
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    })
    return session

def clean_text(text):
    """全角数字を半角に正規化（勝敗判定に必須）"""
    if not text: return ""
    text = unicodedata.normalize('NFKC', text)
    return text.replace("\n", "").replace("\r", "").replace(" ", "").strip()

def get_soup_diagnostic(session, url, check_selector=None):
    """HTML取得＆診断（開催なし判定付き）"""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            res = session.get(url, timeout=20)
            res.encoding = res.apparent_encoding
            
            if res.status_code == 200:
                # 開催なし判定（これをしないとリトライ地獄になる）
                if "データがありません" in res.text:
                    return None, "SKIP"

                soup = BeautifulSoup(res.text, 'html.parser')
                if check_selector and not soup.select_one(check_selector):
                    # 中身が空（エラーページ等）の場合はリトライ
                    time.sleep(RETRY_INTERVAL)
                    continue 
                return soup, None
        except:
            time.sleep(RETRY_INTERVAL)
    return None, "ERROR"

def scrape_race_data(session, jcd, rno, date_str):
    base_url = "https://www.boatrace.jp/owpc/pc/race"
    
    # 1. 直前情報（まずこれで開催有無をチェック）
    soup_before, err = get_soup_diagnostic(
        session, 
        f"{base_url}/beforeinfo?rno={rno}&jcd={jcd:02d}&hd={date_str}",
        check_selector=".is-boatColor1"
    )
    if err == "SKIP" or not soup_before:
        return None

    # 2. 結果（勝敗判定用）
    soup_res, err = get_soup_diagnostic(session, f"{base_url}/raceresult?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    if not soup_res: return None

    # 3. 番組表（勝率等）
    soup_list, err = get_soup_diagnostic(session, f"{base_url}/racelist?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    if not soup_list: return None

    try:
        # --- データ抽出 ---
        wind = 0.0
        try:
            wind_elem = soup_before.find(string=re.compile("風速"))
            if wind_elem:
                data_elem = wind_elem.find_parent(class_="weather1_bodyUnit").select_one(".weather1_bodyUnitLabelData")
                if data_elem: 
                    wind = float(clean_text(data_elem.text).replace("m", ""))
        except: pass 

        res1 = 0
        try:
            res_rows = soup_res.select(".is-p_1-1")
            if res_rows:
                # clean_textで正規化しているので "1" で判定可能
                rank1_boat = clean_text(res_rows[0].select("td")[1].text)
                if rank1_boat == "1":
                    res1 = 1
        except: pass

        temp_ex_times = []
        for i in range(1, 7):
            boat_cell = soup_before.select_one(f".is-boatColor{i}")
            if not boat_cell: return None
            tds = boat_cell.find_parent("tbody").select("td")
            ex_val = clean_text(tds[4].text) or clean_text(tds[5].text)
            
            val = 0.0
            if ex_val and ex_val not in ["-", "0.00"]:
                try: val = float(ex_val)
                except: pass
            temp_ex_times.append(val)

        row = {'date': date_str, 'jcd': jcd, 'rno': rno, 'wind': wind, 'res1': res1}
        
        for i in range(1, 7):
            try:
                tds = soup_list.select_one(f".is-boatColor{i}").find_parent("tbody").select("td")
                row[f'wr{i}'] = float(re.findall(r"\d+\.\d+", clean_text(tds[3].text))[0])
                nums = re.findall(r"\d+\.\d+", clean_text(tds[6].text))
                row[f'mo{i}'] = float(nums[0]) if nums else 0.0
            except:
                row[f'wr{i}'], row[f'mo{i}'] = 0.0, 0.0
            row[f'ex{i}'] = temp_ex_times[i-1]

        return row

    except:
        return None

def process_race_parallel(args):
    # 並列数が多いので少し待機を入れる
    time.sleep(0.5)
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
    
    print(f"🚀 開始: {args.start} 〜 {args.end}")
    
    # 逐次保存用ファイル名
    filename = f"data/chunk_{args.start}.csv"
    file_exists = False
    
    while current <= end_d:
        d_str = current.strftime("%Y%m%d")
        print(f"📅 {d_str}...")
        
        tasks = [(session, jcd, rno, d_str) for jcd in range(1, 25) for rno in range(1, 13)]
        
        day_results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for res in executor.map(process_race_parallel, tasks):
                if res: day_results.append(res)
        
        if day_results:
            df = pd.DataFrame(day_results)
            # 1日終わるごとに追記保存（タイムアウト対策の念のため）
            df.to_csv(filename, mode='a', index=False, header=not file_exists)
            file_exists = True
            print(f"  ✅ {len(day_results)}レース保存")
        
        current += timedelta(days=1)

    print("🎉 完了")
