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

# ログを即時表示
sys.stdout.reconfigure(line_buffering=True)

# ==========================================
# ⚙️ 設定（爆速並列モード）
# ==========================================
MAX_RETRIES = 3
RETRY_INTERVAL = 3
MAX_WORKERS = 8 # 1つの会場で同時に処理するスレッド数

def get_session():
    session = requests.Session()
    # 並列処理用に接続プールを増やす
    adapter = HTTPAdapter(
        pool_connections=MAX_WORKERS,
        pool_maxsize=MAX_WORKERS,
        max_retries=Retry(total=MAX_RETRIES, backoff_factor=1)
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    }
    session.headers.update(headers)
    return session

def get_soup_with_retry(session, url):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            res = session.get(url, timeout=20) # タイムアウトは少し短めに
            res.encoding = res.apparent_encoding
            if res.status_code == 200:
                return BeautifulSoup(res.text, 'html.parser')
        except:
            pass
        time.sleep(RETRY_INTERVAL)
    return None

def clean_text(text):
    if not text: return ""
    return text.replace("\n", "").replace("\r", "").replace(" ", "").replace("\u3000", "").strip()

def scrape_race_data(session, jcd, rno, date_str):
    """修正済みの最強パースロジック"""
    base_url = "https://www.boatrace.jp/owpc/pc/race"
    
    # 3ページ取得
    soup_list = get_soup_with_retry(session, f"{base_url}/racelist?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    soup_before = get_soup_with_retry(session, f"{base_url}/beforeinfo?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    soup_res = get_soup_with_retry(session, f"{base_url}/raceresult?rno={rno}&jcd={jcd:02d}&hd={date_str}")

    if not all([soup_list, soup_before, soup_res]):
        return None

    try:
        # --- 1. 風速取得 ---
        wind = 0.0
        try:
            wind_elem = soup_before.find(string=re.compile("風速"))
            if wind_elem:
                parent = wind_elem.find_parent(class_="weather1_bodyUnit")
                if parent:
                    data_elem = parent.select_one(".weather1_bodyUnitLabelData")
                    if data_elem:
                        w_text = clean_text(data_elem.text).replace("m", "")
                        wind = float(w_text)
        except:
            pass 

        # --- 2. 正解ラベル (1着) ---
        res1 = 0
        try:
            res_rows = soup_res.select(".is-p_1-1")
            if res_rows:
                rank1_boat = clean_text(res_rows[0].select("td")[1].text)
                if rank1_boat == "1":
                    res1 = 1
        except:
            pass

        # --- 3. 展示タイム & 各艇データ ---
        temp_ex_times = []
        for i in range(1, 7):
            # 艇番の色クラスから探す確実な方法
            boat_cell = soup_before.select_one(f".is-boatColor{i}")
            if not boat_cell: return None
            
            tbody = boat_cell.find_parent("tbody")
            tds = tbody.select("td")
            
            ex_val = clean_text(tds[4].text)
            if not ex_val: ex_val = clean_text(tds[5].text)
            
            if not ex_val or ex_val == "-" or ex_val == "0.00":
                return None
            temp_ex_times.append(float(ex_val))

        # --- 4. 出走表データ ---
        row = {'date': date_str, 'jcd': jcd, 'rno': rno, 'wind': wind, 'res1': res1}
        for i in range(1, 7):
            boat_cell_list = soup_list.select_one(f".is-boatColor{i}")
            if not boat_cell_list: return None
            
            tbody_list = boat_cell_list.find_parent("tbody")
            tds_list = tbody_list.select("td")
            
            try:
                row[f'wr{i}'] = float(re.findall(r"\d+\.\d+", tds_list[3].text)[0])
                nums = re.findall(r"\d+\.\d+", tds_list[6].text)
                row[f'mo{i}'] = float(nums[0]) if len(nums) >= 1 else 0.0
            except:
                row[f'wr{i}'] = 0.0
                row[f'mo{i}'] = 0.0

            row[f'ex{i}'] = temp_ex_times[i-1]

        return row

    except:
        return None

def process_race_parallel(args):
    """並列処理用のラッパー関数"""
    session, jcd, rno, date_str = args
    return scrape_race_data(session, jcd, rno, date_str)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    args = parser.parse_args()

    os.makedirs("data", exist_ok=True)
    
    # セッション作成（プール数増強済み）
    session = get_session()
    try:
        session.get("https://www.boatrace.jp/", timeout=10)
    except:
        pass

    start_d = datetime.strptime(args.start, "%Y-%m-%d")
    end_d = datetime.strptime(args.end, "%Y-%m-%d")
    current = start_d
    
    print(f"🚀 並列データ収集開始: {args.start} 〜 {args.end}")
    
    results = []
    
    while current <= end_d:
        d_str = current.strftime("%Y%m%d")
        print(f"📅 {d_str} 並列スキャン中...", end="", flush=True)
        
        # 1日分の全レース（24会場×12レース）のタスクを作成
        tasks = []
        for jcd in range(1, 25):
            for rno in range(1, 13):
                tasks.append((session, jcd, rno, d_str))
        
        # マルチスレッド実行！
        day_results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = executor.map(process_race_parallel, tasks)
            for res in futures:
                if res:
                    day_results.append(res)
        
        print(f" -> {len(day_results)}レース取得")
        results.extend(day_results)
        
        current += timedelta(days=1)

    if results:
        df = pd.DataFrame(results)
        filename = f"data/pure_data_{args.start}_{args.end}.csv"
        df.to_csv(filename, index=False)
        print(f"🎉 全工程完了！CSV保存: {filename} ({len(df)}行)")
    else:
        print("⚠️ データが取得できませんでした。")
