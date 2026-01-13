import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime, timedelta
import argparse
import os
from concurrent.futures import ThreadPoolExecutor

# ==========================================
# ⚙️ 設定
# ==========================================
MAX_RETRIES = 3
RETRY_INTERVAL = 1
MAX_WORKERS = 5 # 同時接続数（5件程度がサイトに優しく、かつ速い）

def get_soup_with_retry(url):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            headers = {'User-Agent': 'Mozilla/5.0'}
            res = requests.get(url, headers=headers, timeout=10)
            res.encoding = res.apparent_encoding
            if res.status_code == 200:
                return BeautifulSoup(res.text, 'html.parser')
        except:
            pass
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_INTERVAL)
    return None

def scrape_race_data(jcd, rno, date_str):
    """1レース分の生データを取得（ダミー排除）"""
    base_url = "https://www.boatrace.jp/owpc/pc/race"
    soup_list = get_soup_with_retry(f"{base_url}/racelist?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    soup_before = get_soup_with_retry(f"{base_url}/beforeinfo?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    soup_res = get_soup_with_retry(f"{base_url}/raceresult?rno={rno}&jcd={jcd:02d}&hd={date_str}")

    if not all([soup_list, soup_before, soup_res]):
        return None

    try:
        w_text = soup_before.select_one('.weather1_bodyUnitLabelData').text.replace('m','').strip()
        wind = float(w_text) if w_text else 0.0
        res1_text = soup_res.select_one('.is-p_0-1 .is-p_1-1') 
        res1 = 1 if (res1_text and res1_text.text.strip() == "1") else 0

        temp_ex_times = []
        for i in range(1, 7):
            ex_val = soup_before.select(f'tbody.is-p_0-{i}')[0].select('td')[4].text.strip()
            if not ex_val or ex_val == "-" or float(ex_val) <= 0:
                return None
            temp_ex_times.append(float(ex_val))

        row = {'date': date_str, 'jcd': jcd, 'rno': rno, 'wind': wind, 'res1': res1}
        for i in range(1, 7):
            tbody = soup_list.select(f'tbody.is-p_0-{i}')[0].select('td')
            row[f'wr{i}'] = float(tbody[3].select_one('div').text.split()[0])
            row[f'mo{i}'] = float(tbody[6].select_one('div').text.split()[0])
            row[f'ex{i}'] = temp_ex_times[i-1]
        return row
    except:
        return None

def process_stadium(args_tuple):
    """会場単位の処理（並列化用）"""
    jcd, date_str = args_tuple
    stadium_results = []
    print(f"🏟️ 会場コード {jcd:02d} スキャン開始...")
    for rno in range(1, 13):
        data = scrape_race_data(jcd, rno, date_str)
        if data:
            stadium_results.append(data)
    print(f"✅ 会場コード {jcd:02d} 終了 ({len(stadium_results)}レース取得)")
    return stadium_results

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    args = parser.parse_args()

    os.makedirs("data", exist_ok=True)
    start_d = datetime.strptime(args.start, "%Y-%m-%d")
    end_d = datetime.strptime(args.end, "%Y-%m-%d")

    current = start_d
    while current <= end_d:
        d_str = current.strftime("%Y%m%d")
        print(f"🚀 収集開始: {d_str}")
        
        # 会場(1-24)を並列で実行して時短！
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            task_args = [(jcd, d_str) for jcd in range(1, 25)]
            all_stadium_results = list(executor.map(process_stadium, task_args))
        
        # 2次元リストを1次元に統合
        flat_results = [item for sublist in all_stadium_results for item in sublist]
        
        if flat_results:
            df = pd.DataFrame(flat_results)
            filename = f"data/pure_data_{d_str}.csv"
            df.to_csv(filename, index=False)
            print(f"✨ 保存完了: {filename} ({len(flat_results)} レース)")
        
        current += timedelta(days=1)
