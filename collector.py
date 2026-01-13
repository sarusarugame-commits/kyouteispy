import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import sys
import argparse
import os
from datetime import datetime, timedelta

# ==========================================
# ⚙️ 設定（ステルスモード・完成版）
# ==========================================
MAX_RETRIES = 3
RETRY_INTERVAL = 5 

def get_session():
    """人間らしいセッションを作成"""
    session = requests.Session()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'ja,en-US;q=0.9,en;q=0.8',
        'Referer': 'https://www.boatrace.jp/',
    }
    session.headers.update(headers)
    return session

def get_soup_with_retry(session, url):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # ログを減らして高速化
            res = session.get(url, timeout=30)
            res.encoding = res.apparent_encoding
            if res.status_code == 200:
                return BeautifulSoup(res.text, 'html.parser')
            elif res.status_code == 403:
                print("⛔ 403 Forbidden", flush=True)
        except:
            pass
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_INTERVAL)
    return None

def scrape_race_data(session, jcd, rno, date_str):
    base_url = "https://www.boatrace.jp/owpc/pc/race"
    
    soup_list = get_soup_with_retry(session, f"{base_url}/racelist?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    if not soup_list: return None
    time.sleep(1) # 人間アピール
    
    soup_before = get_soup_with_retry(session, f"{base_url}/beforeinfo?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    if not soup_before: return None
    time.sleep(1)

    soup_res = get_soup_with_retry(session, f"{base_url}/raceresult?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    if not soup_res: return None

    try:
        # 🔥 【修正箇所】気温ではなく「風速」を確実に見つけるロジック
        wind = 0.0
        weather_units = soup_before.select('.weather1_bodyUnit')
        for unit in weather_units:
            title_elem = unit.select_one('.weather1_bodyUnitLabelTitle')
            if title_elem and '風速' in title_elem.text:
                data_elem = unit.select_one('.weather1_bodyUnitLabelData')
                if data_elem:
                    w_text = data_elem.text.strip().replace('m', '')
                    try:
                        wind = float(w_text)
                    except:
                        pass
                break

        # 正解ラベル
        res1_text = soup_res.select_one('.is-p_0-1 .is-p_1-1') 
        res1 = 1 if (res1_text and res1_text.text.strip() == "1") else 0

        # 展示タイム
        temp_ex_times = []
        for i in range(1, 7):
            ex_elem = soup_before.select(f'tbody.is-p_0-{i}')
            if not ex_elem: return None
            ex_val = ex_elem[0].select('td')[4].text.strip()
            
            # 欠損チェック
            if not ex_val or ex_val == "-" or float(ex_val) <= 0:
                return None
            temp_ex_times.append(float(ex_val))

        # データ格納
        row = {'date': date_str, 'jcd': jcd, 'rno': rno, 'wind': wind, 'res1': res1}
        for i in range(1, 7):
            tbody = soup_list.select(f'tbody.is-p_0-{i}')[0].select('td')
            row[f'wr{i}'] = float(tbody[3].select_one('div').text.split()[0])
            row[f'mo{i}'] = float(tbody[6].select_one('div').text.split()[0])
            row[f'ex{i}'] = temp_ex_times[i-1]

        return row
    except:
        return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    args = parser.parse_args()

    # セッション開始（トップページ踏み）
    session = get_session()
    get_soup_with_retry(session, "https://www.boatrace.jp/")

    start_d = datetime.strptime(args.start, "%Y-%m-%d")
    end_d = datetime.strptime(args.end, "%Y-%m-%d")
    current = start_d
    results = []
    
    print(f"🚀 収集開始: {args.start} 〜 {args.end}", flush=True)

    while current <= end_d:
        d_str = current.strftime("%Y%m%d")
        print(f"📅 処理中: {d_str}", flush=True)
        
        # 16並列で回すときは1日1会場ずつなど負荷分散されるので
        # ここでは直列で丁寧に回す
        for jcd in range(1, 25):
            for rno in range(1, 13):
                data = scrape_race_data(session, jcd, rno, d_str)
                if data:
                    results.append(data)
            # 会場ごとに少し休憩
            time.sleep(1)
            
        current += timedelta(days=1)

    if results:
        df = pd.DataFrame(results)
        os.makedirs("data", exist_ok=True)
        filename = f"data/pure_data_{args.start}_{args.end}.csv"
        df.to_csv(filename, index=False)
        print(f"✅ 保存完了: {filename} ({len(df)}レース)", flush=True)
    else:
        print("⚠️ データなし（レース開催なしか、取得失敗）", flush=True)
