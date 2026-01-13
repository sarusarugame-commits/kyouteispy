import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime, timedelta
import argparse
import os

# ==========================================
# ⚙️ 設定
# ==========================================
MAX_RETRIES = 3           # 最大リトライ回数
RETRY_INTERVAL = 2        # リトライ間の待機時間 (秒)

def get_soup_with_retry(url):
    """HTML取得（エラー時に最大3回リトライ）"""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            headers = {'User-Agent': 'Mozilla/5.0'}
            res = requests.get(url, headers=headers, timeout=15)
            res.encoding = res.apparent_encoding
            
            if res.status_code == 200:
                return BeautifulSoup(res.text, 'html.parser')
            
            print(f"  ⚠️ ステータス {res.status_code}: {url} ({attempt}/{MAX_RETRIES})")
        except Exception as e:
            print(f"  ⚠️ 通信エラー: {url} ({attempt}/{MAX_RETRIES}) - {e}")
        
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_INTERVAL)
            
    return None

def scrape_race_data(jcd, rno, date_str):
    """1レース分の純粋な生データを取得（ダミー排除）"""
    base_url = "https://www.boatrace.jp/owpc/pc/race"
    
    # 3ページをリトライ付きで取得
    soup_list = get_soup_with_retry(f"{base_url}/racelist?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    soup_before = get_soup_with_retry(f"{base_url}/beforeinfo?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    soup_res = get_soup_with_retry(f"{base_url}/raceresult?rno={rno}&jcd={jcd:02d}&hd={date_str}")

    if not all([soup_list, soup_before, soup_res]):
        return None

    try:
        # 風速
        w_text = soup_before.select_one('.weather1_bodyUnitLabelData').text.replace('m','').strip()
        wind = float(w_text) if w_text else 0.0

        # 正解ラベル: 1号艇が1着なら1
        res1_text = soup_res.select_one('.is-p_0-1 .is-p_1-1') 
        res1 = 1 if (res1_text and res1_text.text.strip() == "1") else 0

        # 展示タイムの厳格チェック
        temp_ex_times = []
        for i in range(1, 7):
            ex_val = soup_before.select(f'tbody.is-p_0-{i}')[0].select('td')[4].text.strip()
            
            # 🔥 本物の展示タイムが欠損している場合はレースごとスキップ
            if not ex_val or ex_val == "-" or float(ex_val) <= 0:
                return None
            temp_ex_times.append(float(ex_val))

        # 生データのみを格納（魔法の式は含めない）
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

    os.makedirs("data", exist_ok=True)
    start_d = datetime.strptime(args.start, "%Y-%m-%d")
    end_d = datetime.strptime(args.end, "%Y-%m-%d")

    current = start_d
    results = []
    
    while current <= end_d:
        d_str = current.strftime("%Y%m%d")
        print(f"📅 収集日: {d_str}")
        for jcd in range(1, 25):
            for rno in range(1, 13):
                data = scrape_race_data(jcd, rno, d_str)
                if data:
                    results.append(data)
        current += timedelta(days=1)

    if results:
        df = pd.DataFrame(results)
        filename = f"data/pure_data_{args.start}.csv"
        df.to_csv(filename, index=False)
        print(f"✅ 保存完了: {filename} ({len(df)} レース)")
    else:
        print("❌ 取得可能な有効データがありませんでした。")
