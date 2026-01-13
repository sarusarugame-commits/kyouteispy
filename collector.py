import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import sys
import argparse
import os
import traceback
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# 🔥 ログを強制的に即時表示させるおまじない
sys.stdout.reconfigure(line_buffering=True)

# ==========================================
# ⚙️ 設定（高速並列モード）
# ==========================================
MAX_RETRIES = 3
RETRY_INTERVAL = 3
MAX_WORKERS = 8  # 8会場同時に攻める！

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
            res = session.get(url, timeout=20) # タイムアウトを少し短くして回転率を上げる
            res.encoding = res.apparent_encoding
            if res.status_code == 200:
                return BeautifulSoup(res.text, 'html.parser')
            elif res.status_code == 403:
                print(f"⛔ 403 Forbidden: {url}")
        except:
            pass
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_INTERVAL)
    return None

def scrape_race_data(session, jcd, rno, date_str):
    base_url = "https://www.boatrace.jp/owpc/pc/race"
    
    # 3ページ取得
    soup_list = get_soup_with_retry(session, f"{base_url}/racelist?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    soup_before = get_soup_with_retry(session, f"{base_url}/beforeinfo?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    soup_res = get_soup_with_retry(session, f"{base_url}/raceresult?rno={rno}&jcd={jcd:02d}&hd={date_str}")

    if not all([soup_list, soup_before, soup_res]):
        return None

    try:
        # 風速取得（修正済みロジック）
        wind = 0.0
        weather_units = soup_before.select('.weather1_bodyUnit')
        for unit in weather_units:
            title_elem = unit.select_one('.weather1_bodyUnitLabelTitle')
            if title_elem and '風速' in title_elem.text:
                data_elem = unit.select_one('.weather1_bodyUnitLabelData')
                if data_elem:
                    try:
                        wind = float(data_elem.text.strip().replace('m', ''))
                    except:
                        pass
                break

        # 正解ラベル
        res1_text = soup_res.select_one('.is-p_0-1 .is-p_1-1') 
        res1 = 1 if (res1_text and res1_text.text.strip() == "1") else 0

        # 展示タイム & 欠損チェック
        temp_ex_times = []
        for i in range(1, 7):
            ex_elem = soup_before.select(f'tbody.is-p_0-{i}')
            if not ex_elem: return None
            ex_val = ex_elem[0].select('td')[4].text.strip()
            if not ex_val or ex_val == "-" or float(ex_val) <= 0:
                return None
            temp_ex_times.append(float(ex_val))

        # データ構築
        row = {'date': date_str, 'jcd': jcd, 'rno': rno, 'wind': wind, 'res1': res1}
        for i in range(1, 7):
            tbody = soup_list.select(f'tbody.is-p_0-{i}')[0].select('td')
            row[f'wr{i}'] = float(tbody[3].select_one('div').text.split()[0])
            row[f'mo{i}'] = float(tbody[6].select_one('div').text.split()[0])
            row[f'ex{i}'] = temp_ex_times[i-1]
        return row

    except:
        return None

def process_stadium(args):
    """会場単位で処理するワーカー関数"""
    session, jcd, date_str = args
    results = []
    # ログを出して生存確認
    print(f"🏟️ 会場{jcd:02d} スキャン開始...", flush=True)
    
    for rno in range(1, 13):
        data = scrape_race_data(session, jcd, rno, date_str)
        if data:
            results.append(data)
            
    print(f"✅ 会場{jcd:02d} 完了 ({len(results)}レース)", flush=True)
    return results

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    args = parser.parse_args()

    # セッション準備
    print("🚀 高速収集クライアント起動", flush=True)
    session = get_session()
    
    # 最初にトップページを踏んでCookieゲット
    try:
        get_soup_with_retry(session, "https://www.boatrace.jp/")
        print("🔓 認証突破成功。並列スキャンを開始します...", flush=True)
    except:
        print("⚠️ トップページアクセス失敗（続行します）")

    start_d = datetime.strptime(args.start, "%Y-%m-%d")
    end_d = datetime.strptime(args.end, "%Y-%m-%d")
    current = start_d
    
    while current <= end_d:
        d_str = current.strftime("%Y%m%d")
        print(f"📅 日付処理中: {d_str}", flush=True)
        
        # マルチスレッドで会場ごとに並列実行
        # 引数リスト作成: (session, 会場コード, 日付)
        tasks = [(session, jcd, d_str) for jcd in range(1, 25)]
        
        day_results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # 実行結果を受け取る
            futures = executor.map(process_stadium, tasks)
            for res in futures:
                day_results.extend(res)
        
        if day_results:
            df = pd.DataFrame(day_results)
            os.makedirs("data", exist_ok=True)
            filename = f"data/pure_data_{d_str}.csv"
            df.to_csv(filename, index=False)
            print(f"💾 {d_str} 保存完了: {len(df)}レース", flush=True)
        else:
            print(f"⚠️ {d_str} データなし", flush=True)
            
        current += timedelta(days=1)
