import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import sys
import argparse
import os

# ==========================================
# ⚙️ 設定（ステルスモード）
# ==========================================
MAX_RETRIES = 3
RETRY_INTERVAL = 5 # 焦らず5秒待つ

def get_session():
    """人間らしいセッションを作成"""
    session = requests.Session()
    # 一般的なWindowsのChromeに見せかける強力な偽装ヘッダー
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'ja,en-US;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Referer': 'https://www.boatrace.jp/',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1'
    }
    session.headers.update(headers)
    return session

def get_soup_with_retry(session, url):
    """セッションを使ってアクセス"""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"🌐 アクセス中 ({attempt}/{MAX_RETRIES}): {url}", flush=True)
            # タイムアウトを30秒に延長（粘る）
            res = session.get(url, timeout=30)
            res.encoding = res.apparent_encoding
            
            if res.status_code == 200:
                return BeautifulSoup(res.text, 'html.parser')
            elif res.status_code == 403:
                print("⛔ 403 Forbidden: アクセス拒否されました（IPブロックの可能性大）", flush=True)
            else:
                print(f"⚠️ ステータス {res.status_code}", flush=True)
                
        except Exception as e:
            print(f"💥 エラー: {e}", flush=True)
        
        if attempt < MAX_RETRIES:
            print(f"💤 {RETRY_INTERVAL}秒 待機...", flush=True)
            time.sleep(RETRY_INTERVAL)
            
    return None

def scrape_race_data(session, jcd, rno, date_str):
    base_url = "https://www.boatrace.jp/owpc/pc/race"
    
    # 1レース内でセッション（Cookie）を使い回す
    soup_list = get_soup_with_retry(session, f"{base_url}/racelist?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    if not soup_list: return None
    # 少し間隔を空ける（人間アピール）
    time.sleep(1)
    
    soup_before = get_soup_with_retry(session, f"{base_url}/beforeinfo?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    if not soup_before: return None
    time.sleep(1)

    soup_res = get_soup_with_retry(session, f"{base_url}/raceresult?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    if not soup_res: return None

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

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    args = parser.parse_args()

    # セッション開始
    session = get_session()
    
    # まずトップページにアクセスしてCookieをもらう（重要！）
    print("🏠 トップページに挨拶中...", flush=True)
    get_soup_with_retry(session, "https://www.boatrace.jp/")

    start_d = datetime.strptime(args.start, "%Y-%m-%d")
    end_d = datetime.strptime(args.end, "%Y-%m-%d")
    current = start_d
    results = []

    # テストのため、まずは「最初の1会場・1レース」だけ試す安全装置
    # うまくいったらループに戻す
    d_str = current.strftime("%Y%m%d")
    print(f"🚀 テスト収集: {d_str} 会場01 レース01", flush=True)
    
    data = scrape_race_data(session, 1, 1, d_str)
    if data:
        print("✅ 突破成功！データが取れました！", flush=True)
        print(data, flush=True)
    else:
        print("❌ 突破失敗。やはりIPブロックが強力です。", flush=True)
