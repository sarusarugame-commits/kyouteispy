import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import sys
import argparse
import os
import traceback # エラー詳細を見るために追加
from datetime import datetime, timedelta

# ==========================================
# ⚙️ 設定（診断モード）
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
    """セッションを使ってアクセス"""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"🌐 アクセス中 ({attempt}/{MAX_RETRIES}): {url}", flush=True)
            res = session.get(url, timeout=30)
            res.encoding = res.apparent_encoding
            
            if res.status_code == 200:
                soup = BeautifulSoup(res.text, 'html.parser')
                # 🔥 ここでページタイトルを確認！
                title = soup.title.text.strip() if soup.title else "タイトルなし"
                print(f"📄 ページタイトル: {title}", flush=True)
                return soup
            elif res.status_code == 403:
                print("⛔ 403 Forbidden: アクセス拒否", flush=True)
            else:
                print(f"⚠️ ステータス {res.status_code}", flush=True)
                
        except Exception as e:
            print(f"💥 エラー: {e}", flush=True)
        
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_INTERVAL)
            
    return None

def scrape_race_data(session, jcd, rno, date_str):
    base_url = "https://www.boatrace.jp/owpc/pc/race"
    
    soup_list = get_soup_with_retry(session, f"{base_url}/racelist?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    if not soup_list: return None
    
    # ここではテストのため、あえて間隔を詰めずに原因を探る
    soup_before = get_soup_with_retry(session, f"{base_url}/beforeinfo?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    if not soup_before: return None

    soup_res = get_soup_with_retry(session, f"{base_url}/raceresult?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    if not soup_res: return None

    try:
        # 要素取得テスト
        if soup_before.select_one('.weather1_bodyUnitLabelData'):
            print("✅ 風速データ要素: あり", flush=True)
        else:
            print("❌ 風速データ要素: なし（ページの中身が違う可能性大）", flush=True)

        w_text = soup_before.select_one('.weather1_bodyUnitLabelData').text.replace('m','').strip()
        wind = float(w_text) if w_text else 0.0

        res1_text = soup_res.select_one('.is-p_0-1 .is-p_1-1') 
        res1 = 1 if (res1_text and res1_text.text.strip() == "1") else 0

        temp_ex_times = []
        for i in range(1, 7):
            ex_elem = soup_before.select(f'tbody.is-p_0-{i}')
            if not ex_elem:
                print(f"❌ {i}号艇の展示データが見つかりません", flush=True)
                return None
                
            ex_val = ex_elem[0].select('td')[4].text.strip()
            if not ex_val or ex_val == "-" or float(ex_val) <= 0:
                print(f"⚠️ {i}号艇の展示タイムが無効: {ex_val}", flush=True)
                return None
            temp_ex_times.append(float(ex_val))

        # データ構築
        row = {'date': date_str, 'jcd': jcd, 'rno': rno, 'wind': wind, 'res1': res1}
        # (中略: データ格納処理)
        
        return row

    except Exception as e:
        print("💥 パースエラー発生！詳細:", flush=True)
        print(traceback.format_exc(), flush=True) # エラーの正体を全部出す
        return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    args = parser.parse_args()

    session = get_session()
    
    print("🏠 トップページに挨拶中...", flush=True)
    get_soup_with_retry(session, "https://www.boatrace.jp/")

    # 1/1 桐生(01) 1R でテスト
    print(f"🚀 診断実行: 20250101 会場01 レース01", flush=True)
    data = scrape_race_data(session, 1, 1, "20250101")
    
    if data:
        print("✅ 成功！", flush=True)
    else:
        print("❌ 失敗。上のログを確認してください。", flush=True)
