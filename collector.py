import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import sys
import argparse
import os
import traceback
from datetime import datetime, timedelta

# ログを即時表示（これがないとActionsでログが遅れる）
sys.stdout.reconfigure(line_buffering=True)

# ==========================================
# ⚙️ 設定（1会場ピンポイント診断）
# ==========================================
MAX_RETRIES = 3
RETRY_INTERVAL = 3
TARGET_JCD = 1  # 01:桐生 だけをテスト

def get_session():
    session = requests.Session()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Referer': 'https://www.boatrace.jp/',
    }
    session.headers.update(headers)
    return session

def get_soup_with_retry(session, url):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            res = session.get(url, timeout=10)
            res.encoding = res.apparent_encoding
            if res.status_code == 200:
                return BeautifulSoup(res.text, 'html.parser')
            elif res.status_code == 403:
                print(f"⛔ 403 Forbidden (ブロックされています): {url}")
        except Exception as e:
            print(f"⚠️ 通信エラー: {e}")
        time.sleep(RETRY_INTERVAL)
    return None

def scrape_race_data(session, jcd, rno, date_str):
    base_url = "https://www.boatrace.jp/owpc/pc/race"
    
    # 3ページ取得
    print(f"🔍 {jcd}場 {rno}R: アクセス開始...", end=" ")
    soup_list = get_soup_with_retry(session, f"{base_url}/racelist?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    soup_before = get_soup_with_retry(session, f"{base_url}/beforeinfo?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    soup_res = get_soup_with_retry(session, f"{base_url}/raceresult?rno={rno}&jcd={jcd:02d}&hd={date_str}")

    if not all([soup_list, soup_before, soup_res]):
        print("❌ ページ取得失敗 (HTMLなし)")
        return None

    try:
        # 風速チェック
        wind = 0.0
        try:
            weather_units = soup_before.select('.weather1_bodyUnit')
            for unit in weather_units:
                title_elem = unit.select_one('.weather1_bodyUnitLabelTitle')
                if title_elem and '風速' in title_elem.text:
                    data_elem = unit.select_one('.weather1_bodyUnitLabelData')
                    if data_elem:
                        wind = float(data_elem.text.strip().replace('m', ''))
                    break
        except:
            print("⚠️ 風速エラー(無視)", end=" ")

        # 正解ラベル
        res1 = 0
        try:
            res1_text = soup_res.select_one('.is-p_0-1 .is-p_1-1') 
            res1 = 1 if (res1_text and res1_text.text.strip() == "1") else 0
        except:
            pass

        # 展示タイムチェック（ここが一番あやしい）
        temp_ex_times = []
        for i in range(1, 7):
            ex_elem = soup_before.select(f'tbody.is-p_0-{i}')
            if not ex_elem:
                print(f"❌ {i}号艇データなし", end=" ")
                return None
            
            ex_val = ex_elem[0].select('td')[4].text.strip()
            
            # 詳細ログ出力
            if not ex_val or ex_val == "-" or ex_val == "0.00":
                print(f"❌ {i}号艇展示欠損[{ex_val}]", end=" ")
                return None
            
            try:
                val = float(ex_val)
                if val <= 0:
                    print(f"❌ {i}号艇展示異常[{val}]", end=" ")
                    return None
                temp_ex_times.append(val)
            except:
                print(f"❌ {i}号艇数値変換不可[{ex_val}]", end=" ")
                return None

        # ここまで来れば成功
        # データ構築
        row = {'date': date_str, 'jcd': jcd, 'rno': rno, 'wind': wind, 'res1': res1}
        for i in range(1, 7):
            tbody = soup_list.select(f'tbody.is-p_0-{i}')[0].select('td')
            row[f'wr{i}'] = float(tbody[3].select_one('div').text.split()[0])
            row[f'mo{i}'] = float(tbody[6].select_one('div').text.split()[0])
            row[f'ex{i}'] = temp_ex_times[i-1]
            
        print("✅ 成功！")
        return row

    except Exception as e:
        print(f"💥 パースエラー: {e}")
        return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    args = parser.parse_args()

    os.makedirs("data", exist_ok=True)
    session = get_session()
    
    # 認証用トップページアクセス
    get_soup_with_retry(session, "https://www.boatrace.jp/")

    start_d = datetime.strptime(args.start, "%Y-%m-%d")
    end_d = datetime.strptime(args.end, "%Y-%m-%d")
    current = start_d
    results = []

    print(f"🚀 1会場限定デバッグ: {args.start}")

    while current <= end_d:
        d_str = current.strftime("%Y%m%d")
        
        # 🔥 TARGET_JCD（01:桐生）だけを回す
        jcd = TARGET_JCD 
        print(f"🏟️ {d_str} 会場{jcd:02d} をスキャンします")
        
        for rno in range(1, 13):
            data = scrape_race_data(session, jcd, rno, d_str)
            if data:
                results.append(data)
            time.sleep(1) # サーバー負荷軽減
            
        current += timedelta(days=1)

    if results:
        df = pd.DataFrame(results)
        filename = f"data/pure_data_debug.csv"
        df.to_csv(filename, index=False)
        print(f"\n✨ 完了: {len(df)}レース取得しました。")
    else:
        print("\n💀 全レース失敗しました。上のログの ❌ を確認してください。")
