import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import sys
import argparse
import os
import traceback
from datetime import datetime, timedelta

# ログ即時表示
sys.stdout.reconfigure(line_buffering=True)

# ==========================================
# ⚙️ 設定（デバッグ修正版）
# ==========================================
MAX_RETRIES = 3
RETRY_INTERVAL = 5 

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
            # タイムアウトを60秒に延長して粘る
            res = session.get(url, timeout=60)
            res.encoding = res.apparent_encoding
            
            if res.status_code == 200:
                return BeautifulSoup(res.text, 'html.parser')
            elif res.status_code == 403:
                print(f"⛔ 403 Forbidden: {url}")
        except Exception as e:
            print(f"⚠️ 通信エラー({attempt}): {e}")
        
        time.sleep(RETRY_INTERVAL)
    return None

def scrape_race_data(session, jcd, rno, date_str):
    base_url = "https://www.boatrace.jp/owpc/pc/race"
    
    print(f"🔍 {jcd}場 {rno}R: ", end="")
    
    # 3ページ取得
    soup_list = get_soup_with_retry(session, f"{base_url}/racelist?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    soup_before = get_soup_with_retry(session, f"{base_url}/beforeinfo?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    soup_res = get_soup_with_retry(session, f"{base_url}/raceresult?rno={rno}&jcd={jcd:02d}&hd={date_str}")

    if not all([soup_list, soup_before, soup_res]):
        print("❌ HTML取得失敗")
        return None

    try:
        # 風速チェック（修正済みロジック）
        wind = 0.0
        try:
            weather_units = soup_before.select('.weather1_bodyUnit')
            found_wind = False
            for unit in weather_units:
                title = unit.select_one('.weather1_bodyUnitLabelTitle')
                if title and '風速' in title.text:
                    data = unit.select_one('.weather1_bodyUnitLabelData')
                    if data:
                        wind = float(data.text.strip().replace('m', ''))
                        found_wind = True
                    break
            if not found_wind:
                print("[風速なし(0.0)]", end="")
        except:
            pass

        # 正解ラベル
        res1 = 0
        try:
            res1_text = soup_res.select_one('.is-p_0-1 .is-p_1-1') 
            res1 = 1 if (res1_text and res1_text.text.strip() == "1") else 0
        except:
            pass

        # 展示タイムチェック
        temp_ex_times = []
        for i in range(1, 7):
            # セレクタを少し緩くして検索
            # 前回の失敗箇所: soup_before.select(f'tbody.is-p_0-{i}')
            # 修正: クラス名が完全一致しなくても探せるようにする
            
            ex_val = None
            
            # パターンA: 標準的なクラス指定
            targets = soup_before.select(f'tbody.is-p_0-{i}')
            if targets:
                tds = targets[0].select('td')
                if len(tds) >= 5:
                    ex_val = tds[4].text.strip()
            
            # データが取れなかった場合、HTMLの中身をチラ見せしてデバッグ
            if ex_val is None:
                print(f"❌ {i}号艇HTML解析失敗")
                # bodyの中身の先頭を表示して、正しいページか確認
                print(f"\n🐛 デバッグダンプ: {str(soup_before.body)[:500]} \n")
                return None
            
            if not ex_val or ex_val == "-" or ex_val == "0.00":
                # データ欠損は正常な場合もあるが、理由を表示
                print(f"⚠️ {i}号艇タイムなし[{ex_val}] -> ", end="")
                return None
            
            try:
                temp_ex_times.append(float(ex_val))
            except:
                return None

        # 成功
        row = {'date': date_str, 'jcd': jcd, 'rno': rno, 'wind': wind, 'res1': res1}
        # 他のデータも取得
        for i in range(1, 7):
            tbody = soup_list.select(f'tbody.is-p_0-{i}')[0].select('td')
            row[f'wr{i}'] = float(tbody[3].select_one('div').text.split()[0])
            row[f'mo{i}'] = float(tbody[6].select_one('div').text.split()[0])
            row[f'ex{i}'] = temp_ex_times[i-1]
            
        print("✅ OK")
        return row

    except Exception as e:
        print(f"💥 {e}")
        return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    args = parser.parse_args()

    session = get_session()
    get_soup_with_retry(session, "https://www.boatrace.jp/")

    # 1/1 の 桐生(01) だけテスト
    jcd = 1
    d_str = "20250101"
    
    print(f"🚀 再デバッグ: {d_str} 会場{jcd:02d}")
    
    results = []
    for rno in range(1, 13):
        data = scrape_race_data(session, jcd, rno, d_str)
        if data:
            results.append(data)
        time.sleep(2)

    if results:
        df = pd.DataFrame(results)
        df.to_csv(f"data/debug_{d_str}.csv", index=False)
        print(f"\n🎉 保存完了: {len(df)}レース")
    else:
        print("\n💀 データゼロ")
