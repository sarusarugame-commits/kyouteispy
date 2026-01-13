import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import sys
import argparse
import os
import re
from datetime import datetime, timedelta # 👈 これでエラーは消えます

# ログを即時表示
sys.stdout.reconfigure(line_buffering=True)

# ==========================================
# ⚙️ 設定
# ==========================================
MAX_RETRIES = 3
RETRY_INTERVAL = 3

def get_session():
    session = requests.Session()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    }
    session.headers.update(headers)
    return session

def get_soup_with_retry(session, url):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            res = session.get(url, timeout=30)
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
    base_url = "https://www.boatrace.jp/owpc/pc/race"
    print(f"🔍 {date_str} {jcd}場 {rno}R: ", end="")
    
    # 3ページ取得
    soup_list = get_soup_with_retry(session, f"{base_url}/racelist?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    soup_before = get_soup_with_retry(session, f"{base_url}/beforeinfo?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    soup_res = get_soup_with_retry(session, f"{base_url}/raceresult?rno={rno}&jcd={jcd:02d}&hd={date_str}")

    if not all([soup_list, soup_before, soup_res]):
        print("❌ HTML取得失敗")
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
            if not boat_cell:
                print(f"⚠️ {i}号艇なし ", end="")
                return None
            
            tbody = boat_cell.find_parent("tbody")
            tds = tbody.select("td")
            
            # [写真, 選手名, 体重, 展示, チルト...] -> 通常はindex 4
            ex_val = clean_text(tds[4].text)
            if not ex_val: ex_val = clean_text(tds[5].text) # ズレ対策
            
            if not ex_val or ex_val == "-" or ex_val == "0.00":
                print(f"⚠️ {i}号艇展示欠損 ", end="")
                return None
            
            try:
                temp_ex_times.append(float(ex_val))
            except:
                print(f"❌ 数値化不可[{ex_val}] ", end="")
                return None

        # --- 4. 出走表データ ---
        row = {'date': date_str, 'jcd': jcd, 'rno': rno, 'wind': wind, 'res1': res1}
        
        for i in range(1, 7):
            boat_cell_list = soup_list.select_one(f".is-boatColor{i}")
            if not boat_cell_list: return None
            
            tbody_list = boat_cell_list.find_parent("tbody")
            tds_list = tbody_list.select("td")
            
            try:
                # 全国勝率
                row[f'wr{i}'] = float(re.findall(r"\d+\.\d+", tds_list[3].text)[0])
                # モーター2連率
                nums = re.findall(r"\d+\.\d+", tds_list[6].text)
                if len(nums) >= 1:
                     row[f'mo{i}'] = float(nums[0])
                else:
                     row[f'mo{i}'] = 0.0
            except:
                row[f'wr{i}'] = 0.0
                row[f'mo{i}'] = 0.0

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

    os.makedirs("data", exist_ok=True)
    session = get_session()
    
    # 認証
    try:
        session.get("https://www.boatrace.jp/", timeout=10)
    except:
        pass

    # 🔥 1レース限定デバッグ 🔥
    # 1月1日、桐生(01)、1R 固定
    d_str = "20250101"
    jcd = 1
    rno = 1
    
    print(f"🚀 1レース限定デバッグ開始")
    
    results = []
    data = scrape_race_data(session, jcd, rno, d_str)
    
    if data:
        results.append(data)
        df = pd.DataFrame(results)
        filename = f"data/pure_data_debug_1R.csv"
        df.to_csv(filename, index=False)
        print(f"\n🎉 完了！CSV保存しました: {filename}")
        print(df) # ログに中身を表示
    else:
        print("\n💀 データ取得失敗")
