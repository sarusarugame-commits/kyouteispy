import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import re
import os
import unicodedata
import sys

# --- 設定 ---
# 確実にデータがある過去の日付（例：2025/01/01）
TARGET_DATE = "20250101" 
MAX_RACES = 5  # 5レースだけ確認して終了

def clean_text(text):
    if not text: return ""
    text = unicodedata.normalize('NFKC', str(text))
    return text.replace("\n", "").replace("\r", "").replace(" ", "").replace("¥", "").replace(",", "").strip()

def get_soup(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    for _ in range(3):
        try:
            res = requests.get(url, headers=headers, timeout=10)
            res.encoding = res.apparent_encoding
            if res.status_code == 200:
                if "データがありません" in res.text: return None
                return BeautifulSoup(res.text, 'html.parser')
            time.sleep(1)
        except:
            time.sleep(1)
    return None

def scrape_race(jcd, rno, date_str):
    print(f"[{jcd}場 {rno}R] 取得開始...", end="")
    base_url = "https://www.boatrace.jp/owpc/pc/race"
    
    # URL生成
    url_before = f"{base_url}/beforeinfo?rno={rno}&jcd={jcd:02d}&hd={date_str}"
    url_res = f"{base_url}/raceresult?rno={rno}&jcd={jcd:02d}&hd={date_str}"
    url_list = f"{base_url}/racelist?rno={rno}&jcd={jcd:02d}&hd={date_str}"
    
    # 取得
    soup_before = get_soup(url_before)
    soup_res = get_soup(url_res)
    soup_list = get_soup(url_list)
    
    if not (soup_before and soup_res and soup_list):
        print(" ❌ ページ取得失敗")
        return None

    try:
        row = {'date': date_str, 'jcd': jcd, 'rno': rno}

        # --- ① 風速 ---
        try:
            wind_elem = soup_before.select_one(".weather1_bodyUnitLabelData")
            row['wind'] = float(clean_text(wind_elem.text).replace("m", "")) if wind_elem else 0.0
        except: row['wind'] = 0.0

        # --- ② 着順 (rank1, rank2, rank3) ---
        try:
            rank_rows = soup_res.select("table.is-w495 tbody tr")
            for r in rank_rows:
                tds = r.select("td")
                if len(tds) > 1:
                    rank_idx = clean_text(tds[0].text)
                    boat_num = clean_text(tds[1].text)
                    if rank_idx.isdigit() and int(rank_idx) <= 3:
                        row[f'rank{rank_idx}'] = int(boat_num)
        except: pass

        # --- ③ 3連単配当 (payout) ---
        row['payout'] = 0
        try:
            payout_th = soup_res.find(lambda tag: tag.name == "th" and "3連単" in tag.text)
            if payout_th:
                payout_td = payout_th.find_next_sibling("td").find_next_sibling("td")
                if payout_td:
                    payout_val = clean_text(payout_td.text)
                    if payout_val.isdigit():
                        row['payout'] = int(payout_val)
        except: pass

        # --- ④ 各艇データ (F数, ST, モーター) ---
        for i in range(1, 7):
            # 展示タイム
            try:
                boat_cell = soup_before.select_one(f".is-boatColor{i}")
                if boat_cell:
                    tds = boat_cell.find_parent("tbody").select("td")
                    ex_val = clean_text(tds[4].text)
                    row[f'ex{i}'] = float(ex_val) if ex_val and ex_val != "." else 0.0
                else: row[f'ex{i}'] = 0.0
            except: row[f'ex{i}'] = 0.0

            # 番組表データ
            try:
                list_tbody = soup_list.select_one(f".is-boatColor{i}").find_parent("tbody")
                tds = list_tbody.select("td")
                
                # 勝率
                wr_match = re.search(r"(\d\.\d{2})", clean_text(tds[3].text))
                row[f'wr{i}'] = float(wr_match.group(1)) if wr_match else 0.0
                
                # F数 (選手名欄から)
                f_match = re.search(r"F(\d+)", clean_text(tds[2].text))
                row[f'f{i}'] = int(f_match.group(1)) if f_match else 0
                
                # 平均ST
                st_match = re.search(r"ST(\d\.\d{2})", list_tbody.text)
                row[f'st{i}'] = float(st_match.group(1)) if st_match else 0.17
                
                # モーター2連率 (正しいカラムから抽出)
                mo_text = clean_text(tds[5].text) # 通常ここ
                mo_match = re.search(r"(\d{1,3}\.\d)", mo_text)
                if not mo_match:
                    mo_text = clean_text(tds[6].text) # 念のため隣も
                    mo_match = re.search(r"(\d{1,3}\.\d)", mo_text)
                row[f'mo{i}'] = float(mo_match.group(1)) if mo_match else 0.0
                
            except:
                row[f'wr{i}'], row[f'f{i}'], row[f'st{i}'], row[f'mo{i}'] = 0.0, 0, 0.20, 0.0

        print(" ✅ OK")
        return row

    except Exception as e:
        print(f" ❌ Error: {e}")
        return None

if __name__ == "__main__":
    print(f"DEBUG START: {TARGET_DATE} (Max {MAX_RACES} races)")
    data_list = []
    
    for jcd in range(1, 25):
        if len(data_list) >= MAX_RACES: break
        for rno in range(1, 13):
            if len(data_list) >= MAX_RACES: break
            
            row = scrape_race(jcd, rno, TARGET_DATE)
            if row:
                # ログに重要データを表示（Actionsの画面で確認するため）
                print(f"   -> Result: 1着={row.get('rank1')} 2着={row.get('rank2')} 3着={row.get('rank3')}")
                print(f"   -> Payout: {row.get('payout')}円")
                print(f"   -> 1号艇: F{row.get('f1')} / ST{row.get('st1')} / Motor{row.get('mo1')}%")
                data_list.append(row)
                time.sleep(1)
            
    if data_list:
        df = pd.DataFrame(data_list)
        df.to_csv("debug_result.csv", index=False)
        print("\n--- DEBUG SUCCESS ---")
        print(df.head())
    else:
        print("\n--- NO DATA ---")
        sys.exit(1) # エラーで終了
