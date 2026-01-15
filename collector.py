import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import re
import os
import unicodedata
import sys
import argparse
import random

# ==========================================
# ⚙️ 設定エリア
# ==========================================
DEFAULT_TARGET_DATE = "20250101" 
MAX_RACES = 5 

UA_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0"
]

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

def clean_text(text):
    if not text: return ""
    text = unicodedata.normalize('NFKC', str(text))
    return text.replace("\n", "").replace("\r", "").replace("¥", "").replace(",", "").strip()

def get_soup(url, description="ページ"):
    for i in range(1, 4):
        try:
            headers = {'User-Agent': random.choice(UA_LIST)}
            res = requests.get(url, headers=headers, timeout=30)
            res.encoding = res.apparent_encoding
            if res.status_code == 200:
                if "データがありません" in res.text: return None
                return BeautifulSoup(res.text, 'html.parser')
            time.sleep(3)
        except: time.sleep(3)
    return None

def extract_payout(soup, key_text):
    """指定した賭け式（単勝、2連単など）の配当を抽出する"""
    try:
        # その文字を含む行を探す
        target_th = soup.find(lambda tag: tag.name == "th" and key_text in tag.text)
        if target_th:
            # thの親trを取得 -> その中のtdを探す
            parent_tr = target_th.find_parent("tr")
            tds = parent_tr.select("td")
            
            # 配当金っぽい数字を探す（組番の次にあることが多い）
            for td in tds:
                txt = clean_text(td.text)
                # 数字のみで、かつ "-" を含まない（組番ではない）ものを配当とみなす
                if txt.isdigit() and len(txt) > 1 and "-" not in txt:
                    return int(txt)
    except: pass
    return 0

def scrape_race(jcd, rno, date_str):
    log(f"🏁 【{jcd}場 {rno}R】 データ収集開始")
    base_url = "https://www.boatrace.jp/owpc/pc/race"
    
    soup_before = get_soup(f"{base_url}/beforeinfo?rno={rno}&jcd={jcd:02d}&hd={date_str}", "直前情報")
    if not soup_before: return None

    soup_res = get_soup(f"{base_url}/raceresult?rno={rno}&jcd={jcd:02d}&hd={date_str}", "レース結果")
    if not soup_res: return None

    soup_list = get_soup(f"{base_url}/racelist?rno={rno}&jcd={jcd:02d}&hd={date_str}", "番組表")
    if not soup_list: return None

    try:
        row = {'date': date_str, 'jcd': jcd, 'rno': rno}

        # --- ① 風速 ---
        try:
            wind_elem = soup_before.select_one(".weather1_bodyUnitLabelData")
            row['wind'] = float(clean_text(wind_elem.text).replace("m", "").replace(" ", "")) if wind_elem else 0.0
        except: row['wind'] = 0.0

        # --- ② 着順 ---
        row['rank1'], row['rank2'], row['rank3'] = None, None, None
        try:
            rank_rows = soup_res.select("table.is-w495 tbody tr")
            for r in rank_rows:
                tds = r.select("td")
                if len(tds) > 1:
                    rank_idx = clean_text(tds[0].text).replace(" ", "")
                    boat_text = clean_text(tds[1].text)
                    boat_match = re.search(r"^(\d{1})", boat_text)
                    if rank_idx.isdigit() and int(rank_idx) <= 3 and boat_match:
                        row[f'rank{rank_idx}'] = int(boat_match.group(1))
        except: pass
        row['res1'] = 1 if row.get('rank1') == 1 else 0

        # --- ③ 配当（ここを強化しました） ---
        # 必要な賭け式を全部取る
        row['tansho'] = extract_payout(soup_res, "単勝")
        row['nirentan'] = extract_payout(soup_res, "2連単")
        row['sanrentan'] = extract_payout(soup_res, "3連単")
        row['sanrenpuku'] = extract_payout(soup_res, "3連複")
        
        # 互換性のため payout = 3連単 にしておく
        row['payout'] = row['sanrentan']

        # --- ④ 各艇データ ---
        for i in range(1, 7):
            try:
                # 展示
                boat_cell = soup_before.select_one(f".is-boatColor{i}")
                if boat_cell:
                    tds = boat_cell.find_parent("tbody").select("td")
                    ex_val = clean_text(tds[4].text).replace(" ", "")
                    row[f'ex{i}'] = float(ex_val) if ex_val and ex_val != "." else 0.0
                else: row[f'ex{i}'] = 0.0

                # 詳細
                list_tbody = soup_list.select_one(f".is-boatColor{i}").find_parent("tbody")
                tds = list_tbody.select("td")
                
                wr_match = re.search(r"(\d\.\d{2})", clean_text(tds[3].text))
                row[f'wr{i}'] = float(wr_match.group(1)) if wr_match else 0.0
                
                f_match = re.search(r"F(\d+)", clean_text(tds[2].text))
                row[f'f{i}'] = int(f_match.group(1)) if f_match else 0
                
                st_match = re.search(r"ST(\d\.\d{2})", list_tbody.text.replace("\n", "").replace(" ", ""))
                row[f'st{i}'] = float(st_match.group(1)) if st_match else 0.17
                
                mo_text = clean_text(tds[5].text)
                mo_match = re.search(r"(\d{1,3}\.\d)", mo_text)
                if not mo_match:
                    mo_text = clean_text(tds[6].text)
                    mo_match = re.search(r"(\d{1,3}\.\d)", mo_text)
                row[f'mo{i}'] = float(mo_match.group(1)) if mo_match else 0.0
            except:
                row[f'wr{i}'], row[f'f{i}'], row[f'st{i}'], row[f'mo{i}'] = 0.0, 0, 0.20, 0.0

        log(f"  ✅ 取得成功 (単:{row['tansho']} / 2連:{row['nirentan']} / 3連:{row['sanrentan']})")
        return row

    except Exception as e:
        log(f"  ❌ データ解析エラー: {e}")
        return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default=DEFAULT_TARGET_DATE)
    parser.add_argument("--end", default=None)
    args = parser.parse_args()

    target_date = args.start.replace("-", "")
    log(f"🚀 DEBUG START: {target_date} (Max: {MAX_RACES} races)")
    
    collected_data = []
    
    for jcd in range(1, 25):
        if len(collected_data) >= MAX_RACES: break
        for rno in range(1, 13):
            if len(collected_data) >= MAX_RACES: break
            data = scrape_race(jcd, rno, target_date)
            if data:
                collected_data.append(data)
                time.sleep(3) 
            
    if collected_data:
        os.makedirs("data", exist_ok=True)
        df = pd.DataFrame(collected_data)
        
        # カラム順序整理（配当系を前に）
        cols = ['date', 'jcd', 'rno', 'wind', 'res1', 'rank1', 'rank2', 'rank3', 
                'tansho', 'nirentan', 'sanrentan', 'sanrenpuku', 'payout']
        for i in range(1, 7):
            cols.extend([f'wr{i}', f'mo{i}', f'ex{i}', f'f{i}', f'st{i}'])
        
        use_cols = [c for c in cols if c in df.columns]
        df = df[use_cols]

        output_path = "data/debug_result.csv"
        df.to_csv(output_path, index=False)
        log(f"🎉 完了！ データを保存しました: {output_path}")
    else:
        sys.exit(1)
