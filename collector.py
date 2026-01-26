import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import re
import os
import unicodedata
import argparse
import random
import threading
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

# ==========================================
# ⚙️ 設定エリア
# ==========================================
MAX_WORKERS = 8 # 並列数を少し下げて安定重視に
MAX_RETRIES = 5
RETRY_DELAY = 3

UA_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
]

print_lock = threading.Lock()

def safe_print(msg):
    with print_lock:
        print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

def clean_text(text):
    if not text: return ""
    text = unicodedata.normalize('NFKC', str(text))
    return text.replace("\n", "").replace("\r", "").replace("¥", "").replace(",", "").strip()

def get_session():
    session = requests.Session()
    retries = Retry(total=MAX_RETRIES, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS, max_retries=retries)
    session.mount("https://", adapter)
    return session

def get_soup(session, url):
    for i in range(MAX_RETRIES):
        try:
            headers = {'User-Agent': random.choice(UA_LIST)}
            res = session.get(url, headers=headers, timeout=30)
            res.encoding = res.apparent_encoding
            if res.status_code == 200:
                if "データがありません" in res.text: return None, "SKIP"
                return BeautifulSoup(res.text, 'html.parser'), None
            time.sleep(random.uniform(1, 2))
        except Exception as e:
            time.sleep(RETRY_DELAY)
    return None, "ERROR"

def extract_payout(soup, key_text):
    try:
        for tbl in soup.select("table"):
            if key_text in tbl.text:
                for tr in tbl.select("tr"):
                    if key_text in tr.text:
                        for td in tr.select("td"):
                            txt = clean_text(td.text)
                            if txt.isdigit() and len(txt) >= 2:
                                return int(txt)
    except: pass
    return 0

def scrape_race_data(session, jcd, rno, date_str):
    base_url = "https://www.boatrace.jp/owpc/pc/race"
    
    # ページ取得
    soup_before, err = get_soup(session, f"{base_url}/beforeinfo?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    if err == "SKIP" or not soup_before: return None

    soup_res, err = get_soup(session, f"{base_url}/raceresult?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    if not soup_res: return None

    soup_list, err = get_soup(session, f"{base_url}/racelist?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    if not soup_list: return None

    try:
        row = {'date': date_str, 'jcd': jcd, 'rno': rno}

        # 天候・風
        try:
            wind_elem = soup_before.select_one(".weather1_bodyUnitLabelData")
            row['wind'] = float(clean_text(wind_elem.text).replace("m", "").replace(" ", "")) if wind_elem else 0.0
        except: row['wind'] = 0.0

        # --- 【修正版】順位取得 ---
        row['rank1'], row['rank2'], row['rank3'] = None, None, None
        try:
            # 確実に結果テーブルの「行」を狙い撃ちする
            result_rows = soup_res.select("table.is-w495 tbody tr")
            
            # 1着 (Row 0)
            if len(result_rows) >= 1:
                # td[1] が艇番 (例: "1" や "1号艇" の画像)
                r1_txt = clean_text(result_rows[0].select("td")[1].text)
                row['rank1'] = int(re.search(r"^(\d{1})", r1_txt).group(1))
            
            # 2着 (Row 1)
            if len(result_rows) >= 2:
                r2_txt = clean_text(result_rows[1].select("td")[1].text)
                row['rank2'] = int(re.search(r"^(\d{1})", r2_txt).group(1))
            
            # 3着 (Row 2)
            if len(result_rows) >= 3:
                r3_txt = clean_text(result_rows[2].select("td")[1].text)
                row['rank3'] = int(re.search(r"^(\d{1})", r3_txt).group(1))
        except: pass
        
        row['res1'] = 1 if row.get('rank1') == 1 else 0

        # 払い戻し
        row['tansho'] = extract_payout(soup_res, "単勝")
        row['nirentan'] = extract_payout(soup_res, "2連単")
        row['sanrentan'] = extract_payout(soup_res, "3連単")
        row['sanrenpuku'] = extract_payout(soup_res, "3連複")
        row['payout'] = row['sanrentan']

        # 各艇データ
        for i in range(1, 7):
            row[f'wr{i}'] = 0.0
            row[f'mo{i}'] = 0.0
            row[f'ex{i}'] = 0.0
            row[f'f{i}'] = 0
            row[f'st{i}'] = 0.20

            try:
                boat_cell = soup_before.select_one(f".is-boatColor{i}")
                if boat_cell:
                    tds = boat_cell.find_parent("tbody").select("td")
                    if len(tds) > 4:
                        ex_val = clean_text(tds[4].text)
                        if re.match(r"\d\.\d{2}", ex_val):
                            row[f'ex{i}'] = float(ex_val)
            except: pass

            try:
                list_cell = soup_list.select_one(f".is-boatColor{i}")
                if list_cell:
                    tds = list_cell.find_parent("tbody").select("td")
                    if len(tds) > 3:
                        txt = clean_text(tds[3].text)
                        f_match = re.search(r"F(\d+)", txt)
                        if f_match: row[f'f{i}'] = int(f_match.group(1))
                        st_match = re.search(r"(\.\d{2}|\d\.\d{2})", txt)
                        if st_match:
                            val = float(st_match.group(1))
                            if val < 1.0: row[f'st{i}'] = val
                    if len(tds) > 4:
                        txt = tds[4].get_text(" ").strip()
                        wr_match = re.search(r"(\d\.\d{2})", txt)
                        if wr_match: row[f'wr{i}'] = float(wr_match.group(1))
                    if len(tds) > 6:
                        txt = tds[6].get_text(" ").strip()
                        mo_vals = re.findall(r"(\d{1,3}\.\d{2})", txt)
                        if len(mo_vals) >= 1:
                            row[f'mo{i}'] = float(mo_vals[0])
            except: pass
        
        return row
    except: return None

def process_wrapper(args):
    session, jcd, rno, date_str = args
    time.sleep(random.uniform(0.5, 1.5))
    try:
        return scrape_race_data(session, jcd, rno, date_str)
    except Exception as e:
        safe_print(f"❌ Error in process: {e}")
        return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    # 引数を必須(required=True)から任意に変更し、デフォルト値を設定
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    today = datetime.now().strftime("%Y-%m-%d")
    
    parser.add_argument("--start", default=yesterday, help="開始日 (YYYY-MM-DD)")
    parser.add_argument("--end", default=today, help="終了日 (YYYY-MM-DD)")
    args = parser.parse_args()

    session = get_session()
    
    try:
        start_d = datetime.strptime(args.start, "%Y-%m-%d")
        end_d = datetime.strptime(args.end, "%Y-%m-%d")
    except ValueError:
        print("❌ 日付の形式が間違っています。YYYY-MM-DD で指定してください。")
        input("Enterを押して終了...")
        sys.exit(1)

    current = start_d
    
    safe_print(f"🚀 収集開始: {args.start} 〜 {args.end}")
    
    os.makedirs("data", exist_ok=True)
    filename = f"data/data_{args.start.replace('-','')}_{args.end.replace('-','')}.csv"
    
    if not os.path.exists(filename):
        cols = ['date', 'jcd', 'rno', 'wind', 'res1', 'rank1', 'rank2', 'rank3', 
                'tansho', 'nirentan', 'sanrentan', 'sanrenpuku', 'payout']
        for i in range(1, 7):
            cols.extend([f'wr{i}', f'mo{i}', f'ex{i}', f'f{i}', f'st{i}'])
        pd.DataFrame(columns=cols).to_csv(filename, index=False)

    total_races = 0
    
    while current <= end_d:
        d_str = current.strftime("%Y%m%d")
        safe_print(f"📅 {d_str} のデータを収集中...")
        
        tasks = []
        for jcd in range(1, 25):
            for rno in range(1, 13):
                tasks.append((session, jcd, rno, d_str))
        
        results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # エラーをキャッチするために as_completed を使用
            future_to_task = {executor.submit(process_wrapper, t): t for t in tasks}
            for future in as_completed(future_to_task):
                try:
                    res = future.result()
                    if res: results.append(res)
                except Exception as exc:
                    safe_print(f"⚠️ Thread Error: {exc}")
        
        if results:
            df = pd.DataFrame(results)
            cols = ['date', 'jcd', 'rno', 'wind', 'res1', 'rank1', 'rank2', 'rank3', 
                    'tansho', 'nirentan', 'sanrentan', 'sanrenpuku', 'payout']
            for i in range(1, 7):
                cols.extend([f'wr{i}', f'mo{i}', f'ex{i}', f'f{i}', f'st{i}'])
            
            use_cols = [c for c in cols if c in df.columns]
            df = df[use_cols]
            
            df.to_csv(filename, mode='a', index=False, header=False)
            safe_print(f"  ✅ {len(df)}レース 保存しました")
            total_races += len(df)
        else:
            safe_print(f"  ⚠️ データが見つかりませんでした (中止または開催なし)")
        
        current += timedelta(days=1)
    
    safe_print("="*40)
    safe_print(f"🎉 すべて完了しました！")
    safe_print(f"📁 保存ファイル: {filename}")
    safe_print(f"📊 合計取得数: {total_races} レース")
    safe_print("="*40)
