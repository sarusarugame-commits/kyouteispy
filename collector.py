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
# 並列数を20に変更
MAX_WORKERS = 20  
MAX_RETRIES = 5
RETRY_DELAY = 3
TIMEOUT_SEC = 20

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

def get_column_names():
    """CSVのカラム定義を一箇所で管理"""
    cols = ['date', 'jcd', 'rno', 'wind', 'res1', 'rank1', 'rank2', 'rank3', 
            'tansho', 'nirentan', 'sanrentan', 'sanrenpuku', 'payout']
    for i in range(1, 7):
        cols.extend([f'wr{i}', f'mo{i}', f'ex{i}', f'f{i}', f'st{i}'])
    return cols

def get_session():
    session = requests.Session()
    retries = Retry(
        total=MAX_RETRIES,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    # 並列数に合わせてプールサイズも拡張
    adapter = HTTPAdapter(pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS, max_retries=retries)
    session.mount("https://", adapter)
    return session

def get_soup(session, url):
    for i in range(MAX_RETRIES):
        try:
            headers = {'User-Agent': random.choice(UA_LIST)}
            res = session.get(url, headers=headers, timeout=TIMEOUT_SEC)
            res.encoding = res.apparent_encoding
            
            if res.status_code == 200:
                if "データがありません" in res.text or "メンテナンス" in res.text:
                    return None, "SKIP"
                return BeautifulSoup(res.text, 'html.parser'), None
            
            if res.status_code == 404:
                return None, "ERROR"
                
            time.sleep(random.uniform(1, 2))
        except Exception:
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
                            if txt.isdigit() and (len(txt) >= 3 or int(txt) > 100):
                                return int(txt)
    except: pass
    return 0

def scrape_race_data(session, jcd, rno, date_str):
    base_url = "https://www.boatrace.jp/owpc/pc/race"
    
    url_res = f"{base_url}/raceresult?rno={rno}&jcd={jcd:02d}&hd={date_str}"
    url_bef = f"{base_url}/beforeinfo?rno={rno}&jcd={jcd:02d}&hd={date_str}"
    url_lst = f"{base_url}/racelist?rno={rno}&jcd={jcd:02d}&hd={date_str}"

    soup_res, err = get_soup(session, url_res)
    if err == "SKIP" or not soup_res: return None
    
    soup_before, _ = get_soup(session, url_bef)
    soup_list, _ = get_soup(session, url_lst)

    try:
        row = {'date': date_str, 'jcd': jcd, 'rno': rno}

        # 天候・風
        try:
            wind_elem = soup_before.select_one(".weather1_bodyUnitLabelData") if soup_before else None
            if wind_elem:
                w_txt = clean_text(wind_elem.text)
                m = re.search(r"(\d+)", w_txt)
                row['wind'] = float(m.group(1)) if m else 0.0
            else:
                row['wind'] = 0.0
        except: row['wind'] = 0.0

        # 順位
        row['rank1'], row['rank2'], row['rank3'] = None, None, None
        try:
            result_rows = soup_res.select("table.is-w495 tbody tr")
            if len(result_rows) >= 1:
                r1_txt = clean_text(result_rows[0].select("td")[1].text)
                row['rank1'] = int(re.search(r"^(\d{1})", r1_txt).group(1))
            if len(result_rows) >= 2:
                r2_txt = clean_text(result_rows[1].select("td")[1].text)
                row['rank2'] = int(re.search(r"^(\d{1})", r2_txt).group(1))
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

            # 展示タイム
            if soup_before:
                try:
                    boat_cell = soup_before.select_one(f".is-boatColor{i}")
                    if boat_cell:
                        tr = boat_cell.find_parent("tr")
                        tds = tr.select("td")
                        if len(tds) > 4:
                            for td in tds[4:]:
                                val = clean_text(td.text)
                                if re.match(r"^\d\.\d{2}$", val):
                                    row[f'ex{i}'] = float(val)
                                    break
                except: pass

            # 勝率・F・ST
            if soup_list:
                try:
                    list_cell = soup_list.select_one(f".is-boatColor{i}")
                    if list_cell:
                        tr = list_cell.find_parent("tr")
                        tds = tr.select("td")
                        full_row_text = " ".join([clean_text(td.text) for td in tds])
                        
                        f_match = re.search(r"F(\d+)", full_row_text)
                        if f_match: row[f'f{i}'] = int(f_match.group(1))
                        
                        st_matches = re.findall(r"(\.\d{2}|0\.\d{2})", full_row_text)
                        if st_matches:
                            for st_val in st_matches:
                                v = float(st_val)
                                if 0.0 < v < 0.5:
                                    row[f'st{i}'] = v
                                    break
                        
                        wr_matches = re.findall(r"(\d\.\d{2})", full_row_text)
                        for val in wr_matches:
                            v = float(val)
                            if 1.0 <= v <= 9.99:
                                row[f'wr{i}'] = v
                                break
                        
                        mo_matches = re.findall(r"(\d{2}\.\d{2})", full_row_text)
                        if mo_matches:
                            row[f'mo{i}'] = float(mo_matches[0])
                except: pass
        
        return row
    except: return None

def process_wrapper(args):
    session, jcd, rno, date_str = args
    # 並列数が多いので、サーバーへのアクセス集中を避けるためわずかに待機
    time.sleep(random.uniform(0.1, 0.4))
    try:
        return scrape_race_data(session, jcd, rno, date_str)
    except:
        return None

def show_progress(processed, total):
    bar_len = 30
    filled = int(bar_len * processed / total)
    bar = "=" * filled + "-" * (bar_len - filled)
    percent = 100 * processed / total
    print(f"\r⏳ [{bar}] {percent:.1f}% ({processed}/{total})", end="")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    today = datetime.now().strftime("%Y-%m-%d")
    
    parser.add_argument("--start", help="開始日 (YYYY-MM-DD)")
    parser.add_argument("--end", help="終了日 (YYYY-MM-DD)")
    parser.add_argument("--year", type=int, help="指定した年全体を収集")
    
    args = parser.parse_args()

    if args.year:
        start_d = datetime(args.year, 1, 1)
        end_d = datetime(args.year, 12, 31)
    else:
        s_str = args.start if args.start else yesterday
        e_str = args.end if args.end else today
        try:
            start_d = datetime.strptime(s_str, "%Y-%m-%d")
            end_d = datetime.strptime(e_str, "%Y-%m-%d")
        except ValueError:
            print("❌ 日付エラー: YYYY-MM-DD 形式で指定してください。")
            sys.exit(1)

    if start_d > end_d:
        print("❌ エラー: 開始日が終了日より後になっています。")
        sys.exit(1)

    session = get_session()
    current = start_d
    
    safe_print(f"🚀 収集開始: {start_d.strftime('%Y-%m-%d')} 〜 {end_d.strftime('%Y-%m-%d')}")
    safe_print(f"⚡ 並列スレッド数: {MAX_WORKERS}")
    
    os.makedirs("data", exist_ok=True)
    filename = f"data/race_data_{start_d.strftime('%Y%m%d')}_{end_d.strftime('%Y%m%d')}.csv"
    
    csv_columns = get_column_names()

    # ファイルがなければヘッダーを作成
    if not os.path.exists(filename):
        pd.DataFrame(columns=csv_columns).to_csv(filename, index=False)

    total_races = 0
    
    while current <= end_d:
        d_str = current.strftime("%Y%m%d")
        safe_print(f"📅 {current.strftime('%Y-%m-%d')} のデータを収集中...")
        
        tasks = []
        for jcd in range(1, 25):
            for rno in range(1, 13):
                tasks.append((session, jcd, rno, d_str))
        
        task_total = len(tasks)
        processed = 0
        results = []
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_task = {executor.submit(process_wrapper, t): t for t in tasks}
            
            for future in as_completed(future_to_task):
                processed += 1
                show_progress(processed, task_total)
                
                try:
                    res = future.result()
                    if res: results.append(res)
                except: pass
        
        print("") 
        
        if results:
            df = pd.DataFrame(results)
            
            # カラムが存在しない場合NaNで埋めて、順序を統一する
            df = df.reindex(columns=csv_columns)
            
            # 追記モード
            df.to_csv(filename, mode='a', index=False, header=False)
            safe_print(f"  ✅ {len(df)}レース 保存しました")
            total_races += len(df)
        else:
            safe_print(f"  ⚠️ データなし (開催なし or エラー)")
        
        current += timedelta(days=1)
    
    safe_print("="*40)
    safe_print(f"🎉 すべて完了しました！")
    safe_print(f"📁 保存ファイル: {filename}")
    safe_print(f"📊 合計取得数: {total_races} レース")
    safe_print("="*40)
