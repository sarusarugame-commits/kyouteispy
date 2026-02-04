import pandas as pd
from curl_cffi import requests
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
import time
import re
import os
import unicodedata
import argparse
import random
import threading
import sys
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from itertools import permutations
import warnings

# ==========================================
# ⚙️ 設定エリア
# ==========================================
MAX_WORKERS = 15       # 並列数
TIMEOUT_SEC = 20       # タイムアウト
MAX_RETRIES = 5        # 最大リトライ回数
RETRY_DELAY = 3        # リトライ待機秒数

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
print_lock = threading.Lock()

def safe_print(msg):
    with print_lock:
        timestamp = time.strftime("%H:%M:%S")
        print(f"[{timestamp}] {msg}", flush=True)

def clean_text(text):
    if not text: return ""
    text = unicodedata.normalize("NFKC", str(text))
    return text.replace("\n", "").replace("\r", "").replace("¥", "").replace(",", "").strip()

def get_column_names():
    cols = [
        "date", "jcd", "rno", "wind", "res1", 
        "rank1", "rank2", "rank3", 
        "tansho", "nirentan", "sanrentan", "sanrenpuku", "payout",
    ]
    for i in range(1, 7):
        cols.extend([f"pid{i}", f"wr{i}", f"mo{i}", f"ex{i}", f"f{i}", f"st{i}"])
    for i, j, k in permutations(range(1, 7), 3):
        cols.append(f"odds_{i}-{j}-{k}")
    # 2連単
    for i, j in permutations(range(1, 7), 2):
        cols.append(f"odds_2t_{i}-{j}")
    return cols

def get_session():
    return requests.Session(impersonate="chrome120")

def get_soup(session, url):
    for i in range(MAX_RETRIES):
        try:
            res = session.get(url, timeout=TIMEOUT_SEC)
            if res.status_code == 200:
                res.encoding = res.charset if res.charset else 'utf-8'
                return BeautifulSoup(res.content, "lxml"), None
            if res.status_code == 404:
                return None, "ERROR"
            time.sleep(random.uniform(1, 2))
        except Exception:
            time.sleep(RETRY_DELAY)
    return None, "ERROR"

def extract_payout(soup, key_text):
    if not soup: return 0
    try:
        for tbl in soup.select("table.is-w495"):
            if key_text in tbl.text:
                rows = tbl.select("tr")
                for tr in rows:
                    if key_text in tr.text:
                        tds = tr.select("td")
                        if not tds: continue
                        for td in reversed(tds):
                            txt = clean_text(td.text)
                            if txt.isdigit():
                                val = int(txt)
                                if val >= 100: return val
    except: pass
    return 0

def get_odds_2t_map(session, jcd, rno, date_str):
    """2連単の全オッズを取得 (テーブル構造解析版)"""
    url = f"https://www.boatrace.jp/owpc/pc/race/odds2tf?rno={rno}&jcd={jcd:02d}&hd={date_str}"
    soup, err = get_soup(session, url)
    
    odds_map = {f"odds_2t_{i}-{j}": 0.0 for i, j in permutations(range(1, 7), 2)}
    if not soup: return odds_map

    # div.table1 内のテーブルを探す
    tables = soup.select("div.table1 table")
    
    for tbl in tables:
        rows = tbl.select("tbody tr")
        if not rows: continue
        
        for tr in rows:
            tds = tr.select("td")
            if len(tds) < 12: continue
            
            for i in range(6): # 1号艇〜6号艇
                try:
                    idx_boat = i * 2
                    idx_odds = i * 2 + 1
                    
                    boat_2nd_txt = clean_text(tds[idx_boat].text)
                    odds_txt = clean_text(tds[idx_odds].text)
                    
                    if re.match(r"^[1-6]$", boat_2nd_txt):
                        try:
                            val = float(odds_txt)
                            boat_1st = i + 1
                            key = f"odds_2t_{boat_1st}-{boat_2nd_txt}"
                            odds_map[key] = val
                        except: pass
                except IndexError: pass
                
    return odds_map

def get_odds_map(session, jcd, rno, date_str):
    url = f"https://www.boatrace.jp/owpc/pc/race/odds3t?rno={rno}&jcd={jcd:02d}&hd={date_str}"
    soup, err = get_soup(session, url)
    
    odds_map = {f"odds_{i}-{j}-{k}": 0.0 for i, j, k in permutations(range(1, 7), 3)}
    if not soup: return odds_map

    target_tbody = None
    tables = soup.select("div.table1 table")
    for tbl in tables:
        if tbl.select(".oddsPoint"):
            tbody = tbl.select_one("tbody")
            if tbody:
                target_tbody = tbody
                break
    
    if not target_tbody: return odds_map
        
    rows = target_tbody.select("tr")
    rowspan_counters = [0] * 6
    current_2nd_boats = [0] * 6

    for tr in rows:
        tds = tr.select("td")
        col_cursor = 0
        for block_idx in range(6):
            if col_cursor >= len(tds): break
            current_1st = block_idx + 1 
            if rowspan_counters[block_idx] > 0:
                if col_cursor + 1 >= len(tds): break
                val_2nd = current_2nd_boats[block_idx]
                txt_3rd = clean_text(tds[col_cursor].text)
                txt_odds = clean_text(tds[col_cursor+1].text)
                rowspan_counters[block_idx] -= 1
                col_cursor += 2
            else:
                if col_cursor + 2 >= len(tds): break
                td_2nd = tds[col_cursor]
                txt_2nd = clean_text(td_2nd.text)
                rs = int(td_2nd.get("rowspan", 1))
                rowspan_counters[block_idx] = rs - 1
                try: val_2nd = int(txt_2nd)
                except: val_2nd = 0
                current_2nd_boats[block_idx] = val_2nd
                txt_3rd = clean_text(tds[col_cursor+1].text)
                txt_odds = clean_text(tds[col_cursor+2].text)
                col_cursor += 3
            try:
                if val_2nd > 0 and txt_3rd.isdigit():
                    odds_val = float(txt_odds) if txt_odds.replace('.','').isdigit() else 0.0
                    key = f"odds_{current_1st}-{val_2nd}-{txt_3rd}"
                    odds_map[key] = odds_val
            except: continue
    return odds_map

def scrape_race_data(session, jcd, rno, date_str):
    base_url = "https://www.boatrace.jp/owpc/pc/race"
    url_res = f"{base_url}/raceresult?rno={rno}&jcd={jcd:02d}&hd={date_str}"
    
    soup_res, err = get_soup(session, url_res)
    if not soup_res or not soup_res.select("table.is-w495"):
        return None # 開催なし

    url_bef = f"{base_url}/beforeinfo?rno={rno}&jcd={jcd:02d}&hd={date_str}"
    url_lst = f"{base_url}/racelist?rno={rno}&jcd={jcd:02d}&hd={date_str}"

    soup_before, _ = get_soup(session, url_bef)
    soup_list, _ = get_soup(session, url_lst)

    try:
        row = {"date": date_str, "jcd": jcd, "rno": rno}
        
        row["wind"] = 0.0
        if soup_before:
            try:
                wind_unit = soup_before.select_one(".is-windDirection")
                if wind_unit:
                    wind_data = wind_unit.select_one(".weather1_bodyUnitLabelData")
                    if wind_data:
                        m = re.search(r"(\d+)", clean_text(wind_data.text))
                        row["wind"] = float(m.group(1)) if m else 0.0
            except: pass

        row["rank1"], row["rank2"], row["rank3"] = None, None, None
        try:
            result_rows = soup_res.select("table.is-w495 tbody tr")
            for idx, r_key in enumerate(["rank1", "rank2", "rank3"]):
                if len(result_rows) > idx:
                    rank_td = result_rows[idx].select("td")
                    if len(rank_td) >= 2:
                        r_txt = clean_text(rank_td[1].text)
                        if r_txt.isdigit(): row[r_key] = int(r_txt)
        except: pass
        row["res1"] = 1 if row.get("rank1") == 1 else 0

        row["tansho"] = extract_payout(soup_res, "単勝")
        row["nirentan"] = extract_payout(soup_res, "2連単")
        row["sanrentan"] = extract_payout(soup_res, "3連単")
        row["sanrenpuku"] = extract_payout(soup_res, "3連複")
        row["payout"] = row["sanrentan"]

        odds_data = get_odds_map(session, jcd, rno, date_str)
        row.update(odds_data)
        
        # 2連単取得ロジック
        odds_2t_data = get_odds_2t_map(session, jcd, rno, date_str)
        row.update(odds_2t_data)

        for i in range(1, 7):
            row[f"pid{i}"] = 0; row[f"wr{i}"] = 0.0; row[f"mo{i}"] = 0.0
            row[f"ex{i}"] = 0.0; row[f"f{i}"] = 0; row[f"st{i}"] = 0.20

            if soup_list:
                try:
                    tbodies = soup_list.select("tbody.is-fs12")
                    if len(tbodies) >= i:
                        tbody = tbodies[i - 1]
                        txt_all = clean_text(tbody.text)
                        pid_match = re.search(r"([2-5]\d{3})", txt_all)
                        if pid_match: row[f"pid{i}"] = int(pid_match.group(1))
                        tds = tbody.select("td")
                        full_txt = " ".join([clean_text(td.text) for td in tds])
                        m_wr = re.search(r"(\d\.\d{2})", clean_text(tds[2].text) if len(tds)>2 else "")
                        if m_wr and 1.0 <= float(m_wr.group(1)) <= 9.99:
                            row[f"wr{i}"] = float(m_wr.group(1))
                        mo_matches = re.findall(r"(\d{2}\.\d{2})", full_txt)
                        if mo_matches:
                            for m_val in mo_matches:
                                if 10.0 <= float(m_val) <= 99.9:
                                    row[f"mo{i}"] = float(m_val)
                                    break
                        st_match = re.search(r"(0\.\d{2})", full_txt)
                        if st_match: row[f"st{i}"] = float(st_match.group(1))
                        f_match = re.search(r"F(\d+)", full_txt)
                        if f_match: row[f"f{i}"] = int(f_match.group(1))
                except: pass

            if soup_before:
                try:
                    boat_td = soup_before.select_one(f"td.is-boatColor{i}")
                    if boat_td:
                        tr = boat_td.find_parent("tr")
                        if tr:
                            for td in tr.select("td")[4:]:
                                val = clean_text(td.text)
                                if re.match(r"^\d\.\d{2}$", val):
                                    if 6.0 <= float(val) <= 7.5:
                                        row[f"ex{i}"] = float(val); break
                except: pass
        return row
    except: return None

def process_wrapper(args):
    session, jcd, rno, date_str = args
    time.sleep(random.uniform(0.5, 1.5))
    try:
        res = scrape_race_data(session, jcd, rno, date_str)
        return res
    except Exception as e:
        safe_print(f"❌ [ERROR] {date_str} 場:{jcd:02} R:{rno:02} -> {e}")
        return None

def show_progress(processed, total):
    bar_len = 30
    filled = int(bar_len * processed / total)
    bar = "=" * filled + "-" * (bar_len - filled)
    percent = 100 * processed / total
    print(f"\r⏳ [{bar}] {percent:.1f}% ({processed}/{total})", end="")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", help="YYYY-MM-DD")
    parser.add_argument("--end", help="YYYY-MM-DD")
    parser.add_argument("--year", type=int)
    parser.add_argument("--year_range", type=int, nargs=2)
    args = parser.parse_args()

    if args.year_range:
        start_y, end_y = args.year_range
        print(f"🔄 自動リレー収集: {start_y}年 〜 {end_y}年")
        start_d = datetime(start_y, 1, 1)
        end_d = datetime(start_y, 12, 31)
    else:
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        if args.year:
            start_d = datetime(args.year, 1, 1)
            end_d = datetime(args.year, 12, 31)
        else:
            default_start = "2025-01-01"
            s_str = args.start if args.start else default_start
            e_str = args.end if args.end else yesterday
            try:
                start_d = datetime.strptime(s_str, "%Y-%m-%d")
                end_d = datetime.strptime(e_str, "%Y-%m-%d")
            except ValueError:
                print("❌ 日付エラー")
                sys.exit(1)

    session = get_session()
    current = start_d
    safe_print(f"🚀 収集開始: {start_d.strftime('%Y-%m-%d')} 〜 {end_d.strftime('%Y-%m-%d')}")
    safe_print(f"⚡ 並列数: {MAX_WORKERS}")
    os.makedirs("data", exist_ok=True)
    filename = f"data/race_data_odds_{start_d.strftime('%Y%m%d')}_{end_d.strftime('%Y%m%d')}.csv"
    csv_columns = get_column_names()

    if not os.path.exists(filename):
        pd.DataFrame(columns=csv_columns).to_csv(filename, index=False)

    while current <= end_d:
        d_str = current.strftime("%Y%m%d")
        safe_print(f"📅 {current.strftime('%Y-%m-%d')} ...")
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
            df = df.reindex(columns=csv_columns)
            df.to_csv(filename, mode="a", index=False, header=False)
            safe_print(f"  ✅ {len(df)}レース 保存")
        else:
            safe_print(f"  ⚠️ データなし")
        current += timedelta(days=1)

    safe_print("=" * 40)
    safe_print(f"🎉 完了: {filename}")
    if args.year_range:
        next_year = args.year_range[0] + 1
        if next_year <= args.year_range[1]:
            print(f"\n🚀 {next_year}年へ自動リレー\n")
            time.sleep(3)
            new_args = [sys.executable, sys.argv[0], "--year_range", str(next_year), str(args.year_range[1])]
            if sys.platform == 'win32':
                subprocess.Popen(new_args)
                sys.exit(0)
            else:
                os.execv(sys.executable, new_args)
