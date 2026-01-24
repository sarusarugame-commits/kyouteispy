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
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

# ==========================================
# ⚙️ 設定エリア
# ==========================================
MAX_WORKERS = 16       # 16並列で攻める
MAX_RETRIES = 5        # 粘り強くリトライ
RETRY_DELAY = 3        # リトライ時の待機秒数

# 偽装用User-Agent
UA_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0"
]

# ログ排他制御用
print_lock = threading.Lock()

def safe_print(msg):
    with print_lock:
        print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

def clean_text(text):
    if not text: return ""
    text = unicodedata.normalize('NFKC', str(text))
    return text.replace("\n", "").replace("\r", "").replace("¥", "").replace(",", "").strip()

def get_session():
    """リトライ機能付きセッション"""
    session = requests.Session()
    retries = Retry(total=MAX_RETRIES, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS, max_retries=retries)
    session.mount("https://", adapter)
    return session

def get_soup(session, url, description="ページ"):
    for i in range(MAX_RETRIES):
        try:
            headers = {'User-Agent': random.choice(UA_LIST)}
            res = session.get(url, headers=headers, timeout=30)
            res.encoding = res.apparent_encoding
            
            if res.status_code == 200:
                if "データがありません" in res.text: return None, "SKIP"
                return BeautifulSoup(res.text, 'html.parser'), None
            time.sleep(random.uniform(1, 3))
        except:
            time.sleep(RETRY_DELAY)
    return None, "ERROR"

def extract_payout(soup, key_text):
    """強力な配当検索ロジック"""
    try:
        tables = soup.select("table")
        for tbl in tables:
            if key_text in tbl.text:
                rows = tbl.select("tr")
                for tr in rows:
                    if key_text in tr.text:
                        tds = tr.select("td")
                        for td in tds:
                            txt = clean_text(td.text)
                            if txt.isdigit() and len(txt) >= 2 and "-" not in txt:
                                return int(txt)
    except: pass
    return 0

def scrape_race_data(session, jcd, rno, date_str):
    base_url = "https://www.boatrace.jp/owpc/pc/race"
    
    # 3ページ取得（サーバー負荷を考慮しつつ確実に）
    soup_before, err = get_soup(session, f"{base_url}/beforeinfo?rno={rno}&jcd={jcd:02d}&hd={date_str}", "直前")
    if err == "SKIP" or not soup_before: return None

    soup_res, err = get_soup(session, f"{base_url}/raceresult?rno={rno}&jcd={jcd:02d}&hd={date_str}", "結果")
    if not soup_res: return None

    soup_list, err = get_soup(session, f"{base_url}/racelist?rno={rno}&jcd={jcd:02d}&hd={date_str}", "番組")
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

        # --- ③ 配当（3連単をpayoutとして使用） ---
        row['tansho'] = extract_payout(soup_res, "単勝")
        row['nirentan'] = extract_payout(soup_res, "2連単")
        row['sanrentan'] = extract_payout(soup_res, "3連単")
        row['sanrenpuku'] = extract_payout(soup_res, "3連複")
        row['payout'] = row['sanrentan'] 

        # --- ④ 各艇データ ---
        for i in range(1, 7):
            # -------------------------------------------------------
            # [A] 直前情報 (beforeinfo) から取得: 展示タイム, モーター勝率
            # -------------------------------------------------------
            try:
                boat_cell = soup_before.select_one(f".is-boatColor{i}")
                if boat_cell:
                    tbody = boat_cell.find_parent("tbody")
                    tds = tbody.select("td")
                    
                    # 展示タイム (通常は右端の方にある)
                    # tdの中身を走査して "6.xx" のような形式を探す方が安全だが、配置固定と仮定
                    ex_val = clean_text(tds[-1].text).replace(" ", "") # 一番右
                    if not re.match(r"\d\.\d{2}", ex_val):
                         ex_val = clean_text(tds[4].text).replace(" ", "") # 念のためインデックス指定も試行
                    row[f'ex{i}'] = float(ex_val) if ex_val and ex_val != "." else 0.0

                    # モーター勝率 (2連率)
                    # "No.xx xx.x%" という形式のセルを探す
                    row[f'mo{i}'] = 0.0
                    for td in tds:
                        txt = clean_text(td.text)
                        # "%" が含まれていて数字がある場合
                        if "%" in txt:
                            mo_match = re.search(r"(\d{1,2}\.\d)", txt)
                            if mo_match:
                                row[f'mo{i}'] = float(mo_match.group(1))
                                break
                else:
                    row[f'ex{i}'] = 0.0
                    row[f'mo{i}'] = 0.0
            except:
                row[f'ex{i}'] = 0.0
                row[f'mo{i}'] = 0.0

            # -------------------------------------------------------
            # [B] 番組表 (racelist) から取得: 選手勝率, F数, ST
            # -------------------------------------------------------
            try:
                list_node = soup_list.select_one(f".is-boatColor{i}")
                if list_node:
                    list_tbody = list_node.find_parent("tbody")
                    row_text = clean_text(list_tbody.text)
                    tds = list_tbody.select("td")
                    
                    # 全国勝率 (x.xx という形式を探す)
                    # 通常 tds[3] あたりだが、行全体から正規表現で探す
                    wr_match = re.search(r"(\d\.\d{2})", clean_text(tds[3].text))
                    row[f'wr{i}'] = float(wr_match.group(1)) if wr_match else 0.0
                    
                    # フライング(F)
                    # 行全体から "F1", "F2" などを探す
                    f_match = re.search(r"F(\d+)", row_text)
                    row[f'f{i}'] = int(f_match.group(1)) if f_match else 0
                    
                    # 平均ST
                    # 行全体から "ST0.15" のような形式を探す
                    st_match = re.search(r"ST(\d\.\d{2})", row_text.replace(" ", ""))
                    row[f'st{i}'] = float(st_match.group(1)) if st_match else 0.17
                else:
                    raise Exception("No Data")
                
            except:
                row[f'wr{i}'] = 0.0
                row[f'f{i}'] = 0
                row[f'st{i}'] = 0.17
        
        return row
    except: return None

def process_wrapper(args):
    """並列実行用ラッパー"""
    session, jcd, rno, date_str = args
    # 少しランダムに待機して、アクセスパターンを分散させる
    time.sleep(random.uniform(0.5, 2.0))
    return scrape_race_data(session, jcd, rno, date_str)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    args = parser.parse_args()

    # セッション作成（コネクションプール有効化）
    session = get_session()
    
    start_d = datetime.strptime(args.start, "%Y-%m-%d")
    end_d = datetime.strptime(args.end, "%Y-%m-%d")
    current = start_d
    
    safe_print(f"🚀 収集開始: {args.start} 〜 {args.end} (並列数: {MAX_WORKERS})")
    
    # 保存ディレクトリ
    os.makedirs("data", exist_ok=True)
    
    # ファイル名を決定
    filename = f"data/data_{args.start.replace('-','')}_{args.end.replace('-','')}.csv"
    file_exists = os.path.exists(filename)

    while current <= end_d:
        d_str = current.strftime("%Y%m%d")
        safe_print(f"📅 {d_str} 処理中...")
        
        # 1日分のタスクリスト作成
        tasks = []
        for jcd in range(1, 25):
            for rno in range(1, 13):
                tasks.append((session, jcd, rno, d_str))
        
        results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for res in executor.map(process_wrapper, tasks):
                if res: results.append(res)
        
        # 1日ごとに保存
        if results:
            df = pd.DataFrame(results)
            # カラム順序
            cols = ['date', 'jcd', 'rno', 'wind', 'res1', 'rank1', 'rank2', 'rank3', 
                    'tansho', 'nirentan', 'sanrentan', 'sanrenpuku', 'payout']
            for i in range(1, 7):
                cols.extend([f'wr{i}', f'mo{i}', f'ex{i}', f'f{i}', f'st{i}'])
            
            # 存在するカラムのみで構成
            use_cols = [c for c in cols if c in df.columns]
            df = df[use_cols]
            
            # 追記モードで保存
            df.to_csv(filename, mode='a', index=False, header=not file_exists)
            file_exists = True
            safe_print(f"  ✅ {len(df)}レース 保存完了")
        
        current += timedelta(days=1)
    
    safe_print(f"🎉 完了！データは {filename} に保存されました")
