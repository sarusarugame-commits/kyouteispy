import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import sys
import argparse
import os
import re
import threading
import unicodedata
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

# ログ設定
sys.stdout.reconfigure(line_buffering=True)
print_lock = threading.Lock()

def safe_print(msg):
    with print_lock:
        print(msg)

# ==========================================
# ⚙️ 設定エリア
# ==========================================
MAX_RETRIES = 3       # リトライ回数
RETRY_INTERVAL = 2    # リトライ待機時間(秒)
MAX_WORKERS = 8       # 並列数（GitHub Actionsなら8-10推奨）

def get_session():
    """リトライ機能付きのセッションを作成"""
    session = requests.Session()
    adapter = HTTPAdapter(
        pool_connections=MAX_WORKERS,
        pool_maxsize=MAX_WORKERS,
        max_retries=Retry(total=MAX_RETRIES, backoff_factor=1)
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    })
    return session

def clean_text(text):
    """テキスト正規化（全角→半角、カンマ・円マーク削除）"""
    if not text: return ""
    text = unicodedata.normalize('NFKC', str(text))
    return text.replace("\n", "").replace("\r", "").replace(" ", "").replace("¥", "").replace(",", "").strip()

def get_soup_diagnostic(session, url, check_selector=None):
    """HTML取得＆診断（開催なし判定付き）"""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            res = session.get(url, timeout=15)
            res.encoding = res.apparent_encoding
            
            if res.status_code == 200:
                # 開催なし判定
                if "データがありません" in res.text:
                    return None, "SKIP"

                soup = BeautifulSoup(res.text, 'html.parser')
                
                # 特定の要素（レース情報など）があるかチェック
                if check_selector and not soup.select_one(check_selector):
                    time.sleep(RETRY_INTERVAL)
                    continue 
                return soup, None
        except:
            time.sleep(RETRY_INTERVAL)
            
    return None, "ERROR"

def scrape_race_data(session, jcd, rno, date_str):
    """1レース分の詳細データを取得（修正版）"""
    base_url = "https://www.boatrace.jp/owpc/pc/race"
    
    # 1. 直前情報（まずこれで開催有無をチェック）
    soup_before, err = get_soup_diagnostic(
        session, 
        f"{base_url}/beforeinfo?rno={rno}&jcd={jcd:02d}&hd={date_str}",
        check_selector=".is-boatColor1"
    )
    if err == "SKIP" or not soup_before:
        return None

    # 2. 結果（着順、配当）
    soup_res, err = get_soup_diagnostic(session, f"{base_url}/raceresult?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    if not soup_res: return None

    # 3. 番組表（F数、ST、勝率、モーター）
    soup_list, err = get_soup_diagnostic(session, f"{base_url}/racelist?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    if not soup_list: return None

    try:
        row = {'date': date_str, 'jcd': jcd, 'rno': rno}

        # --- ① 風速 ---
        try:
            wind_elem = soup_before.select_one(".weather1_bodyUnitLabelData")
            if wind_elem:
                row['wind'] = float(clean_text(wind_elem.text).replace("m", ""))
            else:
                row['wind'] = 0.0
        except: row['wind'] = 0.0

        # --- ② 着順 (rank1~3) ---
        # 初期値
        row['rank1'], row['rank2'], row['rank3'] = None, None, None
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

        # 旧バージョン互換用 (1号艇が1着なら1)
        row['res1'] = 1 if row.get('rank1') == 1 else 0

        # --- ③ 3連単配当 (payout) ---
        row['payout'] = 0
        try:
            # "3連単"を含むthを探す
            payout_th = soup_res.find(lambda tag: tag.name == "th" and "3連単" in tag.text)
            if payout_th:
                # 隣の隣のセルが払戻金
                payout_td = payout_th.find_next_sibling("td").find_next_sibling("td")
                if payout_td:
                    val = clean_text(payout_td.text)
                    if val.isdigit():
                        row['payout'] = int(val)
        except: pass

        # --- ④ 各艇データ (F数, ST, モーター等) ---
        for i in range(1, 7):
            # [直前情報] 展示タイム
            try:
                boat_cell = soup_before.select_one(f".is-boatColor{i}")
                if boat_cell:
                    tds = boat_cell.find_parent("tbody").select("td")
                    ex_val = clean_text(tds[4].text)
                    row[f'ex{i}'] = float(ex_val) if ex_val and ex_val != "." else 0.0
                else: row[f'ex{i}'] = 0.0
            except: row[f'ex{i}'] = 0.0

            # [番組表] 詳細データ
            try:
                list_tbody = soup_list.select_one(f".is-boatColor{i}").find_parent("tbody")
                tds = list_tbody.select("td")
                
                # 全国勝率 (tds[3])
                wr_match = re.search(r"(\d\.\d{2})", clean_text(tds[3].text))
                row[f'wr{i}'] = float(wr_match.group(1)) if wr_match else 0.0
                
                # F数 (選手名欄 tds[2] から "F1" 等を抽出)
                f_match = re.search(r"F(\d+)", clean_text(tds[2].text))
                row[f'f{i}'] = int(f_match.group(1)) if f_match else 0
                
                # 平均ST (tds[3] または行全体から "ST0.15" を探す)
                st_match = re.search(r"ST(\d\.\d{2})", list_tbody.text.replace("\n", ""))
                row[f'st{i}'] = float(st_match.group(1)) if st_match else 0.17
                
                # モーター2連率 (tds[5] または tds[6] から "%" のついた数字を抽出)
                mo_text = clean_text(tds[5].text) # 通常はここ
                mo_match = re.search(r"(\d{1,3}\.\d)", mo_text)
                if not mo_match:
                    mo_text = clean_text(tds[6].text) # 念のため隣もチェック
                    mo_match = re.search(r"(\d{1,3}\.\d)", mo_text)
                row[f'mo{i}'] = float(mo_match.group(1)) if mo_match else 0.0
                
            except:
                # エラー時の安全値
                row[f'wr{i}'], row[f'f{i}'], row[f'st{i}'], row[f'mo{i}'] = 0.0, 0, 0.20, 0.0

        return row

    except Exception as e:
        return None

def process_race_parallel(args):
    """並列処理用ラッパー"""
    time.sleep(0.1) # サーバー負荷軽減のため微小ウェイト
    return scrape_race_data(*args)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    args = parser.parse_args()

    os.makedirs("data", exist_ok=True)
    session = get_session()
    
    start_d = datetime.strptime(args.start, "%Y-%m-%d")
    end_d = datetime.strptime(args.end, "%Y-%m-%d")
    current = start_d
    
    print(f"🚀 収集開始: {args.start} 〜 {args.end}")
    
    # 逐次保存用ファイル名
    filename = f"data/chunk_{args.start.replace('-','')}.csv"
    file_exists = False
    
    while current <= end_d:
        d_str = current.strftime("%Y%m%d")
        print(f"📅 {d_str} 処理中...")
        
        # 1日分の全レースタスク作成
        tasks = [(session, jcd, rno, d_str) for jcd in range(1, 25) for rno in range(1, 13)]
        
        day_results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for res in executor.map(process_race_parallel, tasks):
                if res: day_results.append(res)
        
        if day_results:
            df = pd.DataFrame(day_results)
            
            # カラム順序を整理（見やすくする）
            cols = ['date', 'jcd', 'rno', 'wind', 'res1', 'rank1', 'rank2', 'rank3', 'payout']
            for i in range(1, 7):
                cols.extend([f'wr{i}', f'mo{i}', f'ex{i}', f'f{i}', f'st{i}'])
            
            # 存在するカラムだけ抽出
            use_cols = [c for c in cols if c in df.columns]
            df = df[use_cols]

            # 追記保存
            df.to_csv(filename, mode='a', index=False, header=not file_exists)
            file_exists = True
            safe_print(f"  ✅ {len(day_results)}レース 保存完了")
        else:
            safe_print("  ⚠️ データなし")
        
        current += timedelta(days=1)

    print("🎉 全期間完了")
