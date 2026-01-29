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
MAX_WORKERS = 20  # 並列数
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
    cols = ['date', 'jcd', 'rno', 'wind', 'res1', 'rank1', 'rank2', 'rank3', 
            'tansho', 'nirentan', 'sanrentan', 'sanrenpuku', 'payout']
    # 選手ID (pid) を追加
    for i in range(1, 7):
        cols.extend([f'pid{i}', f'wr{i}', f'mo{i}', f'ex{i}', f'f{i}', f'st{i}'])
    return cols

def get_session():
    session = requests.Session()
    retries = Retry(
        total=MAX_RETRIES,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"]
    )
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
        # 配当テーブルをより厳密に検索
        for tbl in soup.select("table.is-w495"):
            if key_text in tbl.text:
                rows = tbl.select("tr")
                for tr in rows:
                    if key_text in tr.text:
                        # 金額は最後のtdに入っていることが多い
                        tds = tr.select("td")
                        if not tds: continue
                        txt = clean_text(tds[-1].text)
                        if txt.isdigit():
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

        # 天候・風 (beforeinfoから取得)
        row['wind'] = 0.0
        if soup_before:
            try:
                # 天候情報のコンテナを探す
                weather_div = soup_before.select_one(".weather1_body")
                if weather_div:
                    wind_elem = weather_div.select_one(".weather1_bodyUnitLabelData")
                    if wind_elem:
                        w_txt = clean_text(wind_elem.text)
                        m = re.search(r"(\d+)", w_txt)
                        row['wind'] = float(m.group(1)) if m else 0.0
            except: pass

        # 順位
        row['rank1'], row['rank2'], row['rank3'] = None, None, None
        try:
            result_rows = soup_res.select("table.is-w495 tbody tr")
            for idx, r_key in enumerate(['rank1', 'rank2', 'rank3']):
                if len(result_rows) > idx:
                    # 着順の数字を取得
                    rank_td = result_rows[idx].select("td")
                    if len(rank_td) >= 2:
                        r_txt = clean_text(rank_td[1].text) # 艇番
                        if r_txt.isdigit():
                            row[r_key] = int(r_txt)
        except: pass
        
        row['res1'] = 1 if row.get('rank1') == 1 else 0

        # 払い戻し
        row['tansho'] = extract_payout(soup_res, "単勝")
        row['nirentan'] = extract_payout(soup_res, "2連単")
        row['sanrentan'] = extract_payout(soup_res, "3連単")
        row['sanrenpuku'] = extract_payout(soup_res, "3連複")
        row['payout'] = row['sanrentan']

        # 各艇データ取得
        for i in range(1, 7):
            # 初期化
            row[f'pid{i}'] = 0     # 選手ID
            row[f'wr{i}'] = 0.0    # 勝率
            row[f'mo{i}'] = 0.0    # モーター
            row[f'ex{i}'] = 0.0    # 展示タイム
            row[f'f{i}'] = 0       # フライング
            row[f'st{i}'] = 0.20   # 平均ST

            # 1. 出走表(racelist)から ID, 勝率, 平均ST, F数 を取得
            if soup_list:
                try:
                    # 枠番ごとのtbodyを取得 (is-fs12クラスを持つtbody)
                    tbodies = soup_list.select("tbody.is-fs12")
                    if len(tbodies) >= i:
                        tbody = tbodies[i-1] # 枠番に対応するtbody
                        
                        # --- 選手ID (登番) ---
                        # <div class="is-fs11">4320 ... </div>
                        toban_div = tbody.select_one("div.is-fs11")
                        if toban_div:
                            toban_txt = clean_text(toban_div.text)[:4]
                            if toban_txt.isdigit():
                                row[f'pid{i}'] = int(toban_txt)

                        # --- 勝率, モーター, 平均ST ---
                        # tdタグを全取得してインデックスで指定するのが確実
                        tds = tbody.select("td")
                        
                        # 全国勝率 (通常インデックス4あたり)
                        # HTML構造: 級別 | 全国勝率 | 当地勝率 ...
                        # is-lineH2 クラスのセルなどを探す
                        
                        # テキスト全体から勝率っぽい「X.XX」を抽出する正規表現アプローチ
                        full_text = tbody.text
                        
                        # 勝率 (1.00 - 9.99)
                        wr_match = re.search(r"(\d\.\d{2})", full_text) # 最初にヒットするのが全国勝率の可能性が高い
                        if wr_match:
                            # 厳密には td[4] を指定すべきだが、サイト構造変化に強い正規表現で補完
                            if len(tds) > 4:
                                wr_txt = clean_text(tds[4].text)
                                m = re.search(r"(\d\.\d{2})", wr_txt)
                                if m: row[f'wr{i}'] = float(m.group(1))

                        # モーター (td[6] or td[7])
                        if len(tds) > 6:
                            mo_txt = clean_text(tds[6].text) # 2連対率
                            m = re.search(r"(\d{2}\.\d{2})", mo_txt)
                            if m: row[f'mo{i}'] = float(m.group(1))
                            
                            # もしここになければ次のセルを確認
                            if row[f'mo{i}'] == 0.0 and len(tds) > 7:
                                mo_txt = clean_text(tds[7].text)
                                m = re.search(r"(\d{2}\.\d{2})", mo_txt)
                                if m: row[f'mo{i}'] = float(m.group(1))

                        # 平均ST (0.XX)
                        st_match = re.search(r"(0\.\d{2})", full_text)
                        if st_match:
                            row[f'st{i}'] = float(st_match.group(1))
                        
                        # F数 (F1, F2...)
                        f_match = re.search(r"F(\d+)", full_text)
                        if f_match:
                            row[f'f{i}'] = int(f_match.group(1))
                            
                except: pass

            # 2. 直前情報(beforeinfo)から 展示タイム を取得
            if soup_before:
                try:
                    # is-boatColor1 ~ 6 のクラスを持つtdを探す
                    boat_td = soup_before.select_one(f"td.is-boatColor{i}")
                    if boat_td:
                        # その行(tr)を取得
                        tr = boat_td.find_parent("tr")
                        tds = tr.select("td")
                        # 展示タイムは通常後ろの方にある (td[4]以降)
                        # 値が "6.XX" のような形式を探す
                        for td in tds[4:]:
                            val = clean_text(td.text)
                            if re.match(r"^\d\.\d{2}$", val):
                                # 6.50 ~ 7.00 くらいの値が展示タイム
                                if 6.0 <= float(val) <= 7.5:
                                    row[f'ex{i}'] = float(val)
                                    break
                except: pass

        return row
    except: return None

def process_wrapper(args):
    session, jcd, rno, date_str = args
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
            df = df.reindex(columns=csv_columns)
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
