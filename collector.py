import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import sys
import argparse
import os
import re
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

# ログを即時表示（GitHub Actions用）
sys.stdout.reconfigure(line_buffering=True)
print_lock = threading.Lock()

def safe_print(msg):
    with print_lock:
        print(msg)

# ==========================================
# ⚙️ 設定エリア
# ==========================================
MAX_RETRIES = 5       # リトライ回数
RETRY_INTERVAL = 5    # 通常リトライ時の待機時間
BAN_WAIT_TIME = 20    # ⛔ BAN/アクセス制限検知時の待機時間
MAX_WORKERS = 2       # 安全のため「2」推奨（増やしすぎると診断ログでエラーが埋め尽くされます）

def get_session():
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
    if not text: return ""
    return text.replace("\n", "").replace("\r", "").replace(" ", "").replace("\u3000", "").strip()

def get_soup_diagnostic(session, url, check_selector=None):
    """
    HTMLを取得し、内容を診断して返す関数
    Returns: (soup, error_message)
    - 成功時: (soup_object, None)
    - 失敗時: (None, "エラー詳細メッセージ")
    """
    last_error = ""
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            res = session.get(url, timeout=30)
            res.encoding = res.apparent_encoding
            
            if res.status_code == 200:
                soup = BeautifulSoup(res.text, 'html.parser')
                
                # チェック要素（例：.is-boatColor1）があるか確認
                if check_selector:
                    if not soup.select_one(check_selector):
                        # 200 OK だが中身が違う（エラーページ等）
                        page_title = clean_text(soup.title.text) if soup.title else "No Title"
                        body_sample = clean_text(soup.body.text)[:50] if soup.body else "No Body"
                        
                        err_msg = f"⛔ 解析失敗（中身が不正） Title:【{page_title}】 Text: {body_sample}..."
                        
                        # アクセス制限系なら待機
                        if "アクセス" in page_title or "Error" in page_title:
                            safe_print(f"   🛡️ ブロック検知。{BAN_WAIT_TIME}秒待機します...")
                            time.sleep(BAN_WAIT_TIME * attempt)
                        
                        last_error = err_msg
                        continue # リトライへ
                
                # 正常
                return soup, None
            
            else:
                last_error = f"HttpError: {res.status_code}"
                
        except Exception as e:
            last_error = f"ConnectionError: {e}"
            
        time.sleep(RETRY_INTERVAL)
    
    return None, last_error

def scrape_race_data(session, jcd, rno, date_str):
    base_url = "https://www.boatrace.jp/owpc/pc/race"
    log_prefix = f"{date_str} J{jcd:02} R{rno:02}"
    
    # 1. 直前情報（ここに一番重要なデータが多いので最初にチェック）
    soup_before, err = get_soup_diagnostic(
        session, 
        f"{base_url}/beforeinfo?rno={rno}&jcd={jcd:02d}&hd={date_str}",
        check_selector=".is-boatColor1" # これがないと話にならない
    )
    
    if not soup_before:
        # 失敗ログ（ここで「なぜダメだったか」が出る）
        safe_print(f"❌ {log_prefix}: 直前情報取得失敗 -> {err}")
        return None

    # 2. 番組表
    soup_list, err = get_soup_diagnostic(session, f"{base_url}/racelist?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    if not soup_list:
        safe_print(f"❌ {log_prefix}: 番組表取得失敗 -> {err}")
        return None

    # 3. 結果
    soup_res, err = get_soup_diagnostic(session, f"{base_url}/raceresult?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    if not soup_res:
        safe_print(f"❌ {log_prefix}: 結果取得失敗 -> {err}")
        return None

    try:
        # --- データ抽出ロジック ---
        
        # 風速
        wind = 0.0
        try:
            wind_elem = soup_before.find(string=re.compile("風速"))
            if wind_elem:
                parent = wind_elem.find_parent(class_="weather1_bodyUnit")
                if parent:
                    data_elem = parent.select_one(".weather1_bodyUnitLabelData")
                    if data_elem:
                        wind = float(clean_text(data_elem.text).replace("m", ""))
        except: pass 

        # 1着フラグ
        res1 = 0
        try:
            res_rows = soup_res.select(".is-p_1-1")
            if res_rows:
                rank1_boat = clean_text(res_rows[0].select("td")[1].text)
                if rank1_boat == "1":
                    res1 = 1
        except: pass

        # 展示タイム
        temp_ex_times = []
        for i in range(1, 7):
            boat_cell = soup_before.select_one(f".is-boatColor{i}")
            if not boat_cell:
                # 事前チェックを通っているのでここは起きにくいはず
                safe_print(f"⚠️ {log_prefix}: 構造エラー（{i}号艇が見つかりません）")
                return None

            tbody = boat_cell.find_parent("tbody")
            tds = tbody.select("td")
            
            # 展示タイム取得（列ズレ対応）
            ex_val = clean_text(tds[4].text)
            if not ex_val: ex_val = clean_text(tds[5].text)

            val_float = 0.0
            if ex_val and ex_val not in ["-", "0.00", "\xa0"]:
                try:
                    val_float = float(ex_val)
                except: pass
            temp_ex_times.append(val_float)

        # データ格納
        row = {'date': date_str, 'jcd': jcd, 'rno': rno, 'wind': wind, 'res1': res1}
        
        for i in range(1, 7):
            try:
                boat_cell_list = soup_list.select_one(f".is-boatColor{i}")
                if boat_cell_list:
                    tbody_list = boat_cell_list.find_parent("tbody")
                    tds_list = tbody_list.select("td")
                    
                    row[f'wr{i}'] = float(re.findall(r"\d+\.\d+", tds_list[3].text)[0])
                    nums = re.findall(r"\d+\.\d+", tds_list[6].text)
                    row[f'mo{i}'] = float(nums[0]) if nums else 0.0
                else:
                    row[f'wr{i}'], row[f'mo{i}'] = 0.0, 0.0
            except:
                row[f'wr{i}'], row[f'mo{i}'] = 0.0, 0.0

            row[f'ex{i}'] = temp_ex_times[i-1]

        safe_print(f"✅ {log_prefix}: 完了")
        return row

    except Exception as e:
        safe_print(f"💥 {log_prefix}: データ抽出中にエラー {e}")
        return None

def process_race_parallel(args):
    # BAN対策のスリープ
    time.sleep(1.0)
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

    print(f"🚀 本番データ収集（診断ログ付）開始: {args.start} 〜 {args.end}")
    
    results = []
    
    while current <= end_d:
        d_str = current.strftime("%Y%m%d")
        print(f"\n--- 📅 {d_str} 処理中 ---")
        
        tasks = []
        for jcd in range(1, 25):
            for rno in range(1, 13):
                tasks.append((session, jcd, rno, d_str))
        
        day_results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = executor.map(process_race_parallel, tasks)
            for res in futures:
                if res:
                    day_results.append(res)
        
        print(f"📊 {d_str}: {len(day_results)}レース取得")
        results.extend(day_results)
        current += timedelta(days=1)

    if results:
        df = pd.DataFrame(results)
        filename = f"data/chunk_{args.start}.csv"
        df.to_csv(filename, index=False)
        print(f"\n🎉 全工程完了！CSV保存: {filename} ({len(df)}行)")
    else:
        print("\n⚠️ データが取得できませんでした。ログを確認してください。")
