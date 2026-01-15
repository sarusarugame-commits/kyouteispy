import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import re
import os
import unicodedata
import sys
import argparse

# ==========================================
# ⚙️ 設定エリア
# ==========================================
DEFAULT_TARGET_DATE = "20250101" 
MAX_RACES = 5 

def log(msg):
    """ログを即時出力するための関数（flush=True必須）"""
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

def clean_text(text):
    if not text: return ""
    text = unicodedata.normalize('NFKC', str(text))
    return text.replace("\n", "").replace("\r", "").replace(" ", "").replace("¥", "").replace(",", "").strip()

def get_soup(url, description="ページ"):
    """HTML取得（詳細ログ付き）"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    for i in range(1, 4): # 3回リトライ
        try:
            # log(f"  -> {description} にアクセス中 (試行{i}/3)...")
            res = requests.get(url, headers=headers, timeout=10)
            res.encoding = res.apparent_encoding
            
            if res.status_code == 200:
                if "データがありません" in res.text:
                    log(f"     ⚠️ {description}: データなし")
                    return None
                return BeautifulSoup(res.text, 'html.parser')
            else:
                log(f"     ⚠️ {description}: ステータスコード異常 {res.status_code}")
                time.sleep(1)
        except Exception as e:
            log(f"     ❌ {description}: エラー {e} (リトライします)")
            time.sleep(1)
            
    log(f"     💀 {description}: 取得失敗（ギブアップ）")
    return None

def scrape_race(jcd, rno, date_str):
    """1レース分の詳細データを取得"""
    log(f"🏁 【{jcd}場 {rno}R】 データ収集開始")
    base_url = "https://www.boatrace.jp/owpc/pc/race"
    
    # URL生成
    url_before = f"{base_url}/beforeinfo?rno={rno}&jcd={jcd:02d}&hd={date_str}"
    url_res = f"{base_url}/raceresult?rno={rno}&jcd={jcd:02d}&hd={date_str}"
    url_list = f"{base_url}/racelist?rno={rno}&jcd={jcd:02d}&hd={date_str}"
    
    # ページ取得（一つずつ確認）
    soup_before = get_soup(url_before, "直前情報")
    if not soup_before: return None

    soup_res = get_soup(url_res, "レース結果")
    if not soup_res: return None

    soup_list = get_soup(url_list, "番組表")
    if not soup_list: return None

    try:
        log(f"  -> データの抽出・解析中...")
        row = {'date': date_str, 'jcd': jcd, 'rno': rno}

        # --- ① 風速 ---
        try:
            wind_elem = soup_before.select_one(".weather1_bodyUnitLabelData")
            row['wind'] = float(clean_text(wind_elem.text).replace("m", "")) if wind_elem else 0.0
        except: row['wind'] = 0.0

        # --- ② 着順 (rank1, rank2, rank3) ---
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
        
        row['res1'] = 1 if row.get('rank1') == 1 else 0

        # --- ③ 3連単配当 (payout) ---
        row['payout'] = 0
        try:
            payout_th = soup_res.find(lambda tag: tag.name == "th" and "3連単" in tag.text)
            if payout_th:
                payout_td = payout_th.find_next_sibling("td").find_next_sibling("td")
                if payout_td:
                    val = clean_text(payout_td.text)
                    if val.isdigit():
                        row['payout'] = int(val)
        except: pass

        # --- ④ 各艇データ ---
        for i in range(1, 7):
            try:
                # 展示タイム
                boat_cell = soup_before.select_one(f".is-boatColor{i}")
                if boat_cell:
                    tds = boat_cell.find_parent("tbody").select("td")
                    ex_val = clean_text(tds[4].text)
                    row[f'ex{i}'] = float(ex_val) if ex_val and ex_val != "." else 0.0
                else: row[f'ex{i}'] = 0.0

                # 詳細データ
                list_tbody = soup_list.select_one(f".is-boatColor{i}").find_parent("tbody")
                tds = list_tbody.select("td")
                
                wr_match = re.search(r"(\d\.\d{2})", clean_text(tds[3].text))
                row[f'wr{i}'] = float(wr_match.group(1)) if wr_match else 0.0
                
                f_match = re.search(r"F(\d+)", clean_text(tds[2].text))
                row[f'f{i}'] = int(f_match.group(1)) if f_match else 0
                
                st_match = re.search(r"ST(\d\.\d{2})", list_tbody.text.replace("\n", ""))
                row[f'st{i}'] = float(st_match.group(1)) if st_match else 0.17
                
                mo_text = clean_text(tds[5].text)
                mo_match = re.search(r"(\d{1,3}\.\d)", mo_text)
                if not mo_match:
                    mo_text = clean_text(tds[6].text)
                    mo_match = re.search(r"(\d{1,3}\.\d)", mo_text)
                row[f'mo{i}'] = float(mo_match.group(1)) if mo_match else 0.0
                
            except:
                row[f'wr{i}'], row[f'f{i}'], row[f'st{i}'], row[f'mo{i}'] = 0.0, 0, 0.20, 0.0

        log(f"  ✅ 取得成功 (1着:{row.get('rank1')} / 配当:¥{row.get('payout')})")
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
    log("==================================================")
    
    collected_data = []
    
    # 24場×12Rを巡回
    for jcd in range(1, 25):
        if len(collected_data) >= MAX_RACES: break
        
        for rno in range(1, 13):
            if len(collected_data) >= MAX_RACES: break
            
            data = scrape_race(jcd, rno, target_date)
            if data:
                collected_data.append(data)
                time.sleep(1) # 負荷軽減
            
    # CSV保存
    if collected_data:
        log("==================================================")
        os.makedirs("data", exist_ok=True)
        df = pd.DataFrame(collected_data)
        
        cols = ['date', 'jcd', 'rno', 'wind', 'res1', 'rank1', 'rank2', 'rank3', 'payout']
        for i in range(1, 7):
            cols.extend([f'wr{i}', f'mo{i}', f'ex{i}', f'f{i}', f'st{i}'])
        
        use_cols = [c for c in cols if c in df.columns]
        df = df[use_cols]

        output_path = "data/debug_result.csv"
        df.to_csv(output_path, index=False)
        log(f"🎉 完了！ {len(df)}件のデータを保存しました: {output_path}")
    else:
        log("⚠️ データが1件も取得できませんでした。")
        sys.exit(1)
