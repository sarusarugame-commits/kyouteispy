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

# 偽装用User-Agentリスト
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
    # 余分な空白を削除するが、数字がくっつかないように注意
    return text.replace("\n", "").replace("\r", "").replace("¥", "").replace(",", "").strip()

def get_soup(url, description="ページ"):
    for i in range(1, 4):
        try:
            headers = {'User-Agent': random.choice(UA_LIST)}
            res = requests.get(url, headers=headers, timeout=30)
            res.encoding = res.apparent_encoding
            
            if res.status_code == 200:
                if "データがありません" in res.text:
                    log(f"     ⚠️ {description}: データなし")
                    return None
                return BeautifulSoup(res.text, 'html.parser')
            time.sleep(3)
        except Exception as e:
            log(f"     ❌ {description}: {e} (Wait 3s...)")
            time.sleep(3)
    log(f"     💀 {description}: 取得失敗")
    return None

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
        log(f"  -> データの抽出・解析中...")
        row = {'date': date_str, 'jcd': jcd, 'rno': rno}

        # --- ① 風速 ---
        try:
            wind_elem = soup_before.select_one(".weather1_bodyUnitLabelData")
            row['wind'] = float(clean_text(wind_elem.text).replace("m", "").replace(" ", "")) if wind_elem else 0.0
        except: row['wind'] = 0.0

        # --- ② 着順 (rank1, rank2, rank3) ---
        # ★修正: 艇番がくっつく問題に対処
        row['rank1'], row['rank2'], row['rank3'] = None, None, None
        try:
            # 着順テーブルを取得（通常ページ上部）
            rank_rows = soup_res.select("table.is-w495 tbody tr")
            for r in rank_rows:
                tds = r.select("td")
                if len(tds) > 1:
                    # 着順
                    rank_idx = clean_text(tds[0].text).replace(" ", "")
                    
                    # 艇番（ここが重要：数字だけを厳密に抽出）
                    boat_text = clean_text(tds[1].text)
                    boat_match = re.search(r"^(\d{1})", boat_text) # 先頭の1桁だけを取る
                    
                    if rank_idx.isdigit() and int(rank_idx) <= 3 and boat_match:
                        row[f'rank{rank_idx}'] = int(boat_match.group(1))
        except Exception as e:
            log(f"     ⚠️ 着順解析エラー: {e}")

        row['res1'] = 1 if row.get('rank1') == 1 else 0

        # --- ③ 3連単配当 (payout) ---
        # ★修正: 検索ロジックを柔軟に
        row['payout'] = 0
        try:
            # ページ内の全テーブルを走査して「3連単」を探す
            found_payout = False
            tables = soup_res.select("table")
            for tbl in tables:
                if "3連単" in tbl.text:
                    # このテーブルの中に配当があるはず
                    rows = tbl.select("tr")
                    for tr in rows:
                        # ヘッダー(th)かセル(td)に "3連単" がある行を探す
                        if "3連単" in tr.text:
                            tds = tr.select("td")
                            # [組番, 払戻金, 人気] の並びが多いが、場合によるので金額っぽいものを探す
                            for td in tds:
                                txt = clean_text(td.text)
                                # 数字のみ（金額）で、組番（1-2-3など）ではないものを探す
                                if txt.isdigit() and len(txt) > 1 and "-" not in txt:
                                    row['payout'] = int(txt)
                                    found_payout = True
                                    break
                        if found_payout: break
                if found_payout: break
            
            if not found_payout:
                 log("     ⚠️ 配当が見つかりませんでした")
        except Exception as e:
            log(f"     ⚠️ 配当取得エラー: {e}")

        # --- ④ 各艇データ ---
        for i in range(1, 7):
            try:
                # 展示タイム
                boat_cell = soup_before.select_one(f".is-boatColor{i}")
                if boat_cell:
                    tds = boat_cell.find_parent("tbody").select("td")
                    ex_val = clean_text(tds[4].text).replace(" ", "")
                    row[f'ex{i}'] = float(ex_val) if ex_val and ex_val != "." else 0.0
                else: row[f'ex{i}'] = 0.0

                # 詳細データ
                list_tbody = soup_list.select_one(f".is-boatColor{i}").find_parent("tbody")
                tds = list_tbody.select("td")
                
                wr_match = re.search(r"(\d\.\d{2})", clean_text(tds[3].text))
                row[f'wr{i}'] = float(wr_match.group(1)) if wr_match else 0.0
                
                f_match = re.search(r"F(\d+)", clean_text(tds[2].text))
                row[f'f{i}'] = int(f_match.group(1)) if f_match else 0
                
                # 平均ST
                st_match = re.search(r"ST(\d\.\d{2})", list_tbody.text.replace("\n", "").replace(" ", ""))
                row[f'st{i}'] = float(st_match.group(1)) if st_match else 0.17
                
                # モーター
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
    
    for jcd in range(1, 25):
        if len(collected_data) >= MAX_RACES: break
        
        for rno in range(1, 13):
            if len(collected_data) >= MAX_RACES: break
            
            data = scrape_race(jcd, rno, target_date)
            if data:
                collected_data.append(data)
                time.sleep(3) 
            
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
