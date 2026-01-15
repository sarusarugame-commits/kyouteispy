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
# デバッグ用のデフォルト日付（引数がない場合に使用）
DEFAULT_TARGET_DATE = "20250101" 
# 何レース取ったら終了するか
MAX_RACES = 5 

def clean_text(text):
    """テキスト正規化（全角→半角、カンマ・円マーク削除）"""
    if not text: return ""
    text = unicodedata.normalize('NFKC', str(text))
    return text.replace("\n", "").replace("\r", "").replace(" ", "").replace("¥", "").replace(",", "").strip()

def get_soup(url):
    """HTML取得（簡易リトライ付き）"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    for _ in range(3): # 3回リトライ
        try:
            res = requests.get(url, headers=headers, timeout=10)
            res.encoding = res.apparent_encoding
            if res.status_code == 200:
                if "データがありません" in res.text: return None
                return BeautifulSoup(res.text, 'html.parser')
            time.sleep(1)
        except:
            time.sleep(1)
    return None

def scrape_race(jcd, rno, date_str):
    """1レース分の詳細データを取得"""
    print(f"[{jcd}場 {rno}R] 取得中...", end="")
    base_url = "https://www.boatrace.jp/owpc/pc/race"
    
    # URL生成
    url_before = f"{base_url}/beforeinfo?rno={rno}&jcd={jcd:02d}&hd={date_str}"
    url_res = f"{base_url}/raceresult?rno={rno}&jcd={jcd:02d}&hd={date_str}"
    url_list = f"{base_url}/racelist?rno={rno}&jcd={jcd:02d}&hd={date_str}"
    
    # ページ取得
    soup_before = get_soup(url_before)
    soup_res = get_soup(url_res)
    soup_list = get_soup(url_list)
    
    if not (soup_before and soup_res and soup_list):
        print(" スキップ（ページ取得不可）")
        return None

    try:
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
        
        # 旧バージョン互換用 (1号艇が1着なら1)
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
                
                # 勝率
                wr_match = re.search(r"(\d\.\d{2})", clean_text(tds[3].text))
                row[f'wr{i}'] = float(wr_match.group(1)) if wr_match else 0.0
                
                # F数 (選手名欄 tds[2] から "F1" 等を抽出)
                f_match = re.search(r"F(\d+)", clean_text(tds[2].text))
                row[f'f{i}'] = int(f_match.group(1)) if f_match else 0
                
                # 平均ST (行全体から "ST0.15" を探す)
                st_match = re.search(r"ST(\d\.\d{2})", list_tbody.text.replace("\n", ""))
                row[f'st{i}'] = float(st_match.group(1)) if st_match else 0.17
                
                # モーター2連率 (tds[5] or tds[6] から "%" のついた数字を抽出)
                mo_text = clean_text(tds[5].text) # 通常はここ
                mo_match = re.search(r"(\d{1,3}\.\d)", mo_text)
                if not mo_match:
                    mo_text = clean_text(tds[6].text) # 念のため隣もチェック
                    mo_match = re.search(r"(\d{1,3}\.\d)", mo_text)
                row[f'mo{i}'] = float(mo_match.group(1)) if mo_match else 0.0
                
            except:
                # エラー時の安全値
                row[f'wr{i}'], row[f'f{i}'], row[f'st{i}'], row[f'mo{i}'] = 0.0, 0, 0.20, 0.0

        print(" ✅ OK")
        return row

    except Exception as e:
        print(f" ❌ Error: {e}")
        return None

if __name__ == "__main__":
    # 引数があればそれを使用、なければデフォルト設定
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default=DEFAULT_TARGET_DATE, help="取得開始日 (YYYYMMDD)")
    # end引数は互換性のために残すが、このモードでは使わない
    parser.add_argument("--end", default=None, help="終了日 (無視されます)") 
    args = parser.parse_args()

    target_date = args.start.replace("-", "")
    print(f"🚀 DEBUG START: {target_date} (Limit: {MAX_RACES} races)")
    
    collected_data = []
    
    # 24場×12Rを巡回
    for jcd in range(1, 25):
        if len(collected_data) >= MAX_RACES: break
        
        for rno in range(1, 13):
            if len(collected_data) >= MAX_RACES: break
            
            data = scrape_race(jcd, rno, target_date)
            if data:
                # ログ確認用
                print(f"   -> Result: 1着={data.get('rank1')} / 配当:¥{data.get('payout')}")
                print(f"   -> 1号艇: F{data.get('f1')} / ST{data.get('st1')} / Mo{data.get('mo1')}%")
                collected_data.append(data)
                time.sleep(1) # 負荷軽減
            
    # CSV保存
    if collected_data:
        os.makedirs("data", exist_ok=True)
        df = pd.DataFrame(collected_data)
        
        # カラム順序を整理
        cols = ['date', 'jcd', 'rno', 'wind', 'res1', 'rank1', 'rank2', 'rank3', 'payout']
        for i in range(1, 7):
            cols.extend([f'wr{i}', f'mo{i}', f'ex{i}', f'f{i}', f'st{i}'])
        
        # 存在するカラムだけ抽出
        use_cols = [c for c in cols if c in df.columns]
        df = df[use_cols]

        output_path = "data/debug_result.csv"
        df.to_csv(output_path, index=False)
        print(f"\n🎉 完了！ {len(df)}件のデータを {output_path} に保存しました。")
    else:
        print("\n⚠️ データが取得できませんでした。日付や開催を確認してください。")
        sys.exit(1)
