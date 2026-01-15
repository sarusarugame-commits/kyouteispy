import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import re
import os
import unicodedata
from datetime import datetime

# --- 設定 ---
TARGET_DATE = "20250101" # デバッグ用にデータが存在する日付を指定
MAX_RACES = 5            # 5レース取ったら終了
OUTPUT_FILE = "debug_data.csv"

def clean_text(text):
    """テキストの正規化（全角→半角、空白削除）"""
    if not text: return ""
    text = unicodedata.normalize('NFKC', str(text))
    return text.replace("\n", "").replace("\r", "").replace(" ", "").replace("¥", "").replace(",", "").strip()

def get_soup(url):
    """HTML取得（簡易リトライ付き）"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    for _ in range(3):
        try:
            res = requests.get(url, headers=headers, timeout=10)
            res.encoding = res.apparent_encoding
            if res.status_code == 200:
                # "データがありません" ページ対策
                if "データがありません" in res.text:
                    return None
                return BeautifulSoup(res.text, 'html.parser')
            time.sleep(1)
        except:
            time.sleep(1)
    return None

def scrape_race(jcd, rno, date_str):
    """1レース分の詳細データを取得"""
    base_url = "https://www.boatrace.jp/owpc/pc/race"
    
    # 1. 直前情報（展示タイム、風速）
    url_before = f"{base_url}/beforeinfo?rno={rno}&jcd={jcd:02d}&hd={date_str}"
    soup_before = get_soup(url_before)
    if not soup_before: return None

    # 2. 結果（着順、配当）
    url_res = f"{base_url}/raceresult?rno={rno}&jcd={jcd:02d}&hd={date_str}"
    soup_res = get_soup(url_res)
    if not soup_res: return None

    # 3. 番組表（F数、ST、勝率、モーター）
    url_list = f"{base_url}/racelist?rno={rno}&jcd={jcd:02d}&hd={date_str}"
    soup_list = get_soup(url_list)
    if not soup_list: return None

    try:
        row = {'date': date_str, 'jcd': jcd, 'rno': rno}

        # --- 【修正】風速の取得 ---
        try:
            wind_elem = soup_before.select_one(".weather1_bodyUnitLabelData")
            if wind_elem:
                row['wind'] = float(clean_text(wind_elem.text).replace("m", ""))
            else:
                row['wind'] = 0.0
        except: row['wind'] = 0.0

        # --- 【修正】着順と配当の取得 ---
        # 着順
        ranks = [None] * 6
        try:
            # 順位テーブル（通常のHTML構造を想定）
            rank_rows = soup_res.select("table.is-w495 tbody tr")
            for r in rank_rows:
                tds = r.select("td")
                if len(tds) > 1:
                    rank_idx = clean_text(tds[0].text) # 着順 "1", "2"...
                    boat_num = clean_text(tds[1].text) # 艇番
                    if rank_idx.isdigit() and int(rank_idx) <= 3:
                        # rank1, rank2, rank3 に艇番を入れる
                        row[f'rank{rank_idx}'] = int(boat_num)
        except: pass

        # 3連単配当
        row['payout'] = 0
        try:
            # "3連単" という文字を含むセルを探す
            payout_th = soup_res.find(lambda tag: tag.name == "th" and "3連単" in tag.text)
            if payout_th:
                # その行の、"¥1,230" が入っているセルを探す
                # 構造： tr > th(3連単) + td(組番) + td(払戻金) + ...
                payout_td = payout_th.find_next_sibling("td").find_next_sibling("td")
                if payout_td:
                    payout_val = clean_text(payout_td.text)
                    if payout_val.isdigit():
                        row['payout'] = int(payout_val)
        except: pass

        # 旧バージョンとの互換用 (1号艇が1着なら1)
        row['res1'] = 1 if row.get('rank1') == 1 else 0

        # --- 【修正】各艇データの取得（F数、ST、モーター） ---
        for i in range(1, 7):
            try:
                # 直前情報から展示タイム
                boat_cell = soup_before.select_one(f".is-boatColor{i}")
                if boat_cell:
                    tds_before = boat_cell.find_parent("tbody").select("td")
                    ex_val = clean_text(tds_before[4].text)
                    row[f'ex{i}'] = float(ex_val) if ex_val and ex_val != "." else 0.0
                else:
                    row[f'ex{i}'] = 0.0

                # 番組表から詳細データ
                list_tbody = soup_list.select_one(f".is-boatColor{i}").find_parent("tbody")
                tds_list = list_tbody.select("td")

                # 勝率 (tds[3]付近)
                txt_wr = clean_text(tds_list[3].text)
                wr_match = re.search(r"(\d\.\d{2})", txt_wr)
                row[f'wr{i}'] = float(wr_match.group(1)) if wr_match else 0.0

                # ★F数（選手名欄 tds[2] から抽出）
                txt_name = clean_text(tds_list[2].text)
                f_match = re.search(r"F(\d+)", txt_name)
                row[f'f{i}'] = int(f_match.group(1)) if f_match else 0

                # ★平均ST (tds[3] から "ST0.xx" を探す)
                st_match = re.search(r"ST(\d\.\d{2})", txt_wr) # 勝率と同じセルにある場合が多い
                if not st_match:
                    # 見つからなければセル内全テキストから探す
                    st_match = re.search(r"ST(\d\.\d{2})", list_tbody.text)
                row[f'st{i}'] = float(st_match.group(1)) if st_match else 0.17

                # ★モーター2連率 (tds[6] or tds[5] の "%" を探す)
                txt_motor = clean_text(tds_list[5].text) # 多くの場合はここ
                mo_match = re.search(r"(\d{1,3}\.\d)", txt_motor) # 35.5 のような形
                if not mo_match:
                    txt_motor = clean_text(tds_list[6].text) # 念のため隣も
                    mo_match = re.search(r"(\d{1,3}\.\d)", txt_motor)
                
                # これで "250.0" みたいな変な数字ではなく "35.5" が入るはず
                row[f'mo{i}'] = float(mo_match.group(1)) if mo_match else 0.0

            except Exception as e:
                # エラー時は安全値を埋める
                row[f'wr{i}'] = 0.0
                row[f'ex{i}'] = 0.0
                row[f'f{i}'] = 0
                row[f'st{i}'] = 0.20
                row[f'mo{i}'] = 0.0
        
        return row

    except Exception as e:
        print(f"Error scraping {jcd}場 {rno}R: {e}")
        return None

if __name__ == "__main__":
    print(f"🚀 デバッグ収集開始: {TARGET_DATE} から {MAX_RACES}レース分")
    
    collected_data = []
    count = 0
    
    # 全24場、全12Rを巡回（5つ取れたら終了）
    for jcd in range(1, 25):
        if count >= MAX_RACES: break
        
        for rno in range(1, 13):
            if count >= MAX_RACES: break
            
            print(f"  🔍 {jcd}場 {rno}R を確認中...", end="")
            data = scrape_race(jcd, rno, TARGET_DATE)
            
            if data:
                print(" ✅ 取得成功")
                # ちゃんとデータが入っているか簡易チェック表示
                print(f"     -> 1着:{data.get('rank1')} / 配当:¥{data.get('payout')} / 1号艇F:{data.get('f1')} / 1号艇ST:{data.get('st1')} / モーター:{data.get('mo1')}%")
                collected_data.append(data)
                count += 1
                time.sleep(1) # サーバー負荷軽減
            else:
                print(" ❌ データなし or エラー")

    # CSV保存
    if collected_data:
        df = pd.DataFrame(collected_data)
        
        # カラム順序を綺麗にする
        cols = ['date', 'jcd', 'rno', 'wind', 'res1', 'rank1', 'rank2', 'rank3', 'payout']
        for i in range(1, 7):
            cols.extend([f'wr{i}', f'mo{i}', f'ex{i}', f'f{i}', f'st{i}'])
        
        # 実際にデータフレームにあるカラムだけ選んで並べ替え
        existing_cols = [c for c in cols if c in df.columns]
        df = df[existing_cols]

        df.to_csv(OUTPUT_FILE, index=False)
        print(f"\n🎉 完了！ {len(df)}件のデータを {OUTPUT_FILE} に保存しました。")
        print("中身を確認して、'payout' や 'f1' が正しく取れているかチェックしてください。")
    else:
        print("\n⚠️ データが1件も取得できませんでした。日付やネットワークを確認してください。")
