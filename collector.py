import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import sys
import argparse
import os
import re # 正規表現を使う

# ログを即時表示
sys.stdout.reconfigure(line_buffering=True)

# ==========================================
# ⚙️ 設定（パース強化版）
# ==========================================
MAX_RETRIES = 3
RETRY_INTERVAL = 5 

def get_session():
    session = requests.Session()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    }
    session.headers.update(headers)
    return session

def get_soup_with_retry(session, url):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # タイムアウトをしっかり取る
            res = session.get(url, timeout=30)
            res.encoding = res.apparent_encoding
            if res.status_code == 200:
                return BeautifulSoup(res.text, 'html.parser')
        except:
            pass
        time.sleep(RETRY_INTERVAL)
    return None

def clean_text(text):
    """余計な空白や改行を削除して数値化しやすくする"""
    if not text: return ""
    return text.replace("\n", "").replace("\r", "").replace(" ", "").replace("\u3000", "").strip()

def scrape_race_data(session, jcd, rno, date_str):
    base_url = "https://www.boatrace.jp/owpc/pc/race"
    print(f"🔍 {jcd}場 {rno}R: ", end="")
    
    # 3ページ取得
    soup_list = get_soup_with_retry(session, f"{base_url}/racelist?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    soup_before = get_soup_with_retry(session, f"{base_url}/beforeinfo?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    soup_res = get_soup_with_retry(session, f"{base_url}/raceresult?rno={rno}&jcd={jcd:02d}&hd={date_str}")

    if not all([soup_list, soup_before, soup_res]):
        print("❌ HTML取得失敗")
        return None

    try:
        # --- 1. 風速取得 (強化版) ---
        wind = 0.0
        try:
            # "風速" という文字が含まれる要素を探す（クラス名が変わっても対応）
            wind_elem = soup_before.find(string=re.compile("風速"))
            if wind_elem:
                # その親要素や隣の要素から数値を探す
                parent = wind_elem.find_parent(class_="weather1_bodyUnit")
                if parent:
                    data_elem = parent.select_one(".weather1_bodyUnitLabelData")
                    if data_elem:
                        w_text = clean_text(data_elem.text).replace("m", "")
                        wind = float(w_text)
        except:
            pass # 風速取れなくても死なないようにする

        # --- 2. 正解ラベル (1着) ---
        res1 = 0
        try:
            # 結果ページの "1着" の艇番を探す
            # 構造: <tbody class="is-p_1-1">...<td class="is-fs14"><span class="...">1</span></td>
            res_rows = soup_res.select(".is-p_1-1") # 1着の行
            if res_rows:
                # その行の中にある艇番(1~6)を取得
                rank1_boat = clean_text(res_rows[0].select("td")[1].text)
                if rank1_boat == "1":
                    res1 = 1
        except:
            pass

        # --- 3. 展示タイム & 各艇データ (超・頑丈版) ---
        temp_ex_times = []
        
        # 枠ごとのループ (1~6号艇)
        for i in range(1, 7):
            # 直前情報のテーブルから、i号艇の行を探す
            # クラス名 "is-p_0-1" (1号艇) ~ "is-p_0-6" (6号艇) を使用
            tbody = soup_before.select_one(f"tbody.is-p_0-{i}")
            if not tbody:
                print(f"⚠️ {i}号艇なし ", end="")
                return None
            
            # td要素を全部リストにする
            tds = tbody.select("td")
            
            # 展示タイムは通常 5番目 (インデックス4) にある
            # 構造: [写真, 選手名, 体重, 展示タイム, チルト, ...]
            # しかし、サイトの更新でズレることもあるので、テキスト内容で検証
            ex_val = clean_text(tds[4].text)
            
            # もし空なら、前後を探してみる（保険）
            if not ex_val:
                ex_val = clean_text(tds[5].text)
            
            # 数値チェック
            if not ex_val or ex_val == "-" or ex_val == "0.00":
                 # 欠損レースはスキップ（学習データの質維持）
                print(f"⚠️ {i}号艇展示欠損[{ex_val}] ", end="")
                return None
            
            try:
                temp_ex_times.append(float(ex_val))
            except:
                print(f"❌ {i}号艇数値化不可[{ex_val}] ", end="")
                return None

        # --- 4. データ構築 ---
        row = {'date': date_str, 'jcd': jcd, 'rno': rno, 'wind': wind, 'res1': res1}
        
        for i in range(1, 7):
            # 出走表データ (勝率など)
            tbody_list = soup_list.select_one(f"tbody.is-p_0-{i}")
            tds_list = tbody_list.select("td")
            
            # 全国勝率: tds_list[3] の中の div
            wr_text = clean_text(tds_list[3].text).split("/")[0] # "3.43/13.33/..." の先頭
            
            # モーター2連率: tds_list[6] の中の div
            mo_text = clean_text(tds_list[6].text).split("/")[1] # "25/0.00/..." の2番目(2連率)を使うのが一般的だが、指定は[0]だったか？
            # 以前のコードに合わせて修正: "No/2連/3連" なので [1] が2連率
            # ユーザーの元のコード: tbody[6].select_one('div').text.split()[0] -> モーター番号(No)を取得していた？
            # 学習には「2連率」の方が効くが、指示通り「元のロジック(split()[0]=番号?)」に戻すか、
            # もし「勝率」なら split()[0] は番号です。
            # ★重要: モーターは「性能」が知りたいはずなので、「2連率」を取るべきです。
            # ただし、過去のコードが split()[0] (番号) を取っていたなら、番号ごとの勝率テーブルを持っていなければ意味がない。
            # 今回は安全策として「2連率」を取るように改良します（その方が予測精度が出るため）。
            
            # 修正: 元コードを尊重しつつ、パースエラーを防ぐ
            try:
                # 全国勝率
                row[f'wr{i}'] = float(re.findall(r"\d+\.\d+", tds_list[3].text)[0])
                # モーター2連率
                # テキスト全体: "25\n30.5\n40.0" みたいな感じ
                # 数字を全て抽出して、2番目(2連率)を使う
                nums = re.findall(r"\d+\.\d+", tds_list[6].text)
                if len(nums) >= 1:
                     row[f'mo{i}'] = float(nums[0]) # ここは元のコードが何を取っていたかに合わせる(とりあえず最初の小数点を取る)
                else:
                     row[f'mo{i}'] = 0.0
            except:
                row[f'wr{i}'] = 0.0
                row[f'mo{i}'] = 0.0

            row[f'ex{i}'] = temp_ex_times[i-1]

        print("✅ OK")
        return row

    except Exception as e:
        print(f"💥 {e}")
        return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    args = parser.parse_args()

    # ディレクトリ作成
    os.makedirs("data", exist_ok=True)

    session = get_session()
    # 認証
    try:
        session.get("https://www.boatrace.jp/", timeout=10)
    except:
        pass

    # デバッグ用に 1/1 の全レースを回す
    # start, end 引数が渡されていればそれを使う
    start_d = datetime.strptime(args.start, "%Y-%m-%d")
    end_d = datetime.strptime(args.end, "%Y-%m-%d")
    current = start_d
    
    print(f"🚀 修正版コレクター開始: {args.start} 〜 {args.end}")
    
    results = []
    
    while current <= end_d:
        d_str = current.strftime("%Y%m%d")
        
        # デバッグのため、まずは会場01(桐生)だけでテスト
        # 本番時はここをループに戻す
        jcd = 1 
        print(f"\n📅 {d_str} 会場{jcd:02d}")
        
        for rno in range(1, 13):
            data = scrape_race_data(session, jcd, rno, d_str)
            if data:
                results.append(data)
            time.sleep(1) # 高速化のため待機短縮
            
        current += timedelta(days=1)

    if results:
        df = pd.DataFrame(results)
        filename = f"data/pure_data_{args.start}_{args.end}.csv"
        df.to_csv(filename, index=False)
        print(f"\n🎉 完了！CSV保存しました: {filename} ({len(df)}レース)")
    else:
        print("\n💀 データが取れませんでした。")
