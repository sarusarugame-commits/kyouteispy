import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime

# ==========================================
# ⚙️ 設定
# ==========================================
TARGET_DATE = "20250101"  # デバッグしたい日付 (YYYYMMDD)

def get_soup(url):
    """HTML取得の共通関数"""
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        res = requests.get(url, headers=headers, timeout=10)
        res.encoding = res.apparent_encoding
        return BeautifulSoup(res.text, 'html.parser')
    except Exception as e:
        print(f"⚠️ 通信エラー: {url} ({e})")
        return None

def scrape_race_data(jcd, rno, date_str):
    """1レース分のデータを取得"""
    base_url = "https://www.boatrace.jp/owpc/pc/race"
    
    # 1. 出走表（勝率・モーター）
    soup_list = get_soup(f"{base_url}/racelist?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    # 2. 直前情報（風速・展示）
    soup_before = get_soup(f"{base_url}/beforeinfo?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    # 3. 結果（的中率の正解ラベル用）
    soup_res = get_soup(f"{base_url}/raceresult?rno={rno}&jcd={jcd:02d}&hd={date_str}")

    if not all([soup_list, soup_before, soup_res]): return None

    try:
        # 風速 (wind)
        w_text = soup_before.select_one('.weather1_bodyUnitLabelData').text.replace('m','').strip()
        wind = float(w_text) if w_text else 0.0

        # 正解ラベル: 1号艇が1着なら1、それ以外なら0
        res1_text = soup_res.select_one('.is-p_0-1 .is-p_1-1') 
        res1 = 1 if (res1_text and res1_text.text.strip() == "1") else 0

        row = {'date': date_str, 'jcd': jcd, 'rno': rno, 'wind': wind, 'res1': res1}

        # 1〜6号艇の基本データ
        for i in range(1, 7):
            # 勝率 (wr) と モーター (mo)
            tbody = soup_list.select(f'tbody.is-p_0-{i}')[0].select('td')
            row[f'wr{i}'] = float(tbody[3].select_one('div').text.split()[0])
            row[f'mo{i}'] = float(tbody[6].select_one('div').text.split()[0])
            
            # 展示タイム (ex) - 欠損時は平均値6.70
            ex_val = soup_before.select(f'tbody.is-p_0-{i}')[0].select('td')[4].text.strip()
            row[f'ex{i}'] = float(ex_val) if ex_val else 6.70

        return row
    except Exception as e:
        # レースが開催されていない、またはデータ不足
        return None

def main():
    print(f"🚀 {TARGET_DATE} のデバッグ収集を開始します...")
    results = []
    
    # 全24会場 × 12レースを走査
    for jcd in range(1, 25):
        print(f"🏟️ 会場コード {jcd:02d} をスキャン中...")
        for rno in range(1, 13):
            data = scrape_race_data(jcd, rno, TARGET_DATE)
            if data:
                results.append(data)
                print(f"  ✅ {rno}R 取得成功")
            time.sleep(0.1) # サーバー負荷軽減

    if results:
        df = pd.DataFrame(results)
        filename = f"debug_data_{TARGET_DATE}.csv"
        df.to_csv(filename, index=False)
        print(f"\n✨ 収集完了！ {len(df)} レースのデータを保存しました: {filename}")
    else:
        print("\n❌ 有効なデータが取得できませんでした。")

if __name__ == "__main__":
    main()
