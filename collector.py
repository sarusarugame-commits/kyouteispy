import requests
from bs4 import BeautifulSoup
import re

# ターゲット：2025/01/01 桐生 1R
target_urls = {
    "番組表 (racelist)": "https://www.boatrace.jp/owpc/pc/race/racelist?rno=1&jcd=01&hd=20250101",
    "直前情報 (beforeinfo)": "https://www.boatrace.jp/owpc/pc/race/beforeinfo?rno=1&jcd=01&hd=20250101"
}

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

def clean(text):
    return text.replace("\n", "").replace("\r", "").replace(" ", "").strip()

print("=== 🛠️ 診断開始 ===")

for name, url in target_urls.items():
    print(f"\n📡 {name} にアクセス中...")
    print(f"URL: {url}")
    
    try:
        res = requests.get(url, headers=headers, timeout=10)
        res.encoding = res.apparent_encoding
        soup = BeautifulSoup(res.text, 'html.parser')
        
        # 1号艇（is-boatColor1）を探す
        boat1 = soup.select_one(".is-boatColor1")
        
        if boat1:
            tbody = boat1.find_parent("tbody")
            tds = tbody.select("td")
            
            print(f"✅ 1号艇のデータを取得しました。各セルの内容を確認してください：")
            for i, td in enumerate(tds):
                # 中身を少し綺麗にして表示
                content = clean(td.text)
                print(f"  [td番号 {i}] : {content}")
                
            # --- 簡易解析チェック ---
            if "racelist" in url:
                # 勝率は通常 td[3] にある "X.XX" という数値
                print("\n  🔍 [勝率チェック]")
                if len(tds) > 3:
                    print(f"  今のコードはここを見ています -> td[3]: {clean(tds[3].text)}")
                
            if "beforeinfo" in url:
                # モーター勝率は通常 td[2] にある "XX.X%"
                print("\n  🔍 [モーターチェック]")
                if len(tds) > 2:
                    print(f"  今のコードはここを見ています -> td[2]: {clean(tds[2].text)}")
                
        else:
            print("❌ 1号艇の要素 (.is-boatColor1) が見つかりませんでした。HTML構造が大幅に違う可能性があります。")
            
    except Exception as e:
        print(f"❌ エラー発生: {e}")

print("\n=== 診断終了 ===")
