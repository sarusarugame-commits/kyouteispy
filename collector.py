import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import sys
import argparse
import os

# ログ即時表示
sys.stdout.reconfigure(line_buffering=True)

# ==========================================
# ⚙️ 設定：HTML現物保存モード
# ==========================================
MAX_RETRIES = 3
RETRY_INTERVAL = 5 

def get_session():
    session = requests.Session()
    # あえてシンプルなヘッダーに戻してみる（過度な偽装が逆効果な場合があるため）
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    }
    session.headers.update(headers)
    return session

def scrape_race_data(session, jcd, rno, date_str):
    url_list = f"https://www.boatrace.jp/owpc/pc/race/racelist?rno={rno}&jcd={jcd:02d}&hd={date_str}"
    
    print(f"🔍 {jcd}場 {rno}R: アクセス中...", end="")
    
    try:
        res = session.get(url_list, timeout=20)
        res.encoding = res.apparent_encoding
        
        # HTMLの中身をチェック
        soup = BeautifulSoup(res.text, 'html.parser')
        
        # タイトル取得
        title = soup.title.text.strip() if soup.title else "タイトルなし"
        
        # ❌ データがあるべきテーブルを探す
        target_table = soup.select_one('table.is-w495')
        
        if target_table:
            print(f" ✅ 成功！(タイトル: {title})")
            # ここで本来のデータ取得処理...（今回は省略）
            return {'status': 'ok'}
        else:
            print(f" ❌ データなし (タイトル: {title})")
            
            # 🔥 失敗したHTMLをファイルに保存！（これが証拠になる）
            filename = f"error_html_{rno}.html"
            with open(filename, "w", encoding="utf-8") as f:
                f.write(res.text)
            print(f" 💾 HTMLを保存しました: {filename}")
            
            return None

    except Exception as e:
        print(f" 💥 エラー: {e}")
        return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    args = parser.parse_args()

    session = get_session()
    
    # 認証
    try:
        session.get("https://www.boatrace.jp/", timeout=10)
    except:
        pass

    # テスト実行
    scrape_race_data(session, 1, 1, "20250101")
