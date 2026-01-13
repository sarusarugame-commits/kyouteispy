import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import sys

# ログを即座に出力する関数
def log(msg):
    print(msg, flush=True)

def get_soup(url):
    try:
        log(f"🌐 アクセス中: {url}")
        res = requests.get(url, timeout=10)
        res.encoding = res.apparent_encoding
        if res.status_code == 200:
            return BeautifulSoup(res.text, 'html.parser')
        log(f"❌ エラー: ステータスコード {res.status_code}")
    except Exception as e:
        log(f"💥 例外発生: {e}")
    return None

if __name__ == "__main__":
    log("🚀 デバッグ実行開始（1会場・1レース限定）")
    
    # テストとして「桐生(01) 1R」だけを取得
    date_str = "20250101"
    jcd = 1
    rno = 1
    
    base_url = "https://www.boatrace.jp/owpc/pc/race/racelist"
    url = f"{base_url}?rno={rno}&jcd={jcd:02d}&hd={date_str}"
    
    soup = get_soup(url)
    
    if soup:
        log("✅ データの取得に成功しました！")
        # 試しに1号艇の名前だけ出してみる
        try:
            name = soup.select_one(".name").text.strip()
            log(f"👤 1号艇の名前: {name}")
        except:
            log("⚠️ ページ構造が違う、または選手名が見つかりません")
    else:
        log("💀 データが取得できませんでした。GitHubのIPがブロックされている可能性があります。")
    
    log("🏁 デバッグ終了")
