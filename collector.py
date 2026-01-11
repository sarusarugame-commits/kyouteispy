import os
import sys
import time
import requests
import polars as pl
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import re
import argparse

# ログ出力用
STADIUMS = {f"{i:02}": n for i, n in enumerate(["","桐生","戸田","江戸川","平和島","多摩川","浜名湖","蒲郡","常滑","津","三国","びわこ","住之江","尼崎","鳴門","丸亀","児島","宮島","徳山","下関","若松","芦屋","福岡","唐津","大村"])}

def get_soup(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    try:
        res = requests.get(url, headers=headers, timeout=15)
        res.encoding = res.apparent_encoding
        time.sleep(1.2) # GitHubのIPはBANされやすいので少しだけ慎重に
        return BeautifulSoup(res.text, 'html.parser') if res.status_code == 200 else None
    except: return None

def safe_float(v, d=0.0):
    try: return float(re.sub(r"[FL%mcm℃ ]", "", str(v))) if v and str(v).strip() != "-" else d
    except: return d

def get_race_data(date_str, jcd, rno):
    base = "https://www.boatrace.jp/owpc/pc/race"
    s_prog = get_soup(f"{base}/racelist?rno={rno}&jcd={jcd}&hd={date_str}")
    s_before = get_soup(f"{base}/beforeinfo?rno={rno}&jcd={jcd}&hd={date_str}")
    s_result = get_soup(f"{base}/raceresult?rno={rno}&jcd={jcd}&hd={date_str}")
    
    if not (s_prog and s_before and s_result) or "データがありません" in s_result.text: return None

    try:
        # 結果・配当
        num_set = s_result.select(".numberSet1_number")
        if not num_set: return None
        res1 = int(safe_float(num_set[0].text))
        pay_elem = s_result.select(".is-payout1")
        payout = int(safe_float(pay_elem[-1].text)) if pay_elem else 0

        # 気象
        w = s_before.select(".weather1_bodyUnitLabelData")
        data = {"date":date_str, "stadium":jcd, "rno":rno, "res1":res1, "payout":payout,
                "wind":safe_float(w[2].text) if len(w)>2 else 0, "wave":safe_float(w[4].text) if len(w)>4 else 0}

        # 選手データ
        progs = s_prog.select("tbody.is-fs12")
        befores = s_before.select("tbody.is-fs12")
        for i in range(1, 7):
            # 出走表
            if i-1 < len(progs):
                cells = progs[i-1].select("td.is-lineH2")
                data[f"wr{i}"] = safe_float(cells[1].get_text().split("\n")[0]) if len(cells)>1 else 0
                data[f"mo{i}"] = safe_float(cells[3].get_text().split("\n")[1]) if len(cells)>3 else 0
            # 展示
            ex_t = 6.99
            if i-1 < len(befores):
                m = re.search(r"([0-6]\.\d{2})", befores[i-1].get_text())
                if m: ex_t = float(m.group(1))
            data[f"ex{i}"] = ex_t
        return data
    except: return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    args = parser.parse_args()

    os.makedirs("data", exist_ok=True)
    curr = datetime.strptime(args.start, "%Y-%m-%d")
    end = datetime.strptime(args.end, "%Y-%m-%d")

    while curr <= end:
        d_str = curr.strftime("%Y%m%d")
        day_list = []
        print(f"📅 {d_str} Start...")
        for j in range(1, 25):
            j_str = f"{j:02}"
            print(f"  {STADIUMS[j_str]}", end=" ", flush=True)
            for r in range(1, 13):
                res = get_race_data(d_str, j_str, r)
                if res: day_list.append(res)
            print("Done")
        if day_list:
            pl.DataFrame(day_list).write_csv(f"data/boat_{d_str}.csv")
        curr += timedelta(days=1)
