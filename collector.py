import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import sys
import argparse
import os
import re
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

sys.stdout.reconfigure(line_buffering=True)

MAX_RETRIES = 5
RETRY_INTERVAL = 3
MAX_WORKERS = 8 

def get_session():
    session = requests.Session()
    adapter = HTTPAdapter(pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS, max_retries=Retry(total=MAX_RETRIES, backoff_factor=1))
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'})
    return session

def get_soup_with_retry(session, url):
    for _ in range(MAX_RETRIES):
        try:
            res = session.get(url, timeout=20)
            res.encoding = res.apparent_encoding
            if res.status_code == 200:
                return BeautifulSoup(res.text, 'html.parser')
        except: pass
        time.sleep(RETRY_INTERVAL)
    return None

def clean_text(text):
    if not text: return ""
    return text.replace("\n", "").replace("\r", "").replace(" ", "").replace("\u3000", "").strip()

def scrape_race_data(session, jcd, rno, date_str):
    base_url = "https://www.boatrace.jp/owpc/pc/race"
    soup_list = get_soup_with_retry(session, f"{base_url}/racelist?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    soup_before = get_soup_with_retry(session, f"{base_url}/beforeinfo?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    soup_res = get_soup_with_retry(session, f"{base_url}/raceresult?rno={rno}&jcd={jcd:02d}&hd={date_str}")

    if not all([soup_list, soup_before, soup_res]): return None

    try:
        wind = 0.0
        try:
            wind_elem = soup_before.find(string=re.compile("風速"))
            if wind_elem:
                data_elem = wind_elem.find_parent(class_="weather1_bodyUnit").select_one(".weather1_bodyUnitLabelData")
                if data_elem: wind = float(clean_text(data_elem.text).replace("m", ""))
        except: pass 

        res1 = 0
        try:
            res_rows = soup_res.select(".is-p_1-1")
            if res_rows:
                rank_text = clean_text(res_rows[0].select("td")[1].text)
                if rank_text == "1" or rank_text == "１": res1 = 1
        except: pass

        temp_ex_times = []
        for i in range(1, 7):
            boat_cell = soup_before.select_one(f".is-boatColor{i}")
            if not boat_cell: return None
            ex_val = clean_text(boat_cell.find_parent("tbody").select("td")[4].text)
            if not ex_val: ex_val = clean_text(boat_cell.find_parent("tbody").select("td")[5].text)
            if not ex_val or ex_val in ["-", "0.00"]: return None
            temp_ex_times.append(float(ex_val))

        row = {'date': date_str, 'jcd': jcd, 'rno': rno, 'wind': wind, 'res1': res1}
        for i in range(1, 7):
            tds = soup_list.select_one(f".is-boatColor{i}").find_parent("tbody").select("td")
            try:
                row[f'wr{i}'] = float(re.findall(r"\d+\.\d+", tds[3].text)[0])
                nums = re.findall(r"\d+\.\d+", tds[6].text)
                row[f'mo{i}'] = float(nums[0]) if nums else 0.0
            except:
                row[f'wr{i}'], row[f'mo{i}'] = 0.0, 0.0
            row[f'ex{i}'] = temp_ex_times[i-1]
        return row
    except: return None

def process_race_parallel(args):
    return scrape_race_data(*args)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    args = parser.parse_args()
    
    os.makedirs("data", exist_ok=True)
    session = get_session()
    start_d = datetime.strptime(args.start, "%Y-%m-%d")
    end_d = datetime.strptime(args.end, "%Y-%m-%d")
    current = start_d
    results = []
    
    while current <= end_d:
        d_str = current.strftime("%Y%m%d")
        tasks = [(session, jcd, rno, d_str) for jcd in range(1, 25) for rno in range(1, 13)]
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for res in executor.map(process_race_parallel, tasks):
                if res: results.append(res)
        current += timedelta(days=1)

    if results:
        pd.DataFrame(results).to_csv(f"data/chunk_{args.start}.csv", index=False)
