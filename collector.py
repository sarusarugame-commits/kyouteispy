import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import re
import os
import unicodedata
import argparse
import random
import threading
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

# ==========================================
# ⚙️ 設定エリア
# ==========================================
# 並列数を20に変更
MAX_WORKERS = 20
MAX_RETRIES = 5
RETRY_DELAY = 3
TIMEOUT_SEC = 20

UA_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
]

print_lock = threading.Lock()


def safe_print(msg):
    with print_lock:
        timestamp = time.strftime("%H:%M:%S")
        print(f"[{timestamp}] {msg}", flush=True)


def clean_text(text):
    if not text:
        return ""
    text = unicodedata.normalize("NFKC", str(text))
    return (
        text.replace("\n", "")
        .replace("\r", "")
        .replace("¥", "")
        .replace(",", "")
        .strip()
    )


def get_column_names():
    """CSVのカラム定義を一箇所で管理"""
    cols = [
        "date",
        "jcd",
        "rno",
        "wind",
        "res1",
        "rank1",
        "rank2",
        "rank3",
        "tansho",
        "nirentan",
        "sanrentan",
        "sanrenpuku",
        "payout",
    ]
    for i in range(1, 7):
        # pid (選手ID) を追加
        cols.extend([f"pid{i}", f"wr{i}", f"mo{i}", f"ex{i}", f"f{i}", f"st{i}"])
    return cols


def get_session():
    session = requests.Session()
    retries = Retry(
        total=MAX_RETRIES,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    # 並列数に合わせてプールサイズも拡張
    adapter = HTTPAdapter(
        pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS, max_retries=retries
    )
    session.mount("https://", adapter)
    return session


def get_soup(session, url):
    for i in range(MAX_RETRIES):
        try:
            headers = {"User-Agent": random.choice(UA_LIST)}
            res = session.get(url, headers=headers, timeout=TIMEOUT_SEC)
            res.encoding = res.apparent_encoding

            if res.status_code == 200:
                if "データがありません" in res.text or "メンテナンス" in res.text:
                    return None, "SKIP"
                return BeautifulSoup(res.text, "html.parser"), None

            if res.status_code == 404:
                return None, "ERROR"

            time.sleep(random.uniform(1, 2))
        except Exception:
            time.sleep(RETRY_DELAY)

    return None, "ERROR"


def extract_payout(soup, key_text):
    if not soup:
        return 0
    try:
        # table.is-w495 をピンポイントで狙う
        for tbl in soup.select("table.is-w495"):
            if key_text in tbl.text:
                rows = tbl.select("tr")
                for tr in rows:
                    if key_text in tr.text:
                        # 金額は行の後ろの方にある。
                        # 人気順(1桁~2桁)を拾わないよう、後ろから走査して「100以上」の数値を探す
                        tds = tr.select("td")
                        if not tds:
                            continue

                        for td in reversed(tds):
                            txt = clean_text(td.text)
                            if txt.isdigit():
                                val = int(txt)
                                # 100円以上なら金額とみなす（人気順などの誤取得防止）
                                if val >= 100:
                                    return val
    except:
        pass
    return 0


def scrape_race_data(session, jcd, rno, date_str):
    base_url = "https://www.boatrace.jp/owpc/pc/race"

    url_res = f"{base_url}/raceresult?rno={rno}&jcd={jcd:02d}&hd={date_str}"
    url_bef = f"{base_url}/beforeinfo?rno={rno}&jcd={jcd:02d}&hd={date_str}"
    url_lst = f"{base_url}/racelist?rno={rno}&jcd={jcd:02d}&hd={date_str}"

    soup_res, err = get_soup(session, url_res)
    if err == "SKIP" or not soup_res:
        return None

    soup_before, _ = get_soup(session, url_bef)
    soup_list, _ = get_soup(session, url_lst)

    try:
        row = {"date": date_str, "jcd": jcd, "rno": rno}

        # 天候・風 (beforeinfoから取得)
        row["wind"] = 0.0
        if soup_before:
            try:
                # クラス名 "is-windDirection" (風向・風速エリア) をピンポイントで取得
                # ※以前のコードでは "is-temperature" (気温) を拾っていました
                wind_unit = soup_before.select_one(".is-windDirection")
                if wind_unit:
                    wind_data = wind_unit.select_one(".weather1_bodyUnitLabelData")
                    if wind_data:
                        w_txt = clean_text(wind_data.text)
                        m = re.search(r"(\d+)", w_txt)
                        row["wind"] = float(m.group(1)) if m else 0.0
            except:
                pass

        # 順位
        row["rank1"], row["rank2"], row["rank3"] = None, None, None
        try:
            result_rows = soup_res.select("table.is-w495 tbody tr")
            for idx, r_key in enumerate(["rank1", "rank2", "rank3"]):
                if len(result_rows) > idx:
                    # 着順の数字を取得
                    rank_td = result_rows[idx].select("td")
                    if len(rank_td) >= 2:
                        r_txt = clean_text(rank_td[1].text)
                        if r_txt.isdigit():
                            row[r_key] = int(r_txt)
        except:
            pass

        row["res1"] = 1 if row.get("rank1") == 1 else 0

        # 払い戻し
        row["tansho"] = extract_payout(soup_res, "単勝")
        row["nirentan"] = extract_payout(soup_res, "2連単")
        row["sanrentan"] = extract_payout(soup_res, "3連単")
        row["sanrenpuku"] = extract_payout(soup_res, "3連複")
        row["payout"] = row["sanrentan"]

        # 各艇データ取得
        for i in range(1, 7):
            # 初期化
            row[f"pid{i}"] = 0  # 選手ID
            row[f"wr{i}"] = 0.0  # 勝率
            row[f"mo{i}"] = 0.0  # モーター
            row[f"ex{i}"] = 0.0  # 展示タイム
            row[f"f{i}"] = 0  # フライング
            row[f"st{i}"] = 0.20  # 平均ST

            # 1. 出走表(racelist)から ID, 勝率, 平均ST, F数 を取得
            if soup_list:
                try:
                    # 枠番ごとのtbodyを取得 (is-fs12クラスを持つtbody)
                    tbodies = soup_list.select("tbody.is-fs12")
                    if len(tbodies) >= i:
                        tbody = tbodies[i - 1]  # 枠番に対応するtbody

                        # --- 選手ID (登番) ---
                        # クラス名(is-fs11)に依存せず、テキスト全体から「4桁の数字」を探す
                        # 登録番号は通常2000番台～5000番台。年齢(2桁)や体重(3桁)と区別可能。
                        txt_all = clean_text(tbody.text)
                        # 先頭から検索して最初に見つかる4桁の数字(登録番号)を取得
                        pid_match = re.search(r"([2-5]\d{3})", txt_all)
                        if pid_match:
                            row[f"pid{i}"] = int(pid_match.group(1))

                        # --- 勝率, モーター, 平均ST, F ---
                        tds = tbody.select("td")
                        full_row_text = " ".join([clean_text(td.text) for td in tds])

                        # 勝率: tdを個別にチェックして確実に拾う
                        for td in tds:
                            txt = clean_text(td.text)
                            # 完全一致だと余計な文字がある場合に失敗するため、部分一致(search)に変更
                            m = re.search(r"(\d\.\d{2})", txt)
                            if m:
                                val = float(m.group(1))
                                # 勝率は 1.00 ～ 9.99 の範囲（モーター2連対率は10.0以上なので区別可能）
                                if 1.0 <= val <= 9.99:
                                    row[f"wr{i}"] = val
                                    break  # 最初に見つかるのが全国勝率

                        # モーター: XX.XX 形式を探す
                        mo_matches = re.findall(r"(\d{2}\.\d{2})", full_row_text)
                        if mo_matches:
                            for m_val in mo_matches:
                                if 10.0 <= float(m_val) <= 99.9:
                                    row[f"mo{i}"] = float(m_val)
                                    break

                        # 平均ST (0.XX)
                        st_match = re.search(r"(0\.\d{2})", full_row_text)
                        if st_match:
                            row[f"st{i}"] = float(st_match.group(1))

                        # F数 (F1, F2...)
                        f_match = re.search(r"F(\d+)", full_row_text)
                        if f_match:
                            row[f"f{i}"] = int(f_match.group(1))

                except:
                    pass

            # 2. 直前情報(beforeinfo)から 展示タイム を取得
            if soup_before:
                try:
                    # is-boatColor1 ~ 6 のクラスを持つtdを探す
                    boat_td = soup_before.select_one(f"td.is-boatColor{i}")
                    if boat_td:
                        # その行(tr)を取得
                        tr = boat_td.find_parent("tr")
                        if tr:
                            tds = tr.select("td")
                            # 展示タイムは通常後ろの方にある (td[4]以降)
                            # 値が "6.XX" のような形式を探す
                            for td in tds[4:]:
                                val = clean_text(td.text)
                                if re.match(r"^\d\.\d{2}$", val):
                                    # 6.50 ~ 7.00 くらいの値が展示タイム
                                    if 6.0 <= float(val) <= 7.5:
                                        row[f"ex{i}"] = float(val)
                                        break
                except:
                    pass

        return row
    except:
        return None


def process_wrapper(args):
    session, jcd, rno, date_str = args
    time.sleep(random.uniform(0.1, 0.4))
    try:
        result = scrape_race_data(session, jcd, rno, date_str)
        if result is None:
            # 失敗時はログに残す
            safe_print(
                f"⚠️ [SKIP] {date_str} 場:{jcd:02} R:{rno:02} -> データなし/取得失敗"
            )
        return result
    except Exception as e:
        safe_print(f"❌ [ERROR] {date_str} 場:{jcd:02} R:{rno:02} -> {e}")
        return None


def show_progress(processed, total):
    bar_len = 30
    filled = int(bar_len * processed / total)
    bar = "=" * filled + "-" * (bar_len - filled)
    percent = 100 * processed / total
    print(f"\r⏳ [{bar}] {percent:.1f}% ({processed}/{total})", end="")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    today = datetime.now().strftime("%Y-%m-%d")

    parser.add_argument("--start", help="開始日 (YYYY-MM-DD)")
    parser.add_argument("--end", help="終了日 (YYYY-MM-DD)")
    parser.add_argument("--year", type=int, help="指定した年全体を収集")

    args = parser.parse_args()

    if args.year:
        start_d = datetime(args.year, 1, 1)
        end_d = datetime(args.year, 12, 31)
    else:
        # 【修正】引数がない場合はデータが存在する過去の日付（2024-12-01）をデフォルトにする
        default_date = "2024-12-01"
        s_str = args.start if args.start else default_date
        e_str = args.end if args.end else default_date
        try:
            start_d = datetime.strptime(s_str, "%Y-%m-%d")
            end_d = datetime.strptime(e_str, "%Y-%m-%d")
        except ValueError:
            print("❌ 日付エラー: YYYY-MM-DD 形式で指定してください。")
            sys.exit(1)

    if start_d > end_d:
        print("❌ エラー: 開始日が終了日より後になっています。")
        sys.exit(1)

    session = get_session()
    current = start_d

    safe_print(
        f"🚀 収集開始: {start_d.strftime('%Y-%m-%d')} 〜 {end_d.strftime('%Y-%m-%d')}"
    )
    safe_print(f"⚡ 並列スレッド数: {MAX_WORKERS}")

    os.makedirs("data", exist_ok=True)
    filename = (
        f"data/race_data_{start_d.strftime('%Y%m%d')}_{end_d.strftime('%Y%m%d')}.csv"
    )

    csv_columns = get_column_names()

    # ファイルがなければヘッダーを作成
    if not os.path.exists(filename):
        pd.DataFrame(columns=csv_columns).to_csv(filename, index=False)

    total_races = 0

    while current <= end_d:
        d_str = current.strftime("%Y%m%d")
        safe_print(f"📅 {current.strftime('%Y-%m-%d')} のデータを収集中...")

        tasks = []
        for jcd in range(1, 25):
            for rno in range(1, 13):
                tasks.append((session, jcd, rno, d_str))

        # 【本番モード】全レース取得
        # random.shuffle(tasks)
        # tasks = tasks[:10]
        safe_print(
            f"🚀 本番モード: {len(tasks)} レース分のタスクを投入します (全件取得)"
        )

        task_total = len(tasks)
        processed = 0
        results = []

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_task = {executor.submit(process_wrapper, t): t for t in tasks}

            for future in as_completed(future_to_task):
                processed += 1
                show_progress(processed, task_total)

                try:
                    res = future.result()
                    if res:
                        results.append(res)
                except:
                    pass

        print("")

        if results:
            df = pd.DataFrame(results)

            # カラムが存在しない場合NaNで埋めて、順序を統一する
            df = df.reindex(columns=csv_columns)

            # 追記モード
            df.to_csv(filename, mode="a", index=False, header=False)
            safe_print(f"  ✅ {len(df)}レース 保存しました")
            total_races += len(df)
        else:
            safe_print(f"  ⚠️ データなし (開催なし or エラー)")

        current += timedelta(days=1)

    safe_print("=" * 40)
    safe_print(f"🎉 すべて完了しました！")
    safe_print(f"📁 保存ファイル: {filename}")
    safe_print(f"📊 合計取得数: {total_races} レース")
    safe_print("=" * 40)
