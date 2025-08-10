# scrape_rakuten_keiba_2024.py
# 2024年の地方競馬（帯広以外）の全レースについて、
# 人気順＋単勝オッズ（全頭）と三連単の確定組番・払戻、
# 距離・馬場状態などメタ情報も取得してCSV出力します。

import time
import json
import re
from datetime import date, timedelta
from typing import List, Dict, Optional, Tuple

import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm

BASE = "https://keiba.rakuten.co.jp"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; KeibaStudyBot/1.0; +https://example.com/)",
}
SLEEP_BETWEEN_REQUESTS = 1.5  # サイトに優しい低速（秒）
YEAR = 2024

# 帯広を除外
EXCLUDE_TRACKS = {"帯広"}

# 出力ファイル
RACE_CSV = "races_2024_all_local_ex_obihiro.csv"   # レース単位
HORSE_CSV = "horses_2024_all_local_ex_obihiro.csv" # 馬単位
CHECKPOINT = "checkpoint_seen_races.json"

def get_soup(url: str) -> Optional[BeautifulSoup]:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        if resp.status_code != 200:
            return None
        return BeautifulSoup(resp.text, "lxml")
    except requests.RequestException:
        return None

def date_range(year: int):
    d = date(year, 1, 1)
    end = date(year, 12, 31)
    while d <= end:
        yield d
        d += timedelta(days=1)

def save_checkpoint(seen: Dict[str, bool]):
    with open(CHECKPOINT, "w", encoding="utf-8") as f:
        json.dump(seen, f, ensure_ascii=False, indent=2)

def load_checkpoint() -> Dict[str, bool]:
    try:
        with open(CHECKPOINT, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {}

# 日付ごとの開催一覧URL候補
CANDIDATE_LIST_PAGES = [
    "{base}/race_card/list/RACEID/{ymd}000000000000",
    "{base}/calendar/list?l={ymd}",
    "{base}/race_schedule/list?l={ymd}",
]

def discover_race_card_links(ymd: str) -> List[str]:
    found = set()
    for tpl in CANDIDATE_LIST_PAGES:
        url = tpl.format(base=BASE, ymd=ymd)
        soup = get_soup(url)
        time.sleep(SLEEP_BETWEEN_REQUESTS)
        if not soup:
            continue
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/race_card/" in href:
                found.add(href if href.startswith("http") else BASE + href)
    return sorted(found)

def find_odds_and_dividend_links(race_card_url: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    soup = get_soup(race_card_url)
    time.sleep(SLEEP_BETWEEN_REQUESTS)
    if not soup:
        return None, None, None

    track_name = None
    for sel in ["h1", "h2", ".headingArea", ".raceTtl", ".raceHeader"]:
        node = soup.select_one(sel)
        if node:
            txt = node.get_text(strip=True)
            m = re.search(r"(帯広|門別|盛岡|水沢|浦和|船橋|大井|川崎|金沢|笠松|名古屋|園田|高知|佐賀)", txt)
            if m:
                track_name = m.group(1)
                break

    odds_url = None
    div_url = None
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/odds/tanfuku/" in href and not odds_url:
            odds_url = href if href.startswith("http") else BASE + href
        if "/race_dividend/list/" in href and not div_url:
            div_url = href if href.startswith("http") else BASE + href

    return track_name, odds_url, div_url

def parse_race_meta(race_card_url: str):
    soup = get_soup(race_card_url)
    time.sleep(SLEEP_BETWEEN_REQUESTS)
    if not soup:
        return None, None, None, None, None

    race_no = None
    distance_m = None
    surface = None
    course_dir = None
    going = None

    headline = ""
    for sel in ["h1", "h2", ".raceHeader", ".headingArea", ".raceTtl"]:
        node = soup.select_one(sel)
        if node:
            headline = node.get_text(" ", strip=True)
            break

    m = re.search(r"(\d{3,4})\s*m", headline)
    if m:
        distance_m = int(m.group(1))

    if "芝" in headline:
        surface = "芝"
    elif "ダ" in headline or "ダート" in headline:
        surface = "ダート"

    m = re.search(r"(右|左|直線)", headline)
    if m:
        course_dir = m.group(1)

    m = re.search(r"(\d+)\s*R", headline, re.IGNORECASE)
    if m:
        race_no = int(m.group(1))

    page_text = soup.get_text(" ", strip=True)
    m = re.search(r"(馬場)\s*[:：]?\s*(良|稍重|重|不良)", headline) or \
        re.search(r"(馬場)\s*[:：]?\s*(良|稍重|重|不良)", page_text)
    if m:
        going = m.group(2)

    return race_no, distance_m, surface, course_dir, going

def parse_tanfuku_odds(odds_url: str) -> List[Dict]:
    soup = get_soup(odds_url)
    time.sleep(SLEEP_BETWEEN_REQUESTS)
    if not soup:
        return []
    rows = []
    table = soup.select_one("table")
    if not table:
        return rows
    for tr in table.find_all("tr"):
        tds = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
        if len(tds) < 4:
            continue
        try:
            popularity = int(re.sub(r"\D", "", tds[0]))
            horse_no = int(re.sub(r"\D", "", tds[1]))
            win_odds = float(tds[3].replace(",", ""))
        except:
            continue
        rows.append({
            "popularity": popularity,
            "horse_no": horse_no,
            "win_odds": win_odds
        })
    rows.sort(key=lambda x: x["popularity"])
    return rows

def parse_trifecta(div_url: str) -> Tuple[Optional[str], Optional[int]]:
    soup = get_soup(div_url)
    time.sleep(SLEEP_BETWEEN_REQUESTS)
    if not soup:
        return None, None
    text = soup.get_text(" ", strip=True)
    m = re.search(r"(三連単|3連単)\s*([0-9]+-[0-9]+-[0-9]+)\s*([0-9,]+)\s*円", text)
    if m:
        return m.group(2), int(m.group(3).replace(",", ""))
    for tr in soup.find_all("tr"):
        tds = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
        row_txt = " ".join(tds)
        if "三連単" in row_txt or "3連単" in row_txt:
            m2 = re.search(r"([0-9]+-[0-9]+-[0-9]+)", row_txt)
            m3 = re.search(r"([0-9,]+)\s*円", row_txt)
            if m2 and m3:
                return m2.group(1), int(m3.group(1).replace(",", ""))
    return None, None

def main():
    seen = load_checkpoint()
    races_out = []
    horses_out = []

    for d in tqdm(list(date_range(YEAR)), desc=f"Searching {YEAR}"):
        ymd = d.strftime("%Y%m%d")
        race_card_links = discover_race_card_links(ymd)
        if not race_card_links:
            continue

        for rc_url in race_card_links:
            if rc_url in seen:
                continue

            track_name, odds_url, div_url = find_odds_and_dividend_links(rc_url)
            race_no, distance_m, surface, course_dir, going = parse_race_meta(rc_url)

            if not track_name or (track_name in EXCLUDE_TRACKS):
                seen[rc_url] = True
                continue
            if not odds_url or not div_url:
                seen[rc_url] = True
                continue

            odds_rows = parse_tanfuku_odds(odds_url)
            if not odds_rows:
                seen[rc_url] = True
                continue

            trifecta_combo, trifecta_pay = parse_trifecta(div_url)

            m = re.search(r"/RACEID/([0-9A-Za-z]+)", rc_url)
            race_key = m.group(1) if m else rc_url

            rrow = {
                "date": ymd,
                "track": track_name,
                "race_no": race_no,
                "distance_m": distance_m,
                "surface": surface,
                "course_dir": course_dir,
                "going": going,
                "race_key": race_key,
                "race_card_url": rc_url,
                "odds_url": odds_url,
                "dividend_url": div_url,
                "trifecta_combo": trifecta_combo,
                "trifecta_pay_100yen": trifecta_pay
            }
            races_out.append(rrow)

            for od in odds_rows:
                horses_out.append({
                    "date": ymd,
                    "track": track_name,
                    "race_key": race_key,
                    "popularity": od["popularity"],
                    "horse_no": od["horse_no"],
                    "win_odds": od["win_odds"]
                })

            seen[rc_url] = True

            if len(races_out) % 50 == 0:
                pd.DataFrame(races_out).to_csv(RACE_CSV, index=False)
                pd.DataFrame(horses_out).to_csv(HORSE_CSV, index=False)
                save_checkpoint(seen)

    pd.DataFrame(races_out).to_csv(RACE_CSV, index=False)
    pd.DataFrame(horses_out).to_csv(HORSE_CSV, index=False)
    save_checkpoint(seen)
    print("DONE.")
    print(f"- RACES  : {RACE_CSV}")
    print(f"- HORSES : {HORSE_CSV}")
    print(f"- CHECK  : {CHECKPOINT}")

if __name__ == "__main__":
    main()